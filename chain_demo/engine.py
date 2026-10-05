"""Pure v0.3 YAML reducer. I/O and installed plugins belong to Activities.

The previous v0.2 reducer is preserved in legacy_engine.py.
"""
import copy

from .agent_requests import make_scoped_job, scope_snapshot
from .context import ContextLedger
from .contracts import (validate_event, validate_agent_result, validate_hitl_request,
                        validate_hitl_decision, validate_hitl_decision_shape)
from .expressions import matches, evaluate
from .effect_contracts import validate_effect_receipt
from .state_specification import state_specification_view
from .orchestration_schema import normalize_action, validate_engine_bundle
from .source_contracts import identity, validate_completion, REVISION_EVENTS
from .validation import (canonical_hash, fields, hash_value, json_value, safe_audit_id,
                         safe_audit_reason, strings, text, timestamp)

digest = canonical_hash


class Engine:
    def __init__(self, bundle, initial, *, now):
        validate_engine_bundle(bundle)
        self.bundle = copy.deepcopy(bundle)
        self.wf = self.bundle["workflow"]
        self.policy = self.bundle["policy"]
        self.initial = copy.deepcopy(initial)
        self.context = ContextLedger(initial, known_at=now)
        if initial["site_id"] != self.wf["site_id"]:
            raise ValueError("Initial site not admitted")
        self.now = now
        self.state = None
        self.generation = self.context_version = self.counter = 0
        self.history, self.audit, self.out, self.waiters, self.notifications = [], [], [], [], []
        self.effects = []
        self.pending, self.timers, self.results, self.agent_outputs, self.agent_runs = {}, {}, {}, {}, {}
        self.context_requests, self.summary_cache, self.open_hitl = {}, {}, {}
        self.hitl_history = {}
        self._invalidated_evidence = {}
        self._revision_epoch = 0
        self._processed_revisions = set()
        self._source_positions, self._revision_pending = {}, set()
        self._deferred_decisions = []
        self._events, self._resolved, self._latest = {}, {}, {}
        self._sequence = 0
        self._source_order = []
        self._transition_result = None
        self.sealed = False

    @property
    def facts(self):
        return self.context.snapshot()["facts"]

    @property
    def terminal(self):
        return bool(self.state and self.wf["states"][self.state].get("terminal"))

    @property
    def done(self):
        return self.terminal and not self.pending and not self._source_order and not self._deferred_decisions

    def _time(self, now):
        if timestamp(now) < timestamp(self.now):
            raise ValueError("Reducer clock cannot move backwards")
        self.now = now

    def _id(self, kind):
        self.counter += 1
        return f"{self.initial['episode_id']}:{kind}:{self.counter:04d}"

    def _log(self, kind, **details):
        entry = {"type": kind, "time": self.now, "state": self.state, "generation": self.generation,
                 "context_version": self.context_version, **copy.deepcopy(details),
                 "prev_hash": self.audit[-1]["entry_hash"] if self.audit else "sha256:" + "0"*64}
        entry["entry_hash"] = digest(entry)
        self.audit.append(entry)

    def _reject_input(self, kind, reason, **identifiers):
        """Only diagnostics are made safe; the rejected input remains unusable."""
        self._log(kind, reason=safe_audit_reason(reason),
                  **{key: safe_audit_id(value) for key, value in identifiers.items()})

    def _commands(self):
        result, self.out = self.out, []
        return copy.deepcopy(result)

    def _queue(self, command):
        self.pending[command["id"]] = copy.deepcopy(command)
        self.out.append(copy.deepcopy(command))

    def start(self, now):
        self._time(now)
        if self.state is not None:
            raise ValueError("Episode already started")
        self._log("EPISODE_CREATED", initial_snapshot_id=self.context.snapshot()["snapshot_id"])
        self._enter(self.wf["initial_state"], {})
        return self._commands()

    def receive(self, event, now):
        self._time(now)
        try:
            validate_event(event)
            fields(event["payload"], {"data_ref", "content_hash"}, where="Source Event payload")
            hash_value(event["payload"]["content_hash"])
            if identity(event) != identity(self.initial):
                raise ValueError("Event identity mismatch")
            if timestamp(event["received_at"]) > timestamp(now):
                raise ValueError("Event received in future")
            if event["payload"]["data_ref"] != event["source_ref"]:
                raise ValueError("Source reference mismatch")
            if not any(matches(item, event) for item in self.wf["subscriptions"]):
                raise ValueError("Event not subscribed")
            eid = event["event_id"]
            if eid in self._events:
                if digest(event) != self._events[eid]:
                    raise ValueError("Conflicting Event ID")
                self._log("EVENT_DUPLICATE", event_id=eid)
                return self._commands()
            if event["sequence_no"] <= self._sequence:
                raise ValueError("Event ingress sequence must increase")
        except ValueError as exc:
            self._reject_input("EVENT_REJECTED", str(exc),
                               event_id=event.get("event_id") if isinstance(event, dict) else None)
            return self._commands()
        self._events[eid] = digest(event)
        self._sequence = event["sequence_no"]
        cid = self._id("source")
        command = {"kind": "resolve", "id": cid, "event": copy.deepcopy(event),
                   "engine_hash": self.bundle["bundle_hash"]}
        self._source_order.append(cid)
        self._source_positions[cid]=event["sequence_no"]
        if event["event_type"] in REVISION_EVENTS.values():self._revision_pending.add(cid)
        self._queue(command)
        self._log("EVENT_ADMITTED", event_id=eid, sequence_no=event["sequence_no"], source_ref=event["source_ref"])
        return self._commands()

    def _env(self, event):
        return {"event": event, "payload": event.get("payload", {}), "result": event.get("result", {}),
                "execution": event.get("execution", {}), "episode": {"state": self.state,
                "context": {name: fact["value"] for name, fact in self.facts.items()}}}

    def _dispatch(self, event):
        if self.terminal:
            return
        for route in self.wf["states"][self.state].get("on_event", []):
            if not matches(route["when"], event):
                continue
            if "guard" in route and not evaluate(route["guard"], self._env(event)):
                self._log("GUARD_BLOCKED", when=route["when"], cause_event_id=event.get("event_id"))
                continue
            self._actions(route.get("do", []), event)
            if "transition" in route:
                self._transition(route["transition"], event)
                break

    def _transition(self, target, event):
        checks = []
        for rule in self.policy["transition_rules"]:
            if rule["from"] not in {"*", self.state} or rule["to"] not in {"*", target}:
                continue
            ok = True
            if "checkpoint" in rule:
                ok = (bool(event.get("validated_hitl")) and
                      event.get("payload", {}).get("checkpoint") == rule["checkpoint"] and
                      event["payload"].get("decision") == rule["decision"])
            if "guard" in rule:
                ok = ok and evaluate(rule["guard"], self._env(event))
            checks.append({"rule": rule["id"], "passed": ok})
        if any(not c["passed"] for c in checks):
            self._log("TRANSITION_DENIED", to=target, guard_results=checks)
            return
        self._log("STATE_TRANSITION", from_state=self.state, to=target,
                  cause_event_id=event.get("event_id"), cause_event_type=event.get("event_type"), guard_results=checks)
        self._enter(target, event)

    def _enter(self, state, event):
        self.state = state
        self.generation += 1
        self.history.append(state)
        self._transition_result = event.get("execution", {}).get("request_id")
        for request in self.open_hitl.values():
            if request["status"] == "OPEN":
                request["status"] = "CLOSED"
        self.waiters = [w for w in self.waiters if w.get("cross_state")]
        self._log("STATE_ENTERED")
        self._actions(self.wf["states"][state].get("on_enter", []), event)
        timeout = self.wf["states"][state].get("timeout")
        if timeout:
            self._timer("state_timeout", timeout["after_min"]*60)

    def _actions(self, actions, event):
        for raw in actions:
            action = normalize_action(raw)
            name = next(iter(action))
            value = action[name]
            if name == "authorize_and_run":
                self._consume({"kind": "agent", "alias": value, "mode": action["mode"],
                               "generation": self.generation, "cause_event_id": event.get("event_id")})
            elif name == "ensure_context":
                rid = value["request_id"] if "request_id" in value else self._id("context")
                self._consume({"kind": "context", "request_id": rid, "scope": value["scope"],
                               "generation": self.generation, "cross_state": True})
            elif name == "request_hitl":
                self._consume({"kind": "hitl", "checkpoint": value, "generation": self.generation})
            elif name == "notify":
                cid = self._id("notification")
                request = {"request_schema": "chain-notification/v0.3", "request_id": cid,
                           "episode_id": self.initial["episode_id"], **copy.deepcopy(value)}
                self._queue({"kind": "notify", "id": cid, "job": {"catalog_hash": self.bundle["catalog_hash"],
                             "request": request, "recorded_at": self.now}, "engine_hash": self.bundle["bundle_hash"]})
            elif name == "apply_effect":
                cid = self._id("effect")
                request = {"request_schema": "chain-effect/v0.3", "request_id": cid,
                           "episode_id": self.initial["episode_id"], "effect_version": self.counter,
                           **copy.deepcopy(value)}
                self._queue({"kind": "effect", "id": cid, "job": {"catalog_hash": self.bundle["catalog_hash"],
                             "request": request, "recorded_at": self.now}, "engine_hash": self.bundle["bundle_hash"]})
            elif name == "re_request_hitl_after_min":
                self._timer("hitl_retry", value*60, checkpoint=event["payload"]["checkpoint"])
            elif name in {"record_reason", "record_hold_reason"}:
                self._log("HITL_REASON_RECORDED", comment=event["payload"]["comment"])
            elif name == "seal_audit_trail":
                self._log("AUDIT_SEAL_REQUESTED")

    def _defer(self, consumer):
        if consumer not in self.waiters:
            self.waiters.append(copy.deepcopy(consumer))

    def _consume(self, consumer):
        if not consumer.get("cross_state") and consumer["generation"] != self.generation:
            return
        kind = consumer["kind"]
        if kind == "agent":
            alias, mode = consumer["alias"], consumer["mode"]
            scope = self.wf["agents"][alias]["scopes"][mode]
            snapshot = scope_snapshot(self.context.snapshot(), scope)
            if self.wf["agents"][alias]["needs_context"]:
                self._summary(consumer, snapshot, "prepare_agent")
            else:
                self._start_agent(alias, mode, snapshot, consumer, "agent")
        elif kind == "context":
            self._summary(consumer, scope_snapshot(self.context.snapshot(), consumer["scope"]), "serve_context")
        elif kind == "hitl":
            cp = consumer["checkpoint"]
            if self.open_hitl.get(cp, {}).get("status") == "OPEN":
                return
            after = self.wf["hitl_checkpoints"][cp]["after"]
            record = self.results.get(after)
            if not record or (record["generation"] != self.generation and
                              record["output"]["request_id"] != self._transition_result):
                self._defer(consumer)
                return
            self._summary(consumer, record["snapshot"], "prepare_hitl",
                          after=after, after_id=record["output"]["request_id"])

    def _summary(self, consumer, snapshot, purpose, **extra):
        alias, mode = self.wf["context_service"]["agent"], self.wf["context_service"]["mode"]
        key = digest({"snapshot": snapshot["snapshot_id"], "scope": sorted(snapshot["facts"]),
                      "manifest": self.wf["agents"][alias]["manifest_hash"]})
        prepared = {"consumer": copy.deepcopy(consumer), "snapshot": copy.deepcopy(snapshot),
                    "summary_key": key, **extra}
        if key in self.summary_cache:
            self._resume_summary(purpose, prepared, self.summary_cache[key])
            return
        self._start_agent(alias, mode, snapshot, prepared, purpose)

    def _start_agent(self, alias, mode, snapshot, consumer, purpose):
        key = (alias, mode, snapshot["snapshot_id"], self.generation if purpose == "agent" else None, self.now)
        for pending in self.pending.values():
            if pending.get("single_flight") == key and pending.get("purpose") == purpose:
                if consumer not in pending["consumers"]:
                    pending["consumers"].append(copy.deepcopy(consumer))
                return
        cid = self._id("agent")
        spec = self.wf["agents"][alias]
        job = make_scoped_job(self.bundle["catalog_hash"], alias, spec, spec["manifest_hash"],
                              snapshot, list(snapshot["facts"]), request_id=cid, mode=mode, evaluated_at=self.now)
        command = {"kind": "agent", "id": cid, "alias": alias, "mode": mode, "purpose": purpose,
                   "job": job, "generation": self.generation, "timeout_s": spec["timeout_s"],
                   "revision_epoch":self._revision_epoch,
                   "consumers": [copy.deepcopy(consumer)], "single_flight": key, "engine_hash": self.bundle["bundle_hash"]}
        if purpose == "agent":
            self._latest[f"{alias}.{mode}"] = cid
        self.agent_runs[alias] = self.agent_runs.get(alias, 0) + 1
        self._queue(command)
        self._log("AGENT_EXECUTION_REQUESTED", request_id=cid, alias=alias, mode=mode,
                  input_snapshot_id=job["snapshot"]["snapshot_id"], scope=job["request"]["scope"])

    def _resume_summary(self, purpose, prepared, record):
        consumer = prepared["consumer"]
        if not consumer.get("cross_state") and consumer["generation"] != self.generation:
            return
        if purpose == "prepare_agent":
            self._start_agent(consumer["alias"], consumer["mode"], prepared["snapshot"], consumer, "agent")
        elif purpose == "serve_context":
            self.context_requests[consumer["request_id"]] = copy.deepcopy(record)
            self._log("STRUCTURED_CONTEXT_SERVED", request_id=consumer["request_id"],
                      input_snapshot_id=record["snapshot"]["snapshot_id"])
        elif purpose == "prepare_hitl":
            after = self.results.get(prepared["after"])
            if not after or after["output"]["request_id"] != prepared["after_id"]:
                self._consume(consumer)
                return
            self._open_hitl(consumer["checkpoint"], after, record)

    def _open_hitl(self, checkpoint, after, summary):
        if self.open_hitl.get(checkpoint, {}).get("status") == "OPEN":
            return
        spec, snapshot = self.wf["hitl_checkpoints"][checkpoint], after["snapshot"]
        refs = {digest(ref): copy.deepcopy(ref) for fact in snapshot["facts"].values()
                for ref in fact.get("dependencies", [fact["source_ref"]])}
        results = [after["output"], summary["output"]]
        request = {"contract_schema": "chain-hitl-request/v0.3", "request_id": self._id("hitl"),
                   "checkpoint": checkpoint, "evidence_snapshot_id": snapshot["snapshot_id"],
                   "dependencies": [refs[k] for k in sorted(refs)],
                   "result_ids": list(dict.fromkeys(r["request_id"] for r in results)),
                   **{key: copy.deepcopy(spec[key]) for key in
                      ("roles", "options", "required_confirmations", "confirmations_on_decisions", "question")}}
        by_id = {r["request_id"]: r for r in results}
        validate_hitl_request(request, snapshot, list(by_id.values()))
        self.open_hitl[checkpoint] = {"status": "OPEN", "state": self.state, "generation": self.generation,
                                     "request": request, "snapshot": copy.deepcopy(snapshot),
                                     "results": copy.deepcopy(list(by_id.values())), "requested_at": self.now}
        self.hitl_history[request["request_id"]]=self.open_hitl[checkpoint]
        self._log("HITL_REQUESTED", checkpoint=checkpoint, request_id=request["request_id"],
                  evidence_snapshot_id=snapshot["snapshot_id"], result_ids=request["result_ids"])

    def decide(self, decision, now):
        self._time(now)
        try:
            validate_hitl_decision_shape(decision)
        except ValueError as exc:
            self._reject_input("HITL_REJECTED", str(exc),
                               request_id=decision.get("request_id") if isinstance(decision, dict) else None)
            return self._commands()
        if self._revision_pending:
            self._deferred_decisions.append({"decision":copy.deepcopy(decision),"through_sequence":self._sequence})
            self._log("HITL_DECISION_DEFERRED_FOR_SOURCE",request_id=decision["request_id"],
                      through_sequence=self._sequence)
        else:self._decide_now(decision)
        return self._commands()

    def _decide_now(self,decision):
        if not isinstance(decision,dict):
            self._reject_input("HITL_REJECTED", "Decision must be an object")
            return
        try:
            text(decision.get("request_id"),"Decision request_id")
            record = self.hitl_history.get(decision["request_id"])
            if not record or record["status"] != "OPEN" or record["generation"] != self.generation:
                raise ValueError("No matching open request")
            request = record["request"]
            validate_hitl_decision(decision, request)
            if timestamp(decision["recorded_at"]) > timestamp(self.now):
                raise ValueError("Decision recorded in future")
            if timestamp(decision["recorded_at"]) < timestamp(record["requested_at"]):
                raise ValueError("Decision precedes its Request")
            rule = self.policy["hitl_rules"][request["checkpoint"]]
            if decision["actor"]["role"] not in rule["roles"]:
                raise ValueError("Policy role denied")
            if decision["decision"] in rule.get("confirmations_on_decisions", []) and not set(
                    rule.get("required_confirmations", [])).issubset(decision["confirmed_items"]):
                raise ValueError("Policy confirmations missing")
        except ValueError as exc:
            self._reject_input("HITL_REJECTED", str(exc), request_id=decision.get("request_id"))
            return
        record["status"] = "DECIDED"
        self._log("HITL_DECISION_RECORDED", **decision)
        self._dispatch({"event_type": "HITL_DECISION", "payload": {**copy.deepcopy(decision),
                        "checkpoint": request["checkpoint"]}, "validated_hitl": True})

    def _flush_decisions(self):
        while self._deferred_decisions:
            item=self._deferred_decisions[0]
            if any(sequence<=item["through_sequence"] for sequence in self._source_positions.values()):return
            self._deferred_decisions.pop(0)
            self._decide_now(item["decision"])

    def request_context(self, request_id, scope, now):
        self._time(now)
        if not self.terminal:
            try:
                json_value({"request_id": request_id, "scope": scope}, "Context request")
                text(request_id)
                strings(scope, "scope", nonempty=True)
                self._consume({"kind": "context", "request_id": request_id, "scope": scope,
                               "generation": self.generation, "cross_state": True})
            except ValueError as exc:
                self._reject_input("CONTEXT_REQUEST_REJECTED", str(exc), request_id=request_id)
        return self._commands()

    def _timer(self, purpose, seconds, **extra):
        timer = {"kind": "timer", "id": self._id("timer"), "purpose": purpose, "seconds": seconds,
                 "generation": self.generation, **extra}
        self.timers[timer["id"]] = copy.deepcopy(timer)
        self.out.append(timer)

    def timer_fired(self, timer, now):
        self._time(now)
        original = self.timers.pop(timer["id"], None)
        if original != timer or timer["generation"] != self.generation or self.terminal:
            return self._commands()
        if timer["purpose"] == "hitl_retry":
            self._consume({"kind": "hitl", "checkpoint": timer["checkpoint"], "generation": self.generation})
        else:
            self._log("STATE_TIMEOUT", auto_transition=False)
            timeout = self.wf["states"][self.state]["timeout"]
            self._actions(timeout["do"], {})
            if timeout.get("repeat"):
                self._timer("state_timeout", timeout["after_min"]*60)
        return self._commands()

    def complete(self, cid, reply, now):
        self._time(now)
        command = self.pending.pop(cid, None)
        if not command:
            self._log("COMPLETION_DUPLICATE_OR_UNKNOWN", request_id=cid)
            return self._commands()
        kind = command["kind"]
        if kind == "resolve":
            try:
                validate_completion(reply)
                event = command["event"]
                if (any(reply[k] != event[k] for k in
                        ("event_id", "event_type", "sequence_no", "source_ref", "published_at", "received_at"))
                        or identity(reply) != identity(event) or
                        reply["content_hash"] != event["payload"]["content_hash"] or
                        reply["source_time"] != event["source_event_time"]):
                    raise ValueError("Context completion does not match admitted Event")
                self._resolved[cid] = (command, copy.deepcopy(reply))
            except (ValueError, KeyError, TypeError) as exc:
                self._resolved[cid] = (command, None)
                self._log("CONTEXT_RESOLUTION_FAILED", event_id=command["event"]["event_id"], reason=str(exc))
            self._apply_ready()
        elif kind == "notify":
            request = command["job"]["request"]
            if (reply.get("status") != "MOCK_RECORDED" or reply.get("request_id") != cid or
                    reply.get("payload_hash") != digest(request)):
                self._log("NOTIFICATION_FAILED_OR_REJECTED", request_id=cid)
            else:
                self.notifications.append(copy.deepcopy(reply))
                self._log("MOCK_RECORDED", **reply)
        elif kind == "effect":
            try:
                validate_effect_receipt(reply, command["job"]["request"])
                if not timestamp(command["job"]["recorded_at"]) <= timestamp(reply["recorded_at"]) <= timestamp(now):
                    raise ValueError("Effect receipt time outside request/completion interval")
            except (ValueError, KeyError, TypeError) as exc:
                self._reject_input("EFFECT_FAILED_OR_REJECTED", str(exc), request_id=cid)
            else:
                self.effects.append(copy.deepcopy(reply))
                self._log("EFFECT_RECORDED", **reply)
        else:
            self._complete_agent(command, reply)
        return self._commands()

    def _apply_ready(self):
        while self._source_order and self._source_order[0] in self._resolved:
            cid = self._source_order.pop(0)
            command, completion = self._resolved.pop(cid)
            self._source_positions.pop(cid)
            self._revision_pending.discard(cid)
            if completion is not None:
                try:self.context.apply(completion, applied_at=self.now)
                except ValueError as exc:
                    self._log("CONTEXT_RESOLUTION_FAILED",event_id=completion["event_id"],reason=str(exc))
                else:
                    self.context_version += 1
                    self._log("CONTEXT_APPLIED", event_id=completion["event_id"], source_ref=completion["source_ref"],
                              snapshot_id=self.context.snapshot()["snapshot_id"])
                    if "revision" in completion:self._apply_revision(completion)
                    # Internal routing view; the admitted wire Event remains untouched.
                    self._dispatch({**command["event"], "payload": copy.deepcopy(completion["payload"])})
            self._flush_decisions()

    def _apply_revision(self,completion):
        key=digest({"ref":completion["source_ref"],"hash":completion["content_hash"]})
        if key in self._processed_revisions:
            self._log("SOURCE_REVISION_DUPLICATE",event_id=completion["event_id"])
            return
        self._processed_revisions.add(key)
        revision=completion["revision"]
        affected={digest(ref) for target in revision["targets"] for ref in self.context.evidence.lineage(target)}
        self._revision_epoch+=1
        self._invalidated_evidence.update({key:self._revision_epoch for key in affected})
        def impacted(record):
            return bool(affected & {digest(ref) for fact in record["snapshot"]["facts"].values()
                                   for ref in fact.get("dependencies",[fact["source_ref"]])})
        invalid_results={key:record for key,record in self.results.items() if impacted(record)}
        for key,record in invalid_results.items():
            del self.results[key]
            self._log("AGENT_EVIDENCE_INVALIDATED",request_id=record["output"]["request_id"],revision_event_id=completion["event_id"])
        for key,record in list(self.summary_cache.items()):
            if impacted(record):del self.summary_cache[key]
        reconfirm=set()
        for checkpoint,record in self.open_hitl.items():
            if record["generation"]!=self.generation:continue
            refs={digest(ref) for ref in record["request"]["dependencies"]}
            if record["status"]=="OPEN" and refs & affected:
                record["status"]="INVALIDATED"
                record["invalidated_by"]={"event_id":completion["event_id"],"revision":copy.deepcopy(revision),"processed_at":self.now}
                self._log("HITL_INVALIDATED",request_id=record["request"]["request_id"],checkpoint=checkpoint,
                          revision_event_id=completion["event_id"],targets=revision["targets"])
                reconfirm.add(checkpoint)
        # A request may still be preparing its Summary when its accepted result
        # is invalidated. Retry only declared checkpoint work in this generation.
        for command in self.pending.values():
            if command.get("purpose")=="prepare_hitl":
                for prepared in command["consumers"]:
                    consumer=prepared["consumer"]
                    if consumer["generation"]==self.generation and prepared["after"] in invalid_results:
                        reconfirm.add(consumer["checkpoint"])
        for checkpoint in sorted(reconfirm):
            self._consume({"kind":"hitl","checkpoint":checkpoint,"generation":self.generation})
        retry={t["checkpoint"] for t in self.timers.values() if t["purpose"]=="hitl_retry" and t["generation"]==self.generation}
        for checkpoint in sorted(reconfirm|retry):
            after=self.wf["hitl_checkpoints"][checkpoint]["after"]
            if after in invalid_results:
                alias,_,mode=after.rpartition(".")
                # The configured `after` authorizes re-evaluation for this HITL;
                # it does not add a clinical State transition or acceptance gate.
                self._consume({"kind":"agent","alias":alias,"mode":mode,"generation":self.generation,
                               "cause_event_id":completion["event_id"]})
        self._log("SOURCE_REVISION_PROCESSED",event_id=completion["event_id"],revision_kind=revision["kind"],
                  targets=revision["targets"],reconfirmation_checkpoints=sorted(reconfirm))

    def _complete_agent(self, command, reply):
        try:
            validate_agent_result(reply, command["job"]["request"])
            if reply["status"] != "SUCCESS":
                raise ValueError(reply["error"])
            output = self.wf["agents"][command["alias"]]["outputs"][command["mode"]]
            fields(reply["result"], set(output), where="Declared Agent output")
            if command["purpose"] != "agent" and reply["result"]["structured_context"] != command["job"]["snapshot"]["facts"]:
                raise ValueError("Structured Context exceeds/changes requested source scope")
        except (ValueError, KeyError, TypeError) as exc:
            self._log("AGENT_FAILED_OR_REJECTED", request_id=command["id"], reason=str(exc))
            return
        reason = None
        if any(self._invalidated_evidence.get(digest(ref),0)>command["revision_epoch"]
               for ref in command["job"]["request"]["dependencies"]):
            reason="EXPLICIT_EVIDENCE_INVALIDATED"
        elif command["purpose"] == "agent":
            key = f"{command['alias']}.{command['mode']}"
            if command["generation"] != self.generation:
                reason = "STATE_GENERATION_CHANGED"
            elif scope_snapshot(self.context.snapshot(), command["job"]["request"]["scope"])["snapshot_id"] != reply["input_snapshot_id"]:
                reason = "SCOPED_INPUT_CHANGED"
            elif self._latest.get(key) != command["id"]:
                reason = "SUPERSEDED_REQUEST"
        elif command["purpose"] != "prepare_hitl" and scope_snapshot(
                self.context.snapshot(), command["job"]["request"]["scope"])["snapshot_id"] != reply["input_snapshot_id"]:
            reason = "SCOPED_INPUT_CHANGED"
        if reason:
            self._log("STALE_RESULT_DISCARDED", request_id=command["id"], reason=reason)
            if command["purpose"] != "agent":
                for prepared in command["consumers"]:
                    self._consume(prepared["consumer"])
            elif reason in {"SCOPED_INPUT_CHANGED","EXPLICIT_EVIDENCE_INVALIDATED"} and command["generation"]==self.generation and self._latest.get(
                    f"{command['alias']}.{command['mode']}")==command["id"]:
                # A relevant source may change without declaring another invocation.
                # Refresh the same permitted work; never retry old-State work.
                self._consume(command["consumers"][0])
            return
        record = {"output": copy.deepcopy(reply), "snapshot": copy.deepcopy(command["job"]["snapshot"]),
                  "generation": command["generation"], "alias": command["alias"], "mode": command["mode"]}
        self.agent_outputs[command["id"]] = copy.deepcopy(record)
        self._log("AGENT_RESULT_ACCEPTED", request_id=command["id"], alias=command["alias"],
                  mode=command["mode"], input_snapshot_id=reply["input_snapshot_id"])
        if command["purpose"] == "agent":
            self.results[f"{command['alias']}.{command['mode']}"] = record
            self._dispatch({"event_type": "AGENT_RESULT_AVAILABLE", "agent": command["alias"],
                            "mode": command["mode"], "result": copy.deepcopy(reply["result"]),
                            "execution": {"status": "SUCCESS", "request_id": reply["request_id"]}})
        else:
            for prepared in command["consumers"]:
                self.summary_cache[prepared["summary_key"]] = copy.deepcopy(record)
                self._resume_summary(command["purpose"], prepared, record)
        waiting, self.waiters = self.waiters, []
        for consumer in waiting:
            self._consume(consumer)

    def snapshot(self):
        result={"episode_id": self.initial["episode_id"], "state": self.state,
            "state_history": self.history, "generation": self.generation, "context_version": self.context_version,
            "facts": self.facts, "source_snapshot": self.context.snapshot(), "fact_history": self.context.history,
            "agent_runs": self.agent_runs, "latest_results": self.results, "agent_outputs": self.agent_outputs,
            "open_hitl": self.open_hitl, "context_requests": self.context_requests,
            "hitl_history":self.hitl_history,"deferred_decision_count":len(self._deferred_decisions),
            "notifications": self.notifications, "audit": self.audit, "pending_count": len(self.pending),
            "unresolved_count": len(self._source_order), "waiter_count": len(self.waiters), "done": self.done,
            "bundle_hash": self.bundle["bundle_hash"], "run_manifest": self.bundle["run_manifest"]}
        if "allowed_effect_operations" in self.policy:
            result["effects"]=self.effects
        if self.state and "specification" in self.wf["states"][self.state]:
            result["state_specification"]=state_specification_view(self.wf["states"][self.state],
                    self.wf,self.facts,self.now,self.policy,state_id=self.state)
        return copy.deepcopy(result)

    def finish(self):
        if not self.done:
            raise ValueError("Cannot seal an incomplete episode")
        if not self.sealed:
            self._log("AUDIT_TRAIL_SEALED", demo_hash_chain_only=True)
            self.sealed = True
        return self.snapshot()
