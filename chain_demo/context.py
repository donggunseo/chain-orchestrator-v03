"""Pure source Context ledger, separate from clinical State/Agent/HITL execution."""
import copy

from .contracts import make_snapshot
from .evidence import EvidenceIndex
from .source_contracts import identity, validate_completion, validate_initial
from .validation import canonical_hash, timestamp


class ContextLedger:
    def __init__(self, initial, *, known_at):
        validate_initial(initial)
        if timestamp(known_at) < timestamp(initial["arrival_time"]):
            raise ValueError("Initial Context precedes arrival")
        self.identity = identity(initial)
        ref = {"system": "CHAIN_INITIAL", "record_id": initial["episode_id"], "version": 1}
        facts = {name: {"value": copy.deepcopy(value), "status": "AVAILABLE", "source_ref": {**ref,"field": name},
                        "source_time": initial["arrival_time"], "known_at": known_at} for name,value in initial.items()}
        self._current = {("CHAIN_INITIAL", initial["episode_id"]): {"version": 1, "facts": facts}}
        self._facts = facts
        self.evidence = EvidenceIndex(facts)
        self._invalidated_refs = set()
        self._known_at = known_at
        self._events = {}
        self._sources = {}
        self._history = []

    @property
    def history(self):
        return copy.deepcopy(self._history)

    def snapshot(self):
        return make_snapshot(self._facts, self._known_at)

    def apply(self, completion, *, applied_at):
        validate_completion(completion)
        if identity(completion) != self.identity:
            raise ValueError("Context resolution identity mismatch")
        if timestamp(applied_at) < max(timestamp(completion["received_at"]), timestamp(self._known_at)):
            raise ValueError("Context application precedes receipt/current Context")
        eid = completion["event_id"]
        if eid in self._events:
            if self._events[eid] != completion["completion_hash"]:
                raise ValueError("Conflicting Context completion for Event ID")
            return self.snapshot()
        ref = completion["source_ref"]
        source_key = (ref["system"], ref["record_id"], ref["version"])
        if source_key in self._sources and self._sources[source_key] != completion["content_hash"]:
            raise ValueError("Conflicting Context source version")
        record_key=source_key[:2]
        current=self._current.get(record_key)
        prepared=self.evidence.prepare(completion["facts"])
        if "revision" in completion and source_key not in self._sources:
            if current is None or ref["version"]<=current["version"]:
                raise ValueError("Revision must advance the current source version")
            for target in completion["revision"]["targets"]:self.evidence.lineage(target)
            targeted={target["field"] for target in completion["revision"]["targets"]}
            def content(fact):
                if fact is None:return None
                data={k:v for k,v in fact.items() if k not in {"known_at","source_ref","dependencies"}}
                deps=[r for r in fact.get("dependencies",[]) if r!=fact["source_ref"]]
                if deps:data["dependencies"]=deps
                return data
            for name,fact in current["facts"].items():
                if name not in targeted and content(fact)!=content(prepared.get(name)):
                    raise ValueError("Revision changes an existing field outside its explicit targets")
        self._events[eid] = completion["completion_hash"]
        self._history.append({"completion": copy.deepcopy(completion), "applied_at": applied_at})
        if source_key in self._sources:
            return self.snapshot()
        self._sources[source_key] = completion["content_hash"]
        self.evidence.commit(prepared)
        if current is None or ref["version"] > current["version"]:
            facts = prepared
            for fact in facts.values():
                fact["known_at"] = applied_at
            self._current[record_key] = {"version": ref["version"], "facts": facts}
            if "revision" in completion:
                affected={canonical_hash(r) for target in completion["revision"]["targets"] for r in self.evidence.lineage(target)}
                self._invalidated_refs.update(affected)
            # Also cover later derived publications that still refer to invalid
            # historical input. A new record ID/version cannot make it usable.
            for item in self._current.values():
                for fact in item["facts"].values():
                    refs={canonical_hash(r) for r in fact.get("dependencies",[])}
                    if refs & self._invalidated_refs and fact["status"]!="INVALIDATED":
                        fact.update(value=None,status="INVALIDATED",known_at=applied_at)
            self._rebuild()
        self._known_at = applied_at
        return self.snapshot()

    def _rebuild(self):
        candidates = {}
        for key in sorted(self._current):
            for name, fact in self._current[key]["facts"].items():
                candidates.setdefault(name, []).append(fact)
        self._facts = {}
        for name, facts in candidates.items():
            if len(facts) == 1:
                self._facts[name] = copy.deepcopy(facts[0]); continue
            dependencies = list({canonical_hash(r):copy.deepcopy(r) for f in facts
                                 for r in f.get("dependencies",[f["source_ref"]])}.values())
            signatures = {canonical_hash({k:f[k] for k in ("value","status","unit","confirmation_status") if k in f}) for f in facts}
            known_at = max((f["known_at"] for f in facts), key=timestamp)
            if len(signatures) == 1:
                combined = copy.deepcopy(facts[0])
            else:
                # Preserve both observations in history; do not choose a clinical winner.
                combined = {"value": None, "status": "CONFLICT", "source_ref": {
                    "system": "CHAIN_CONTEXT", "record_id": canonical_hash(dependencies), "version": 1, "field": name},
                    "source_time": max((f["source_time"] for f in facts), key=timestamp)}
            combined.update(known_at=known_at, dependencies=dependencies)
            self._facts[name] = combined
