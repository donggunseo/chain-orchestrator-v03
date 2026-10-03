"""Closed, pure v0.3 Workflow DSL. No configuration/registry/file imports."""
import copy

from .expressions import validate_expression
from .validation import boolean, canonical_hash, fields, hash_value, positive, strings, text

ACTIONS = {"authorize_and_run", "ensure_context", "request_hitl", "notify", "record_reason",
           "record_hold_reason", "re_request_hitl_after_min", "seal_audit_trail"}


def normalize_action(action):
    action = {action:None} if isinstance(action,str) else copy.deepcopy(action)
    if not isinstance(action,dict):
        raise ValueError("Action requires a mapping or name")
    names = set(action) & ACTIONS
    if len(names)!=1:
        raise ValueError("Unknown/ambiguous Action")
    name = next(iter(names))
    optional = {"authorize_and_run":{"mode"}, "request_hitl":{"after"}}.get(name,set())
    fields(action,{name},optional,"v0.3 Action")
    value=action[name]
    if name=="authorize_and_run":
        text(value);text(action.get("mode"),"explicit Agent mode")
    elif name=="request_hitl":
        text(value)
        if "after" in action:text(action["after"])
    elif name=="ensure_context":
        fields(value,{"scope"},{"request_id"},"Context action")
        strings(value["scope"],nonempty=True)
        if "request_id" in value:text(value["request_id"])
    elif name=="notify":
        fields(value,{"template","channel","recipient"},where="Notification")
        for item in value.values():text(item)
        if value["channel"] not in {"CONSOLE","LOG"}:raise ValueError("Only mock channels supported")
    elif name=="re_request_hitl_after_min":positive(value)
    elif value is not None:raise ValueError("Action takes no argument")
    return {name:value,**{key:action[key] for key in sorted(optional & action.keys())}}


def validate_engine_bundle(bundle):
    fields(bundle,{"engine_schema","workflow","policy","catalog_hash","run_manifest","bundle_hash"},where="Engine bundle")
    if bundle["engine_schema"]!="chain-orchestrator/v0.3":raise ValueError("Unsupported Engine schema")
    hash_value(bundle["catalog_hash"])
    hash_value(bundle["bundle_hash"])
    if canonical_hash({k:v for k,v in bundle.items() if k!="bundle_hash"})!=bundle["bundle_hash"]:
        raise ValueError("Engine bundle hash mismatch")
    wf=fields(bundle["workflow"],{"workflow_id","version","site_id","initial_state","subscriptions","context_service","agents","states","hitl_checkpoints"},where="Workflow")
    for key in ("workflow_id","version","site_id","initial_state"):text(wf[key])
    context=fields(wf["context_service"],{"agent","mode","available_states"},where="Context service")
    text(context["agent"]);text(context["mode"])
    if context["available_states"]!="*":raise ValueError("Context service supports all active States only")
    agents=wf["agents"];states=wf["states"];checkpoints=wf["hitl_checkpoints"]
    for obj in (agents,states,checkpoints):
        if not isinstance(obj,dict):raise ValueError("Named mappings required")
        for key in obj:text(key)
    if not agents or not states or wf["initial_state"] not in states:raise ValueError("Missing initial State/Agent mapping")
    policy=fields(bundle["policy"],{"policy_id","version","site_id","maximum_attempts","approved_agents","allowed_actions_for_agents",
                  "forbidden_actions_for_agents","transition_rules","notification_templates","hitl_rules"},where="Engine Policy")
    for key in ("policy_id","version","site_id"):text(policy[key])
    if wf["site_id"]!=policy["site_id"]:raise ValueError("Workflow/Policy site mismatch")
    positive(policy["maximum_attempts"],integer=True)
    if policy["maximum_attempts"]>2:raise ValueError("At most two Activity attempts supported")
    for key in ("allowed_actions_for_agents","forbidden_actions_for_agents","notification_templates"):strings(policy[key])
    if set(policy["allowed_actions_for_agents"]) & set(policy["forbidden_actions_for_agents"]):raise ValueError("Policy actions overlap")
    for key in ("approved_agents","hitl_rules"):
        if not isinstance(policy[key],dict):raise ValueError(f"Policy {key} requires a mapping")
    for approval in policy["approved_agents"].values():
        fields(approval,{"versions","data_scopes"},where="Agent approval")
        strings(approval["versions"],nonempty=True);strings(approval["data_scopes"])
    for alias,spec in agents.items():
        fields(spec,{"agent_id","version","manifest_hash","data_scopes","actions","modes","scopes","timeout_s","needs_context","outputs"},where="Agent binding")
        for key in ("agent_id","version"):text(spec[key])
        hash_value(spec["manifest_hash"]);positive(spec["timeout_s"]);boolean(spec["needs_context"])
        for key in ("data_scopes","actions","modes"):strings(spec[key],nonempty=True)
        if "." in alias:raise ValueError("Agent alias cannot contain a dot")
        for key in ("scopes","outputs"):
            if not isinstance(spec[key],dict):raise ValueError(f"Agent {key} requires a mapping")
        if set(spec["scopes"])!=set(spec["modes"]) or set(spec["outputs"])!=set(spec["modes"]):raise ValueError("Every mode requires explicit scope/output fields")
        for scope in spec["scopes"].values():strings(scope,nonempty=True)
        for output in spec["outputs"].values():strings(output,nonempty=True)
        approval=policy["approved_agents"].get(spec["agent_id"])
        if not approval or spec["version"] not in approval["versions"]:raise ValueError("Agent version not approved")
        if not set(spec["data_scopes"]).issubset(approval["data_scopes"]):raise ValueError("Agent scope not approved")
        if not set(spec["actions"]).issubset(policy["allowed_actions_for_agents"]) or set(spec["actions"]) & set(policy["forbidden_actions_for_agents"]):raise ValueError("Agent action not approved")
    if context["agent"] not in agents or context["mode"] not in agents[context["agent"]]["modes"]:
        raise ValueError("Undefined Context implementation/mode")
    if agents[context["agent"]]["needs_context"]:raise ValueError("Context service cannot recursively require itself")
    def result_reference(value):
        alias,sep,mode=text(value).rpartition(".")
        if not sep or alias not in agents or mode not in agents[alias]["modes"]:raise ValueError("Undefined result reference")
        return alias,mode
    def pattern(value):
        if not isinstance(value,dict) or not value:raise ValueError("Nonempty Event pattern required")
        for key,expected in value.items():
            text(key)
            if any(not part for part in key.split(".")):raise ValueError("Invalid Event path")
            values=expected if isinstance(expected,list) else [expected]
            if not values or any(type(v) not in (str,bool,int,float,type(None)) for v in values):raise ValueError("Scalar Event pattern required")
    if not isinstance(wf["subscriptions"],list) or not wf["subscriptions"]:raise ValueError("Subscriptions required")
    for item in wf["subscriptions"]:pattern(item)
    for checkpoint,spec in checkpoints.items():
        fields(spec,{"roles","options","required_confirmations","confirmations_on_decisions","after","question"},where="Checkpoint")
        for key in ("roles","options"):strings(spec[key],nonempty=True)
        for key in ("required_confirmations","confirmations_on_decisions"):strings(spec[key])
        if set(spec["confirmations_on_decisions"])-set(spec["options"]):raise ValueError("Unknown confirmation decision")
        text(spec["question"]);result_reference(spec["after"])
        rule=policy["hitl_rules"].get(checkpoint)
        if not rule:raise ValueError("Checkpoint requires explicit Policy role approval")
        fields(rule,{"roles"},{"confirmations_on_decisions","required_confirmations"},"HITL Policy")
        strings(rule["roles"],nonempty=True)
        for key in ("confirmations_on_decisions","required_confirmations"):
            strings(rule.get(key,[]))
        if not set(spec["roles"]).issubset(rule["roles"]):raise ValueError("Workflow cannot widen Policy roles")
        if not set(rule.get("required_confirmations",[])).issubset(spec["required_confirmations"]):raise ValueError("Workflow cannot remove Policy confirmations")
        if not set(rule.get("confirmations_on_decisions",[])).issubset(spec["confirmations_on_decisions"]):raise ValueError("Workflow cannot remove Policy confirmation decisions")
    if set(policy["hitl_rules"])-set(checkpoints):raise ValueError("Undefined Policy checkpoint")
    def actions(items, *, decision_route=False):
        if not isinstance(items,list):raise ValueError("Action list required")
        for raw in items:
            item=normalize_action(raw);name=next(iter(item));value=item[name]
            if name in {"record_reason","record_hold_reason","re_request_hitl_after_min"} and not decision_route:
                raise ValueError("Human-decision Action requires a HITL_DECISION route")
            if name=="authorize_and_run" and (value not in agents or item["mode"] not in agents[value]["modes"]):raise ValueError("Unknown Agent/mode")
            if name=="request_hitl":
                if value not in checkpoints:raise ValueError("Unknown checkpoint")
                if "after" in item and item["after"]!=checkpoints[value]["after"]:raise ValueError("HITL after mismatch")
            if name=="notify" and value["template"] not in policy["notification_templates"]:raise ValueError("Unapproved template")
    for state,spec in states.items():
        fields(spec,set(),{"name","terminal","on_enter","on_event","timeout"},"State")
        if "name" in spec:text(spec["name"])
        if "terminal" in spec:boolean(spec["terminal"])
        if spec.get("terminal") and (spec.get("on_event") or "timeout" in spec):
            raise ValueError("Terminal State cannot declare unused routes/timeouts")
        actions(spec.get("on_enter",[]))
        if not isinstance(spec.get("on_event",[]),list):raise ValueError("Route list required")
        for route in spec.get("on_event",[]):
            fields(route,{"when"},{"guard","do","transition"},"Route")
            pattern(route["when"])
            event_type=route["when"].get("event_type")
            actions(route.get("do",[]),decision_route=event_type=="HITL_DECISION" or event_type==["HITL_DECISION"])
            if "guard" in route:validate_expression(route["guard"])
            if "transition" in route:
                text(route["transition"])
                if route["transition"] not in states:raise ValueError("Undefined State")
        if "timeout" in spec:
            timeout=fields(spec["timeout"],{"after_min","do"},{"repeat"},"Timeout")
            positive(timeout["after_min"]);actions(timeout["do"])
            if "repeat" in timeout:boolean(timeout["repeat"])
    ids=[]
    if not isinstance(policy["transition_rules"],list):raise ValueError("Transition rules require a list")
    for rule in policy["transition_rules"]:
        fields(rule,{"id","from","to"},{"checkpoint","decision","guard"},"Transition Policy")
        ids.append(text(rule["id"]))
        for key in ("from","to","checkpoint","decision"):
            if key in rule:text(rule[key])
        if any(rule[key] not in states and rule[key]!="*" for key in ("from","to")):raise ValueError("Undefined Policy State")
        if ("checkpoint" in rule)!=("decision" in rule):raise ValueError("Policy checkpoint/decision must be paired")
        if "checkpoint" in rule and (rule["checkpoint"] not in checkpoints or rule["decision"] not in checkpoints[rule["checkpoint"]]["options"]):raise ValueError("Undefined Policy decision")
        if "guard" in rule:validate_expression(rule["guard"])
    if len(ids)!=len(set(ids)):raise ValueError("Duplicate Policy rule")
