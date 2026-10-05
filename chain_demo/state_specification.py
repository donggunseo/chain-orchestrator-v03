"""Pure State authoring compiler and evidence views; no I/O or clinical Gates.

The thirteen headings match the supplied State specification viewer. Only
capabilities, routes and timeout Actions compile to execution. Requirement,
condition, freshness and prose annotations only describe evidence for a view.
"""
from __future__ import annotations

import copy

from .expressions import evaluate, validate_expression
from .validation import boolean, fields, json_value, positive, strings, text, timestamp


STATE_SPEC_SCHEMA = "chain-state-spec/v1"
STATE_SPEC_VERSION = "1"
STATE_SPEC_ITEMS = (
    "State", "Clinical Goal and Scope", "Entry Trigger", "Incoming Data / Evidence",
    "Data requirement", "Required Capability", "Agent Input", "Expected Agent Output",
    "HITL", "Guard / Transition condition", "Next State / Branch",
    "Timer / Exception / Fail-safe", "Safety / Audit",
)
_DERIVED_KEYS = {
    "Agent Input": "agents", "Expected Agent Output": "agents",
    "HITL": "checkpoints", "Next State / Branch": "branches",
}
_WORKFLOW_KEYS = {
    "workflow_id", "version", "site_id", "initial_state", "subscriptions",
    "context_service", "agents", "states", "hitl_checkpoints", "state_spec_schema",
}


def _pattern(pattern):
    if not isinstance(pattern, dict) or not pattern:
        raise ValueError("State specification: Event pattern required")
    for path, value in pattern.items():
        text(path, "Event pattern path")
        if path.startswith("$"):
            raise ValueError("Event patterns use paths without $")
        values = value if isinstance(value, list) else [value]
        if not values or any(type(item) not in (str, bool, int, float, type(None)) for item in values):
            raise ValueError("Scalar Event pattern required")
        json_value(values)


def _actions(actions, *, decision_route=False):
    # Deferred import keeps this module usable from the pure runtime validator.
    from .orchestration_schema import normalize_action

    if not isinstance(actions, list):
        raise ValueError("Action list required")
    for action in actions:
        normalized = normalize_action(action)
        name = next(iter(normalized))
        if name in {"record_reason", "record_hold_reason", "re_request_hitl_after_min"} and not decision_route:
            raise ValueError("Human-decision Action requires a HITL_DECISION route")


def _requirements(items):
    if not isinstance(items, list):
        raise ValueError("Data requirement: list required")
    seen = set()
    for item in items:
        fields(item, {"field", "requirement", "description"}, {"condition", "freshness"}, "Data requirement")
        field = text(item["field"], "Data requirement field")
        text(item["description"], "Data requirement description")
        if field in seen:
            raise ValueError("Duplicate Data requirement field")
        seen.add(field)
        if type(item["requirement"]) is not str or item["requirement"] not in {"MANDATORY", "OPTIONAL", "CONDITIONAL"}:
            raise ValueError("Data requirement: unknown requirement")
        if item["requirement"] == "CONDITIONAL" and "condition" not in item:
            raise ValueError("Conditional Data requirement needs a condition")
        if "condition" in item:
            if item["requirement"] != "CONDITIONAL":
                raise ValueError("Data requirement condition requires CONDITIONAL")
            validate_expression(item["condition"])
        if "freshness" in item:
            freshness = fields(item["freshness"], {"max_age_s"}, {"time_field"}, "Data requirement freshness")
            positive(freshness["max_age_s"], "max_age_s")
            if freshness.get("time_field", "source_time") not in {"source_time", "known_at"}:
                raise ValueError("freshness time_field must be source_time or known_at")


def _policy(specification, policy):
    safety = fields(specification["Safety / Audit"], {"policy_id", "notes"}, where="Safety / Audit")
    text(safety["policy_id"], "State specification policy_id")
    strings(safety["notes"], "Safety notes")
    if policy is not None:
        if not isinstance(policy, dict):
            raise ValueError("State specification Policy: object required")
        if safety["policy_id"] != policy.get("policy_id"):
            raise ValueError("State specification Policy ID mismatch")


def _agent_view(workflow, alias, mode, *, usage, trigger, scope=None):
    setting = workflow["agents"].get(alias)
    if not isinstance(setting, dict) or not isinstance(setting.get("scopes"), dict) or not isinstance(setting.get("outputs"), dict):
        raise ValueError("State specification: Unknown Agent/mode")
    if mode not in setting["scopes"] or mode not in setting["outputs"]:
        raise ValueError("State specification: Unknown Agent/mode")
    if "needs_context" not in setting or "timeout_s" not in setting:
        raise ValueError("State specification: Agent needs_context/timeout_s required")
    selected = setting["scopes"][mode] if scope is None else scope
    strings(selected, "Agent input scope", nonempty=True)
    strings(setting["outputs"][mode], "Agent outputs", nonempty=True)
    boolean(setting["needs_context"], "Agent needs_context")
    positive(setting["timeout_s"], "Agent timeout_s")
    return {"agent": alias, "mode": mode, "usage": usage, "trigger": trigger,
            "scope": copy.deepcopy(selected), "needs_context": setting["needs_context"],
            "timeout_s": setting["timeout_s"], "outputs": copy.deepcopy(setting["outputs"][mode])}


def _derived_views(state, workflow):
    agents, checkpoints = [], []
    service = workflow["context_service"]

    def add_agent(alias, mode, usage, trigger, *, scope=None, prepare=True):
        agent = _agent_view(workflow, alias, mode, usage=usage, trigger=trigger, scope=scope)
        if agent not in agents:
            agents.append(agent)
        if prepare and (agent["needs_context"] or usage == "hitl_evidence"):
            add_agent(service["agent"], service["mode"], "context_preparation", trigger,
                      scope=agent["scope"], prepare=False)

    def inspect(actions, trigger):
        for raw in actions:
            action = {raw: None} if isinstance(raw, str) else raw
            if "authorize_and_run" in action:
                add_agent(action["authorize_and_run"], action["mode"], "execution", trigger)
            elif "ensure_context" in action:
                add_agent(service["agent"], service["mode"], "context_request", trigger,
                          scope=action["ensure_context"]["scope"], prepare=False)
            elif "request_hitl" in action:
                name = action["request_hitl"]
                checkpoint = workflow["hitl_checkpoints"].get(name)
                if not isinstance(checkpoint, dict):
                    raise ValueError("State specification: Unknown checkpoint")
                fields(checkpoint, {"roles", "options", "required_confirmations", "confirmations_on_decisions", "after", "question"}, where="Checkpoint")
                if "after" in action and action["after"] != checkpoint["after"]:
                    raise ValueError("HITL after mismatch")
                resolved = {"checkpoint": name, **copy.deepcopy(checkpoint)}
                if resolved not in checkpoints:
                    checkpoints.append(resolved)
                after = checkpoint["after"]
                if not isinstance(after, str) or "." not in after:
                    raise ValueError("State specification: Agent.mode result reference required")
                alias, _, mode = after.rpartition(".")
                add_agent(alias, mode, "hitl_evidence", trigger)

    inspect(state.get("on_enter", []), "on_enter")
    for index, route in enumerate(state.get("on_event", [])):
        inspect(route.get("do", []), f"on_event[{index}]")
    inspect(state.get("timeout", {}).get("do", []), "timeout")
    inputs = [{key: copy.deepcopy(value) for key, value in agent.items() if key != "outputs"} for agent in agents]
    outputs = [{key: copy.deepcopy(value) for key, value in agent.items() if key not in {"scope", "needs_context"}} for agent in agents]
    branches = []
    for route in state.get("on_event", []):
        branch = copy.deepcopy(route)
        if "transition" in branch:
            branch["next_state"] = branch.pop("transition")
        branches.append(branch)
    return {"Agent Input": {"derive": True, "agents": inputs},
            "Expected Agent Output": {"derive": True, "agents": outputs},
            "HITL": {"derive": True, "checkpoints": checkpoints},
            "Next State / Branch": {"derive": True, "branches": branches}}


def _compile_state(specification, workflow, policy):
    fields(specification, set(STATE_SPEC_ITEMS), where="State specification")
    json_value(specification, "State specification")
    identity = fields(specification["State"], {"name", "terminal"}, where="State")
    text(identity["name"], "State name")
    boolean(identity["terminal"], "State terminal")
    goal = fields(specification["Clinical Goal and Scope"], {"goal", "start", "end"}, where="Clinical Goal and Scope")
    for value in goal.values():
        text(value, "Clinical Goal and Scope")
    for key in ("Entry Trigger", "Incoming Data / Evidence"):
        description = fields(specification[key], {"description"}, where=key)
        text(description["description"], key)
    _requirements(specification["Data requirement"])
    _policy(specification, policy)
    capabilities = fields(specification["Required Capability"], {"on_enter"}, where="Required Capability")
    _actions(capabilities["on_enter"])
    for key in _DERIVED_KEYS:
        derived = fields(specification[key], {"derive"}, where=key)
        if derived["derive"] is not True:
            raise ValueError(f"{key}: derive must be true")
    transitions = fields(specification["Guard / Transition condition"], {"transitions"}, where="Guard / Transition condition")["transitions"]
    if not isinstance(transitions, list):
        raise ValueError("State specification: transitions list required")
    routes = []
    for transition in transitions:
        fields(transition, {"when"}, {"guard", "do", "next_state"}, "State specification transition")
        _pattern(transition["when"])
        event_type = transition["when"].get("event_type")
        _actions(transition.get("do", []), decision_route=event_type == "HITL_DECISION" or event_type == ["HITL_DECISION"])
        if "guard" in transition:
            validate_expression(transition["guard"])
        route = copy.deepcopy(transition)
        if "next_state" in route:
            target = text(route.pop("next_state"), "next_state")
            if target not in workflow["states"]:
                raise ValueError("Undefined State")
            route["transition"] = target
        routes.append(route)
    timer = fields(specification["Timer / Exception / Fail-safe"], {"exceptions"}, {"timeout"}, "Timer / Exception / Fail-safe")
    strings(timer["exceptions"], "State exceptions")
    if "timeout" in timer:
        timeout = fields(timer["timeout"], {"after_min", "do"}, {"repeat"}, "Timeout")
        positive(timeout["after_min"], "Timeout after_min")
        _actions(timeout["do"])
        if "repeat" in timeout:
            boolean(timeout["repeat"], "Timeout repeat")
    if identity["terminal"] and (routes or "timeout" in timer):
        raise ValueError("Terminal State cannot declare unused routes/timeouts")
    state = {"name": identity["name"], "on_enter": copy.deepcopy(capabilities["on_enter"])}
    if identity["terminal"]:
        state["terminal"] = True
    if routes:
        state["on_event"] = routes
    if "timeout" in timer:
        state["timeout"] = copy.deepcopy(timer["timeout"])
    metadata = copy.deepcopy(specification)
    metadata.update(_derived_views(state, workflow))
    state["specification"] = metadata
    return state


def compile_state_specifications(workflow, policy=None):
    """Compile thirteen-heading source States; return legacy DSL unchanged.

    All results are copies. Policy, when supplied, must carry the pinned
    policy_id. The runtime bundle validator still validates the whole compiled
    Workflow/Policy/Registry combination; this compiler validates authoring.
    """
    if not isinstance(workflow, dict):
        raise ValueError("Workflow: object required")
    if "state_spec_schema" not in workflow:
        return copy.deepcopy(workflow)
    fields(workflow, _WORKFLOW_KEYS, where="State specification Workflow")
    if workflow["state_spec_schema"] != STATE_SPEC_SCHEMA:
        raise ValueError("Unsupported state_spec_schema")
    if not isinstance(workflow["states"], dict) or not workflow["states"]:
        raise ValueError("State specification: States required")
    if not isinstance(workflow["agents"], dict) or not isinstance(workflow["hitl_checkpoints"], dict):
        raise ValueError("State specification: Agent/checkpoint mappings required")
    service = fields(workflow["context_service"], {"agent", "mode", "available_states"}, where="Context service")
    text(service["agent"], "Context service Agent")
    text(service["mode"], "Context service mode")
    for state_id in workflow["states"]:
        text(state_id, "State ID")
    compiled = copy.deepcopy(workflow)
    compiled.pop("state_spec_schema")
    compiled["state_specification_version"] = STATE_SPEC_VERSION
    compiled["states"] = {name: _compile_state(specification, workflow, policy)
                          for name, specification in workflow["states"].items()}
    return compiled


def validate_state_specification_metadata(state_definition, workflow, policy=None):
    """Verify compiled annotations and derived contracts against runtime fields."""
    metadata = state_definition.get("specification")
    if metadata is None:
        raise ValueError("State has no compiled specification")
    fields(metadata, set(STATE_SPEC_ITEMS), where="Compiled State specification")
    source = copy.deepcopy(metadata)
    for key, derived_key in _DERIVED_KEYS.items():
        fields(source[key], {"derive", derived_key}, where=key)
        source[key] = {"derive": source[key]["derive"]}
    expected = _compile_state(source, workflow, policy)
    if expected != state_definition:
        raise ValueError("State specification does not match compiled execution/derived contracts")


def state_specification_view(state_definition, workflow, facts, now, policy=None, *, state_id=None):
    """Copy the pinned specification and attach current evidence display data.

    CONDITIONAL expressions use the existing typed DSL and the current Fact
    value projection at $episode.context; full Facts are available at $facts.
    Freshness compares now with source_time (default) or known_at. Its display
    status never replaces a Fact status, changes an Action, or blocks a route.
    An explicit State ID binds copied definitions to the caller's current State.
    """
    validate_state_specification_metadata(state_definition, workflow, policy)
    at = timestamp(now, "State specification view now")
    if not isinstance(facts, dict) or not all(isinstance(fact, dict) for fact in facts.values()):
        raise ValueError("State specification view: Fact mapping required")
    view = copy.deepcopy(state_definition["specification"])
    if state_id is not None:
        text(state_id, "State binding ID")
        if state_id not in workflow["states"] or workflow["states"][state_id] != state_definition:
            raise ValueError("State binding does not match its Workflow definition")
    else:
        state_id = next((key for key, value in workflow["states"].items() if value is state_definition), None)
        if state_id is None:
            matches = [key for key, value in workflow["states"].items() if value == state_definition]
            if len(matches) != 1:
                raise ValueError("State specification view needs an unambiguous State in its Workflow")
            state_id = matches[0]
    env = {"episode": {"state": state_id, "context": {key: fact.get("value") for key, fact in facts.items()}},
           "facts": facts}
    for requirement in view["Data requirement"]:
        fact = facts.get(requirement["field"])
        requirement["fact"] = copy.deepcopy(fact)
        requirement["applicable"] = evaluate(requirement["condition"], env) if "condition" in requirement else True
        freshness = requirement.get("freshness")
        if fact is None:
            requirement["freshness_status"] = "MISSING"
        elif freshness is None:
            requirement["freshness_status"] = "NOT_SPECIFIED"
        else:
            recorded = fact.get(freshness.get("time_field", "source_time"))
            if recorded is None:
                requirement["freshness_status"] = "UNKNOWN"
                continue
            try:
                age = (at - timestamp(recorded, "Fact freshness time")).total_seconds()
            except ValueError:
                requirement["freshness_status"] = "INVALID_TIME"
                continue
            requirement["age_s"] = age
            requirement["freshness_status"] = "FUTURE" if age < 0 else "STALE" if age > freshness["max_age_s"] else "FRESH"
    return view
