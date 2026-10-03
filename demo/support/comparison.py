"""Post-run semantic comparison only. Source clinical answers are not imported."""
import copy

from chain_demo.contracts import validate_hitl_request
from chain_demo.data_io import read_json
from chain_demo.validation import canonical_hash, fields, strings, text, timestamp


def load_expected(path,*,test_mode=False):
    if test_mode is not True:raise ValueError("Expected data requires explicit test mode")
    value=read_json(path)
    fields(value,{"test_only","state_history"},{"source_executions","decisions","semantic_profile"},"Expected semantic result")
    if value["test_only"] is not True:raise ValueError("Expected results are test-only")
    strings(value["state_history"],nonempty=True)
    decisions=value.get("decisions",{})
    if not isinstance(decisions,dict):raise ValueError("Expected decisions must be a mapping")
    for k,v in decisions.items():text(k);text(v)
    profile=value.get("semantic_profile","stroke-minimal-v0.3" if "source_executions" in value else "state-path")
    if profile not in {"stroke-minimal-v0.3","state-path"}:raise ValueError("Unsupported comparison profile")
    return {"state_history":copy.deepcopy(value["state_history"]),"decisions":copy.deepcopy(decisions),
            "semantic_profile":profile,"source_hash":canonical_hash(value)}


def compare(snapshot,expected,publications,*,reference_responses=()):
    reference=expected["decisions"] or {r["checkpoint"]:r["decision"] for r in reference_responses}
    history=snapshot.get("hitl_history",{})
    actual={history[e["request_id"]]["request"]["checkpoint"]:e["decision"] for e in snapshot.get("audit",[])
            if e["type"]=="HITL_DECISION_RECORDED"}
    applicable=not any(cp in reference and value!=reference[cp] for cp,value in actual.items())
    match=snapshot["state_history"]==expected["state_history"]
    checks={"terminal_reached":snapshot.get("done") is True,
            "reference_path_when_comparable":match if applicable else True,
            "agent_and_source_succeeded":not any(e["type"] in {"AGENT_FAILED_OR_REJECTED","CONTEXT_RESOLUTION_FAILED","EVENT_REJECTED","TRANSITION_DENIED"} for e in snapshot["audit"]),
            "mock_notification_status":all(n["status"]=="MOCK_RECORDED" for n in snapshot["notifications"])}
    prior="sha256:"+"0"*64
    intact=True
    for entry in snapshot["audit"]:
        intact=intact and entry.get("prev_hash")==prior and entry.get("entry_hash")==canonical_hash(
            {k:v for k,v in entry.items() if k!="entry_hash"})
        prior=entry.get("entry_hash")
    checks["audit_hash_chain"]=intact and bool(snapshot["audit"])
    try:
        for record in history.values():validate_hitl_request(record["request"],record["snapshot"],record["results"])
        checks["fixed_evidence_contracts"]=True
    except ValueError:checks["fixed_evidence_contracts"]=False
    if expected["semantic_profile"]=="stroke-minimal-v0.3":
        audit=snapshot["audit"]
        applied=next((i for i,e in enumerate(audit) if e["type"]=="CONTEXT_APPLIED" and any(
            p["event"]["event_id"]==e.get("event_id") and p["record"]["payload"].get("document_type")=="ED_INITIAL_NOTE" for p in publications)),None)
        calls=[i for i,e in enumerate(audit) if e["type"]=="AGENT_EXECUTION_REQUESTED" and e.get("alias")=="stroke_screening"]
        checks["screening_after_initial_note"]=applied is not None and bool(calls) and min(calls)>applied
        transitions=[e for e in audit if e["type"]=="STATE_TRANSITION" and e["from_state"]=="S2" and e["to"]=="S2_1"]
        valid=True
        for transition in transitions:
            p=next((p for p in publications if p["event"]["event_id"]==transition["cause_event_id"]),None)
            orders=[o for o in publications if o["record"]["event_type"]=="ORDER_PLACED"
                    and timestamp(o["record"]["source_time"])<=timestamp(p["record"]["source_time"])] if p else []
            valid=valid and bool(p and p["record"]["event_type"]=="IMAGING_STUDY_COMPLETED" and any(
                p["record"]["payload"].get("order_id")==o["record"]["payload"].get("order_id") for o in orders))
        checks["matching_ncct_transition"]=valid and (len(transitions)==1 if "S2_1" in snapshot["state_history"] else not transitions)
        finals={e["request_id"]:i for i,e in enumerate(audit) if e["type"]=="AGENT_RESULT_ACCEPTED"
                and e.get("alias")=="tpa_decision_support" and e.get("mode")=="final"}
        requested={e["request_id"]:i for i,e in enumerate(audit) if e["type"]=="HITL_REQUESTED"}
        checks["final_before_hitl2"]=all(r["request"]["request_id"] in requested and any(
            result["request_id"] in finals and finals[result["request_id"]]<requested[r["request"]["request_id"]]
            for result in r["results"]) for r in history.values() if r["request"]["checkpoint"]=="HITL_2_THROMBOLYSIS")
        checks["external_ct_order_actor"]=all(p["actor"]=="SIMULATED_CLINICIAN" for p in publications if p["record"]["event_type"]=="ORDER_PLACED")
    return {"comparison_schema":"chain-semantic-comparison/v0.3","checks":checks,"checks_passed":all(checks.values()),
            "reference_comparison_applicable":applicable,"reference_path_match":match,
            "expected_path":expected["state_history"],"actual_path":snapshot["state_history"],
            "expected_source_hash":expected.get("source_hash"),
            "actual_decisions":actual,"reference_decisions":reference,"clinical_outputs_compared":False,
            "note":"State/trigger/evidence contracts only. Source executions, dose, thresholds, elapsed/window and Agent call counts are not expected clinical answers."}
