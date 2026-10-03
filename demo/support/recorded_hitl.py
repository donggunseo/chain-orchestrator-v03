"""Explicit test-only saved human responses, rebound to the actual open request."""
import asyncio
import copy
from pathlib import Path

from chain_demo.contracts import validate_hitl_decision, validate_hitl_request
from chain_demo.data_io import read_json
from chain_demo.validation import canonical_hash, fields, strings, text, timestamp


def load_recorded(path,*,test_mode=False):
    if test_mode is not True:raise ValueError("Recorded responses require explicit test mode")
    source=read_json(path);responses=[]
    if not isinstance(source,dict):raise ValueError("Saved responses require a JSON object")
    if source.get("test_mode_only") is True and set(source)=={"test_mode_only","events"}:
        if not isinstance(source["events"],list):raise ValueError("Saved events must be a list")
        for event in source["events"]:
            if not isinstance(event,dict) or event.get("event_type")!="HITL_DECISION":raise ValueError("Saved event is not a human decision")
            try:
                payload=event["payload"]
                responses.append({"checkpoint":payload["checkpoint"],"decision":payload["decision"],
                    "actor":{k:payload["actor"][k] for k in ("role","staff_id")},
                    "confirmed_items":copy.deepcopy(payload.get("confirmed_items",[])),"comment":payload["comment"]})
            except (KeyError,TypeError) as exc:raise ValueError("Incomplete saved human response") from exc
    else:
        fields(source,{"recorded_schema","test_mode_only","responses"},where="Recorded test responses")
        if source["recorded_schema"]!="chain-recorded/v0.3" or source["test_mode_only"] is not True:
            raise ValueError("Only explicit synthetic recorded responses supported")
        responses=copy.deepcopy(source["responses"])
    if not isinstance(responses,list) or not responses:raise ValueError("Recorded responses must be a nonempty list")
    for response in responses:
        fields(response,{"checkpoint","decision","actor","confirmed_items","comment"},where="Saved response")
        for key in ("checkpoint","decision","comment"):text(response[key])
        fields(response["actor"],{"role","staff_id"},where="Saved actor")
        for value in response["actor"].values():text(value)
        strings(response["confirmed_items"])
    return {"responses":responses,"source_path":str(Path(path).resolve()),"source_hash":canonical_hash(source),
            "binding":"EXPLICIT_RECORDED_TEST; current request/snapshot/time; no regimen/order/old IDs copied"}


def bind_response(response,record,*,now):
    if record.get("status")!="OPEN":raise ValueError("Saved response requires an actually open request")
    request=record["request"];validate_hitl_request(request,record["snapshot"],record["results"])
    if response["checkpoint"]!=request["checkpoint"]:raise ValueError("Saved checkpoint differs from open request")
    decision={"contract_schema":"chain-hitl-decision/v0.3","request_id":request["request_id"],
        "evidence_snapshot_id":request["evidence_snapshot_id"],"decision":response["decision"],
        "actor":copy.deepcopy(response["actor"]),"confirmed_items":copy.deepcopy(response["confirmed_items"]),
        "evidence_viewed":[request["evidence_snapshot_id"],*request["result_ids"]],
        "comment":response["comment"],"recorded_at":now}
    validate_hitl_decision(decision,request)
    return decision


async def consume(query,submit,source,clock,*,timings=(),anchors=lambda:{},write=print,poll_interval=.01):
    """Wait for each actual request; no source timestamp or expected path drives it."""
    for index,response in enumerate(source["responses"]):
        while True:
            snapshot=await query()
            if snapshot.get("done"):raise ValueError("Episode ended before all saved responses were consumed")
            record=snapshot.get("open_hitl",{}).get(response["checkpoint"])
            if record and record["status"]=="OPEN":break
            await asyncio.sleep(poll_interval)
        if index<len(timings):
            timing=timings[index]
            if timing["checkpoint"]!=response["checkpoint"]:raise ValueError("Timing/saved checkpoint mismatch")
            while timing["anchor"] not in anchors():await asyncio.sleep(poll_interval)
            from datetime import timedelta
            due=(timestamp(anchors()[timing["anchor"]]["at"])+timedelta(seconds=timing["offset_s"])).isoformat()
            await clock.sleep_until(due)
            # A related revision may have invalidated the old request during delay.
            while True:
                snapshot=await query();record=snapshot.get("open_hitl",{}).get(response["checkpoint"])
                if record and record["status"]=="OPEN":break
                await asyncio.sleep(poll_interval)
        decision=bind_response(response,record,now=clock.now())
        offset=len(snapshot["audit"]);await submit(decision)
        write("EXPLICIT_RECORDED_TEST_SUBMITTED | "+decision["request_id"])
        while True:
            snapshot=await query()
            outcome=next((e for e in snapshot["audit"][offset:] if e.get("request_id")==decision["request_id"]
                          and e["type"] in {"HITL_DECISION_RECORDED","HITL_REJECTED"}),None)
            if outcome:
                if outcome["type"]=="HITL_REJECTED":raise ValueError("Saved response rejected: "+outcome["reason"])
                break
            await asyncio.sleep(poll_interval)
