"""v0.3 fixed-evidence console component, only at Client boundaries.

No source/fixture files are read. Blocking input runs in a separate thread;
Workflow/Activity execution and independent ingress remain free to continue.
"""
import asyncio
import copy
from datetime import datetime,timezone
import json

from chain_demo.contracts import validate_hitl_request,validate_hitl_decision
from .presentation import (CONFIRMATION_LABELS, OPTION_LABELS, ROLE_LABELS, label,
                           show_documents, show_evidence)


def _clock():
    return datetime.now(timezone.utc).isoformat()


def _bundle(record):
    record=copy.deepcopy(record)
    if record.get("status")!="OPEN":raise ValueError("Console requires an open HITL request")
    validate_hitl_request(record["request"],record["snapshot"],record["results"])
    return record


def show_request(record,write=print):
    record=_bundle(record)
    lines=[];show_evidence(record,lines.append)
    write("\n".join(lines))
    return record


def collect_decision(record,*,input_fn=input,write=print,clock=_clock):
    record=show_request(record,write);request=record["request"]
    try:
        while True:
            for i,option in enumerate(request["options"],1):write(f"{i}. {label(option,OPTION_LABELS)}")
            choice=input_fn("선택 번호/코드 (json=고정 근거 전체, docs=고정 문서, q=중단): ").strip()
            if choice.lower()=="q":raise KeyboardInterrupt()
            if choice.lower()=="json":
                write(json.dumps(record,ensure_ascii=False,indent=2,sort_keys=True));continue
            if choice.lower()=="docs":
                lines=[];show_documents(record,lines.append)
                write("\n".join(lines));continue
            if choice.isdigit() and 1<=int(choice)<=len(request["options"]):choice=request["options"][int(choice)-1]
            else:choice=choice.upper()
            if choice not in request["options"]:write("허용된 선택지를 입력해 주세요.");continue
            confirmed=[]
            if choice in request["confirmations_on_decisions"]:
                for item in request["required_confirmations"]:
                    if input_fn(label(item,CONFIRMATION_LABELS)+"? [y/N]: ").strip().lower() not in {"y","yes"}:break
                    confirmed.append(item)
                if len(confirmed)!=len(request["required_confirmations"]):
                    write("확인되지 않은 항목이 있어 해당 결정을 전송하지 않았습니다.");continue
            break
        while True:
            write("역할: "+", ".join(f"{i+1}={label(role,ROLE_LABELS)}" for i,role in enumerate(request["roles"])))
            role=input_fn("모의 의료진 역할 번호/코드: ").strip().upper()
            if role.isdigit() and 1<=int(role)<=len(request["roles"]):role=request["roles"][int(role)-1]
            if role in request["roles"]:break
            write("허용된 역할을 입력해 주세요.")
        staff=input_fn("모의 의료진 ID [DEMO-CLINICIAN]: ").strip() or "DEMO-CLINICIAN"
        while True:
            reason=input_fn("결정/보류 사유 (필수): ").strip()
            if reason:break
            write("사유를 입력해 주세요.")
        decision={"contract_schema":"chain-hitl-decision/v0.3","request_id":request["request_id"],
                  "evidence_snapshot_id":request["evidence_snapshot_id"],"decision":choice,
                  "actor":{"role":role,"staff_id":staff},"confirmed_items":confirmed,
                  "evidence_viewed":[request["evidence_snapshot_id"],*request["result_ids"]],
                  "comment":reason,"recorded_at":clock()}
        validate_hitl_decision(decision,request)
        return decision
    except (EOFError,KeyboardInterrupt):
        write("INPUT_CLOSED_NO_DECISION — 응답을 만들거나 전송하지 않았습니다.")
        return None


async def serve_console(query,submit,*,input_fn=input,write=print,clock=_clock,poll_interval=.1,on_request=None):
    """Generic async Client component; query/submit are supplied by its runtime.

    Transport ACK is not decision acceptance. Stale typed decisions may still
    be submitted, and the engine is the final authority for their rejection.
    EOF stops this component only; it never cancels or approves the episode.
    """
    if poll_interval<=0:raise ValueError("Positive console poll interval required")
    awaiting={}
    while True:
        snapshot=await query()
        for request_id,offset in list(awaiting.items()):
            outcome=next((e for e in snapshot.get("audit",[])[offset:] if e.get("request_id")==request_id
                          and e["type"] in {"HITL_REJECTED","HITL_DECISION_RECORDED"}),None)
            if outcome:
                write(("DECISION_REJECTED" if outcome["type"]=="HITL_REJECTED" else "DECISION_ACCEPTED")+
                      " | "+request_id+" | "+(outcome.get("reason","") if outcome["type"]=="HITL_REJECTED"
                                               else "엔진이 응답을 수락했습니다 · "+label(outcome["decision"],OPTION_LABELS)))
                if outcome["type"]=="HITL_DECISION_RECORDED" and outcome.get("decision") in {"DEFER","HOLD"}:
                    write("현재 상태를 유지하며 재요청 타이머를 기다립니다. 독립적인 병원 Event와 Agent 응답은 계속 처리됩니다.")
                del awaiting[request_id]
        if snapshot.get("done"):return
        record=next((copy.deepcopy(r) for r in snapshot.get("open_hitl",{}).values()
                     if r["status"]=="OPEN" and r["generation"]==snapshot.get("generation")
                     and r["request"]["request_id"] not in awaiting),None)
        if record is None:await asyncio.sleep(poll_interval);continue
        request_id=record["request"]["request_id"]
        if on_request:on_request(copy.deepcopy(record["request"]))
        task=asyncio.create_task(asyncio.to_thread(collect_decision,record,input_fn=input_fn,write=write,clock=clock))
        warned=False
        try:
            while not task.done():
                await asyncio.wait({task},timeout=poll_interval)
                snapshot=await query()
                historical=snapshot.get("hitl_history",{}).get(request_id,{})
                if historical.get("status") in {"INVALIDATED","CLOSED"} and not warned:
                    write(("REQUEST_INVALIDATED" if historical["status"]=="INVALIDATED" else "REQUEST_CLOSED")+
                          " | "+request_id+" | 입력 중인 답은 원래 요청에 결속됩니다.")
                    warned=True
            decision=await task
        finally:
            if not task.done():task.cancel()
        if decision is None:return
        offset=len(snapshot.get("audit",[]))
        await submit(decision)
        awaiting[request_id]=offset
        write("DECISION_SUBMITTED_AWAITING_ENGINE | "+request_id+" | 응답 전송됨 · 엔진 수락 확인 대기")


async def serve_temporal_console(handle,**kwargs):
    """Use an existing v0.3 Workflow handle; does not start a new CLI/Simulator."""
    async def query():return await handle.query("snapshot")
    async def submit(decision):await handle.signal("submit_decision",decision)
    return await serve_console(query,submit,**kwargs)
