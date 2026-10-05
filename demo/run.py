"""v0.3 causal hospital + Local or real Temporal transport + owned run artifacts."""
import argparse
import asyncio
import copy
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import select
import sys
from threading import Event, RLock
from uuid import uuid4

from chain_demo.adapters.store import PublishedStore
from chain_demo.agents.runtime import AgentRuntime
from chain_demo.config import ROOT
from chain_demo.data_io import load_initial
from .console import serve_console
from .local_runtime import AsyncLocalRuntime
from .presentation import DemoPresenter, safe_text
from chain_demo.notifier import MockNotifier
from chain_demo.orchestration_activities import OrchestrationActivities
from chain_demo.orchestration_config import load_orchestration_bundle
from chain_demo.validation import canonical_hash, positive, timestamp


def save_json(path,value):
    with Path(path).open("x",encoding="utf8") as stream:
        json.dump(value,stream,ensure_ascii=False,indent=2);stream.write("\n")


class DemoOutput:
    """Format display messages outside the engine; keep full data in artifacts."""
    def __init__(self,write):self.write=write;self.lock=RLock()

    def __call__(self,value):
        value=str(value)
        if value.startswith("MOCK_RECORDED "):
            try:
                receipt=json.loads(value[len("MOCK_RECORDED "):])
                if receipt.get("receipt_schema")=="chain-effect-receipt/v0.3":
                    labels={"SET_FLAG":"EHR 표시 활성화", "CLEAR_FLAG":"EHR 표시 해제",
                            "SET_DASHBOARD":"대시보드 활성화", "CLEAR_DASHBOARD":"대시보드 해제"}
                    operation=labels.get(receipt["operation"],receipt["operation"])
                    result="Mock 상태 반영" if receipt["projection_updated"] else "Mock 상태 유지 · 중복/늦은 요청"
                    value=("MOCK_RECORDED | 모의 표시 기록 · 실제 EHR 변경 없음 | "+safe_text(operation)+
                           " · "+safe_text(receipt["target"])+" / "+safe_text(receipt["resource"])+
                           " | "+result+" | "+safe_text(receipt["request_id"]))
                else:
                    value=("MOCK_RECORDED | 모의 알림 기록 · 실제 발송 없음 | "+
                           safe_text(receipt["recipient"])+" · "+safe_text(receipt["template"])+
                           " | "+safe_text(receipt["request_id"]))
            except (ValueError,KeyError,TypeError):pass
        elif value.startswith("TEST_CLOCK_AT | "):
            value+=" | 가상 에피소드 시계 · Temporal 타이머는 별도 실제 시간"
        with self.lock:self.write(value)


async def watch_timers(runtime,view,*,write,poll_interval=.1):
    """Display I/O is independent of ingress, console input and run completion."""
    last_error=None
    while True:
        view[:]=await runtime.timer_status()
        error=getattr(runtime,"timer_error",None)
        if error!=last_error:
            if error:write("TIMER_OBSERVATION_DELAYED | 타이머 상태 조회 지연 | "+safe_text(error))
            elif last_error:write("TIMER_OBSERVATION_RESUMED | 실제 타이머 상태 조회 재개")
            last_error=error
        await asyncio.sleep(poll_interval)


class StdinReader:
    """Selectable pipe/TTY input with cancellation; never blocks executor shutdown."""
    def __init__(self,stop,write):self.stop=stop;self.write=write;self.buffer=b"";self.eof=False
    def __call__(self,prompt):
        self.write(prompt)
        while not self.stop.is_set():
            if b"\n" in self.buffer:
                line,self.buffer=self.buffer.split(b"\n",1);return line.decode(sys.stdin.encoding or "utf8").rstrip("\r")
            if self.eof:
                if self.buffer:
                    line,self.buffer=self.buffer,b"";return line.decode(sys.stdin.encoding or "utf8")
                raise EOFError
            if select.select([sys.stdin.fileno()],[],[],.1)[0]:
                data=os.read(sys.stdin.fileno(),4096)
                if data:self.buffer+=data
                else:self.eof=True
        raise EOFError


class JournalActivities:
    def __init__(self,native,path):self.native=native;self.path=Path(path)
    def _append(self,value):
        with self.path.open("a",encoding="utf8") as stream:stream.write(json.dumps(value,ensure_ascii=False)+"\n")
    async def execute(self,method,command,*,metadata=None):
        self._append({"phase":"REQUEST","method":method,"command":copy.deepcopy(command),
                      **({"temporal_activity":metadata} if metadata else {})})
        try:reply=await getattr(self.native,method)(command)
        except Exception as exc:
            self._append({"phase":"ERROR","request_id":command["id"],"error":f"{type(exc).__name__}: {exc}"});raise
        self._append({"phase":"RESULT","request_id":command["id"],"reply":reply})
        return reply
    async def resolve(self,command):return await self.execute("resolve",command)
    async def invoke(self,command):return await self.execute("invoke",command)
    async def notify(self,command):return await self.execute("notify",command)
    async def effect(self,command):return await self.execute("effect",command)


async def run(args,*,write=print,input_fn=None):
    from demo.simulator.clock import ManualClock,RealClock
    from demo.simulator.hospital import HospitalSimulator
    write=DemoOutput(write)
    if args.backend!="temporal" and (args.start_local or args.address or args.test_timer_scale is not None):
        raise ValueError("Service/clock options require Temporal backend")
    if args.start_local and args.address:raise ValueError("--address and --start-local cannot be combined")
    if args.test_timer_scale is not None:
        if not args.test_mode:raise ValueError("--test-timer-scale requires --test-mode")
        positive(args.test_timer_scale)
    # Explicit mode checks precede every optional test file read.
    if args.hitl=="recorded" and (not args.test_mode or not args.recorded):raise ValueError("Recorded HITL requires --test-mode and --recorded")
    if (args.test_driver or args.expected or args.recorded) and not args.test_mode:raise ValueError("Test sidecars require --test-mode")
    if args.recorded and args.hitl!="recorded":raise ValueError("--recorded is used only by --hitl recorded")
    if args.test_mode and not args.test_driver:
        args.test_driver=str(ROOT/"demo/scenarios/reference.yaml")
    if args.test_driver:
        from demo.support.test_driver import load_driver,drive
    if args.hitl=="recorded":
        from demo.support.recorded_hitl import load_recorded,consume
    if args.expected:
        from demo.support.comparison import load_expected,compare
    positive(args.run_timeout)
    out=Path(args.output_dir)
    if out.exists():raise FileExistsError("Output directory already exists: "+str(out))
    initial=load_initial(Path(args.episode)/"initial.json")
    if args.arrival:initial["arrival_time"]=timestamp(args.arrival).isoformat()
    elif not args.test_mode:initial["arrival_time"]=datetime.now(timezone.utc).isoformat()
    clock=ManualClock(initial["arrival_time"],test_mode=True) if args.test_mode else RealClock()
    configuration=load_orchestration_bundle(workflow_path=args.workflow,catalog_path=args.agents,
                                           policy_path=args.policy,registry_path=args.registry)
    driver=load_driver(args.test_driver,test_mode=args.test_mode) if args.test_driver else None
    recorded=load_recorded(args.recorded,test_mode=args.test_mode) if args.hitl=="recorded" else None
    if recorded and (len(driver["responses"])!=len(recorded["responses"]) or any(
            t["checkpoint"]!=r["checkpoint"] for t,r in zip(driver["responses"],recorded["responses"]))):
        raise ValueError("Every saved response requires one matching explicit test timing")
    out.mkdir(parents=True,exist_ok=False)
    save_json(out/"manifest.json",{"run_id":uuid4().hex,"backend":args.backend,"test_mode":args.test_mode,
        "initial":initial,"engine_bundle":configuration["engine"],"worker_bundle":configuration["agents"],
        "driver_hash":canonical_hash(driver) if driver else None,"recorded_provenance":recorded,
        "mock_only":True,"clinical_delivery_or_treatment":False,
        "temporal_options":{"address":args.address,"start_local":args.start_local,"test_timer_scale":args.test_timer_scale}
                           if args.backend=="temporal" else None})
    store=PublishedStore(initial);stop=Event();tasks=[];snapshot=None;simulation=None;publications=[];client_requests=[]
    native=OrchestrationActivities(configuration["engine"],AgentRuntime(configuration["agents"]),store,
        MockNotifier(out/"notices.sqlite",configuration["engine"]["policy"]["notification_templates"],write=write))
    activities=JournalActivities(native,out/"commands.jsonl")
    # Own even an empty journal; artifacts are explicit on an early/partial exit.
    (out/"commands.jsonl").touch(exist_ok=False)
    outcome={"outcome_schema":"chain-run-outcome/v0.3","backend":args.backend,"status":"FAILED","reason":None}
    if args.backend=="temporal":
        from .temporal_runtime import TemporalRuntime
        runtime=TemporalRuntime(configuration["engine"],initial,activities,address=args.address or "127.0.0.1:7233",
            start_local=args.start_local,test_mode=args.test_mode,test_now=clock.now() if args.test_mode else None,
            timer_scale=args.test_timer_scale if args.test_timer_scale is not None else 1.0,output=out,save=save_json,write=write)
    else:runtime=AsyncLocalRuntime(configuration["engine"],initial,activities,clock=clock,test_mode=args.test_mode)
    try:
        async with runtime:
            async with HospitalSimulator(args.episode,out/"sources",store,clock,runtime.submit_event) as hospital:
                await hospital.start(initial)
                tasks.append(asyncio.create_task(hospital.watch(runtime.snapshot,poll_interval=.005)))
                if driver:tasks.append(asyncio.create_task(drive(driver,hospital,runtime,clock,write=write,
                    advance=runtime.advance_test_time if args.backend=="temporal" else None,client_requests=client_requests)))
                if recorded:
                    human=asyncio.create_task(consume(runtime.snapshot,runtime.submit_decision,recorded,clock,
                        timings=driver["responses"] if driver else (),anchors=lambda:hospital.scheduler.anchors,write=write))
                else:
                    human=asyncio.create_task(serve_console(runtime.snapshot,runtime.submit_decision,
                        input_fn=input_fn or StdinReader(stop,write),write=write,clock=clock.now,poll_interval=.005,
                        on_request=client_requests.append))
                tasks.append(human)
                timer_views=[]
                tasks.append(asyncio.create_task(watch_timers(runtime,timer_views,write=write)))
                presenter=DemoPresenter(write)
                try:
                    async with asyncio.timeout(args.run_timeout):
                        while True:
                            snapshot=await runtime.snapshot()
                            presenter.observe(snapshot,timers=timer_views)
                            failures=[e for e in snapshot["audit"] if e["type"] in {"AGENT_FAILED_OR_REJECTED","CONTEXT_RESOLUTION_FAILED","NOTIFICATION_FAILED_OR_REJECTED","EFFECT_FAILED_OR_REJECTED"}]
                            if failures or hospital.failures or hospital.scheduler.failures:
                                raise RuntimeError(json.dumps(failures+hospital.failures+hospital.scheduler.failures,ensure_ascii=False))
                            for task in tasks:
                                if task.done() and not task.cancelled() and task.exception():raise task.exception()
                            if snapshot["done"]:
                                snapshot=await runtime.result;outcome["status"]="COMPLETED";break
                            if human.done() and not recorded:
                                outcome.update(status="INPUT_CLOSED",reason="Console ended; no decision/State transition was invented")
                                break
                            if recorded and human.done() and any(r["status"]=="OPEN" for r in snapshot["open_hitl"].values()):
                                raise ValueError("Recorded responses exhausted with an open request; no response was invented")
                            if runtime.result.done():await runtime.result
                            await asyncio.sleep(.005)
                except TimeoutError:outcome.update(status="TIMED_OUT",reason="Run time limit; saved partial state")
                finally:
                    stop.set()
                    for task in tasks:task.cancel()
                    await asyncio.gather(*tasks,return_exceptions=True)
                    snapshot=await runtime.snapshot()
                    simulation={"manifest":hospital.manifest,"anchors":hospital.scheduler.anchors,
                        "audit":hospital.scheduler.audit,"failures":hospital.failures+hospital.scheduler.failures}
                    for publication in hospital.publications:
                        event=publication["event"]
                        record=store.resolve(event["source_ref"],at=clock.now(),content_hash=event["payload"]["content_hash"])["record"]
                        publications.append({**copy.deepcopy(publication),"record":record})
    except asyncio.CancelledError:
        outcome.update(status="INTERRUPTED",reason="Client interrupted; saved partial state")
        stop.set()
    except Exception as exc:
        outcome.update(status="FAILED",reason=f"{type(exc).__name__}: {exc}")
        stop.set()
    finally:
        if snapshot is None:
            try:snapshot=await runtime.snapshot()
            except Exception:snapshot=getattr(runtime,"cached",{"state":None,"done":False})
        save_json(out/"snapshot.json",snapshot);save_json(out/"simulation.json",simulation or {})
        save_json(out/"publications.json",publications)
        # Expected files open only after execution and source/task cleanup.
        if args.expected:
            try:
                comparison=compare(snapshot,load_expected(args.expected,test_mode=args.test_mode),publications,
                                   reference_responses=recorded["responses"] if recorded else ())
                save_json(out/"comparison.json",comparison)
                if outcome["status"]=="COMPLETED" and not comparison["checks_passed"]:
                    outcome.update(status="COMPARISON_FAILED",reason="Post-run semantic checks failed")
            except Exception as exc:
                reason=f"Comparison {type(exc).__name__}: {exc}"
                if outcome["status"]=="FAILED" and outcome["reason"]:outcome["comparison_error"]=reason
                else:outcome.update(status="FAILED",reason=reason)
        outcome["state"]=snapshot["state"];outcome["done"]=snapshot["done"]
        save_json(out/"outcome.json",outcome)
        if outcome["reason"]:write("RUN_REASON | "+outcome["reason"])
        write("RUN_OUTCOME | "+outcome["status"]+" | state="+str(snapshot["state"])+" | "+str(out))
    return 0 if outcome["status"]=="COMPLETED" else 2 if outcome["status"] in {"INPUT_CLOSED","TIMED_OUT","INTERRUPTED"} else 1


def parser():
    result=argparse.ArgumentParser(description=__doc__)
    result.add_argument("--episode",default=str(ROOT/"episodes/stroke_reference_001_v03"))
    result.add_argument("--output-dir",required=True)
    result.add_argument("--backend",choices=("local","temporal"),default="local")
    result.add_argument("--address",help="Existing Temporal Service address; embedded Worker shares Store")
    result.add_argument("--start-local",action="store_true",help="Own an ephemeral real Temporal dev Service")
    result.add_argument("--test-timer-scale",type=float,help="Explicit Temporal test mode only; scales real durable timers")
    result.add_argument("--hitl",choices=("console","recorded"),default="console")
    result.add_argument("--recorded");result.add_argument("--expected")
    result.add_argument("--test-mode",action="store_true");result.add_argument("--test-driver",help="Explicit synthetic clock driver; test mode defaults to demo/scenarios/reference.yaml")
    result.add_argument("--arrival",help="Explicit synthetic arrival ISO timestamp; wall mode defaults to current arrival")
    result.add_argument("--run-timeout",type=float,default=600)
    for name in ("workflow","agents","policy","registry"):result.add_argument("--"+name)
    return result


def main():
    arg_parser=parser();args=arg_parser.parse_args()
    try:code=asyncio.run(run(args,write=lambda value:print(value,flush=True)))
    except (ValueError,FileExistsError,OSError) as exc:arg_parser.error(str(exc))
    raise SystemExit(code)


if __name__=="__main__":main()
