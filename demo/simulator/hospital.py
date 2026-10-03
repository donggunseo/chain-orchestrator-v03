"""External Mock hospital: actual observations activate source publications.

Source templates open only when their causal node occurs. An order is a virtual
clinician's action here, never an engine/notifier effect. Standard Events alone
cross the transport boundary. Expected paths and Agent fixtures are not read.
"""
import asyncio
import copy
from datetime import timedelta
import hashlib
import json
from pathlib import Path

from chain_demo.adapters import EMRAdapter, NursingAdapter, OCSAdapter, LISAdapter, RISPACSAdapter, ManualAdapter
from chain_demo.adapters.ingress import SourceIngress
from chain_demo.data_io import read_json
from chain_demo.source_contracts import identity
from chain_demo.validation import canonical_hash, timestamp
from .rebinding import rebind_record
from .scheduler import CausalScheduler, load_plan


class HospitalSimulator:
    def __init__(self,episode,staging,store,clock,emit,*,plan_path=None):
        self.episode=Path(episode).resolve();self.staging=Path(staging)
        if self.staging.exists():raise ValueError("Refusing to overwrite a staging directory")
        self.staging.mkdir(parents=True)
        self.store=store;self.clock=clock;self.emit=emit;self.ingress=None
        self.plan=load_plan(plan_path or self.episode/"simulation.yaml")
        origins=[r for r in self.plan["releases"] if r["anchor"]=="ARRIVAL" and r["offset_s"]==0]
        if len(origins)!=1:raise ValueError("Hospital requires exactly one ARRIVAL+0 source origin")
        self.reference_arrival=origins[0]["reference_source_time"]
        self.scheduler=CausalScheduler(self.plan,clock,self._occur)
        self.publications=[];self.failures=[];self.tasks=[];self.started=False
        code={str(p.relative_to(Path(__file__).parent)):"sha256:"+hashlib.sha256(p.read_bytes()).hexdigest()
              for p in sorted(Path(__file__).parent.glob("*.py"))}
        self.manifest={"simulation_version":"0.3-step6","plan_hash":canonical_hash(self.plan),
                       "implementation_hash":canonical_hash(code),"implementation_files":code,"source_hashes":{}}

    async def __aenter__(self):await self.scheduler.__aenter__();return self

    async def __aexit__(self,*args):
        await self.scheduler.__aexit__(*args)
        for task in self.tasks:task.cancel()
        await asyncio.gather(*self.tasks,return_exceptions=True);self.tasks=[]

    async def start(self,initial):
        if self.started:raise RuntimeError("Hospital already started")
        if identity(initial)!=self.store.identity:raise ValueError("Hospital patient/episode mismatch")
        self.ingress=SourceIngress(initial,self.store)
        self.arrival_shift=timestamp(initial["arrival_time"])-timestamp(self.reference_arrival)
        self.started=True
        await self.scheduler.mark("ARRIVAL",initial["arrival_time"])

    async def observe(self,snapshot):
        if snapshot.get("episode_id")!=self.store.identity["episode_id"]:raise ValueError("Observed different episode")
        for item in snapshot.get("audit",[]):
            if item["type"]!="STATE_ENTERED":continue
            anchor=self.plan["observations"]["states"].get(item["state"])
            if anchor and anchor not in self.scheduler.anchors:
                await self.scheduler.mark(anchor,item["time"],{"state":item["state"]})
        history=snapshot.get("hitl_history",{})
        for event in snapshot.get("audit",[]):
            if event["type"]!="HITL_DECISION_RECORDED":continue
            record=history.get(event.get("request_id"),{})
            if record.get("status")!="DECIDED":continue
            checkpoint=record["request"]["checkpoint"]
            for rule in self.plan["observations"]["decisions"]:
                if checkpoint==rule["checkpoint"] and event["decision"]==rule["decision"] and rule["anchor"] not in self.scheduler.anchors:
                    await self.scheduler.mark(rule["anchor"],event["time"],{"request_id":event["request_id"]})

    async def watch(self,query,*,poll_interval=.05):
        if poll_interval<=0:raise ValueError("Poll interval must be positive")
        while True:
            await self.observe(await query())
            await asyncio.sleep(poll_interval)

    async def _occur(self,node,at):
        if not self.started:raise RuntimeError("Hospital was not started")
        path=(self.episode/node["source"]).resolve()
        if not path.is_relative_to(self.episode):raise ValueError("Source escaped episode")
        original=await asyncio.to_thread(read_json,path)
        if identity(original)!=self.store.identity:raise ValueError("Source belongs to different episode")
        record=rebind_record(original,node,occurred=at,arrival_shift=self.arrival_shift,anchors=self.scheduler.anchors)
        self.manifest["source_hashes"][node["source"]]=original["content_hash"]
        destination=self.staging/(node["id"]+".json")
        with destination.open("x",encoding="utf8") as stream:
            json.dump(record,stream,ensure_ascii=False,indent=2);stream.write("\n")
        self.tasks.append(asyncio.create_task(self._publish(node,record,at)))
        return {"source_ref":copy.deepcopy(record["source_ref"]),
                **({"order_id":record["payload"]["order_id"]} if "order_id" in record["payload"] else {})}

    async def _publish(self,node,record,occurred):
        try:
            due=(timestamp(occurred)+timedelta(seconds=self.plan["publication_delay"]["offset_s"])).isoformat()
            await self.clock.sleep_until(due)
            classes={"ER_EMR":EMRAdapter,"NURSING_EMR":NursingAdapter,"OCS":OCSAdapter,
                     "LIS":LISAdapter,"RIS_PACS":RISPACSAdapter,"MANUAL_INPUT":ManualAdapter}
            adapter=classes[record["source_ref"]["system"]](self.staging,self.store)
            event=adapter.publish(node["id"]+".json",not_before=due,at=self.clock.now(),
                                  event_id=self.store.identity["episode_id"]+":SIM:"+node["id"])
            event=self.ingress.receive(event,received_at=self.clock.now())
            await self.emit(copy.deepcopy(event))
            self.publications.append({"id":node["id"],"actor":node.get("actor","MOCK_SOURCE"),"event":event})
        except asyncio.CancelledError:raise
        except Exception as exc:self.failures.append({"id":node["id"],"error":f"{type(exc).__name__}: {exc}"})
