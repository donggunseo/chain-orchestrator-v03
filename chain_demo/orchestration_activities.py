"""Worker-owned command boundaries for the v0.3 reducer."""
import asyncio

from temporalio import activity

from .adapters.ingress import resolve_source
from .orchestration_schema import validate_engine_bundle
from .validation import fields
from .effects import MockPresentationEffects
from .effect_contracts import validate_effect_receipt
from .notifier import MockNotifier


class OrchestrationActivities:
    def __init__(self,engine_bundle,runtime,store,notifier,effects=None):
        validate_engine_bundle(engine_bundle)
        if engine_bundle["catalog_hash"]!=runtime.bundle["bundle_hash"]:
            raise ValueError("Worker Runtime does not match Engine catalog")
        if store.identity["site_id"]!=engine_bundle["workflow"]["site_id"]:
            raise ValueError("Worker Store site does not match Workflow")
        if notifier.templates!=frozenset(engine_bundle["policy"]["notification_templates"]):
            raise ValueError("Notifier templates do not match Policy")
        self.engine_hash=engine_bundle["bundle_hash"]
        self.runtime=runtime;self.store=store;self.notifier=notifier
        approved=frozenset(engine_bundle["policy"].get("allowed_effect_operations",[]))
        self.effects=effects
        if self.effects is None and approved:
            if not isinstance(notifier,MockNotifier):
                raise ValueError("An explicit presentation effect adapter is required with a custom Notifier")
            self.effects=MockPresentationEffects(notifier.path,allowed_operations=approved,write=notifier.write)
        if self.effects is not None and self.effects.operations!=approved:
            raise ValueError("Presentation effect operations do not match Policy")

    def _check(self,command,kind,required):
        fields(command,required|{"kind","id","engine_hash"},where="Worker command")
        if command["kind"]!=kind or command["engine_hash"]!=self.engine_hash:
            raise ValueError("Command does not match pinned Worker Workflow")

    @activity.defn(name="chain.resolve_event_v03")
    async def resolve(self,command:dict)->dict:
        self._check(command,"resolve",{"event"})
        return await asyncio.to_thread(resolve_source,command["event"],self.store)

    @activity.defn(name="chain.run_agent_v03")
    async def invoke(self,command:dict)->dict:
        self._check(command,"agent",{"alias","mode","purpose","job","generation","revision_epoch","timeout_s","consumers","single_flight"})
        if command["id"]!=command["job"]["request"]["request_id"]:
            raise ValueError("Command/Request identity mismatch")
        return await asyncio.to_thread(self.runtime.invoke,command["job"])

    @activity.defn(name="chain.notify_v03")
    async def notify(self,command:dict)->dict:
        self._check(command,"notify",{"job"})
        job=fields(command["job"],{"catalog_hash","request","recorded_at"},where="Notification job")
        if job["catalog_hash"]!=self.runtime.bundle["bundle_hash"] or command["id"]!=job["request"]["request_id"]:
            raise ValueError("Notification does not match pinned Worker")
        return await asyncio.to_thread(self.notifier.record,job["request"],recorded_at=job["recorded_at"])

    @activity.defn(name="chain.apply_effect_v03")
    async def effect(self,command:dict)->dict:
        self._check(command,"effect",{"job"})
        if self.effects is None:
            raise ValueError("Presentation effect adapter is not configured")
        job=fields(command["job"],{"catalog_hash","request","recorded_at"},where="Presentation effect job")
        if job["catalog_hash"]!=self.runtime.bundle["bundle_hash"] or command["id"]!=job["request"]["request_id"]:
            raise ValueError("Presentation effect does not match pinned Worker")
        receipt=await asyncio.to_thread(self.effects.record,job["request"],recorded_at=job["recorded_at"])
        validate_effect_receipt(receipt,job["request"])
        return receipt
