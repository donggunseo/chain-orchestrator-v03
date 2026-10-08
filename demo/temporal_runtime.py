"""Client-owned Service/Worker transport. No clinical or fixture dispatch here.

The published Store and embedded Worker share this process. Closing a client
does not approve, cancel or terminate its Workflow. Partial histories remain
partial, and an owned in-memory Service is explicitly ephemeral.
"""
import asyncio
import copy
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from pathlib import Path
from time import monotonic
from uuid import uuid4

from temporalio import activity
from temporalio.client import Client
from temporalio.testing import WorkflowEnvironment
from temporalio.worker import Replayer, Worker

from chain_demo.temporal_workflow import ConfigurableClinicalWorkflow as Workflow
from chain_demo.validation import canonical_hash, positive, timestamp

from .timers import temporal_timer_views


class TemporalActivities:
    """Record actual attempt/identity at the registered Activity boundary."""
    def __init__(self, journal): self.journal = journal

    async def execute(self, method, command):
        info = activity.info()
        metadata = {"attempt": info.attempt, "activity_id": info.activity_id,
                    "workflow_id": info.workflow_id, "workflow_run_id": info.workflow_run_id,
                    "task_queue": info.task_queue}
        return await self.journal.execute(method, command, metadata=metadata)

    @activity.defn(name="chain.resolve_event_v03")
    async def resolve(self, command: dict) -> dict: return await self.execute("resolve", command)

    @activity.defn(name="chain.run_agent_v03")
    async def invoke(self, command: dict) -> dict: return await self.execute("invoke", command)

    @activity.defn(name="chain.notify_v03")
    async def notify(self, command: dict) -> dict: return await self.execute("notify", command)

    @activity.defn(name="chain.apply_effect_v03")
    async def effect(self, command: dict) -> dict: return await self.execute("effect", command)


class TemporalRuntime:
    def __init__(self, bundle, initial, journal, *, address="127.0.0.1:7233", start_local=False,
                 test_mode=False, test_now=None, timer_scale=1.0, output=None, save=None,
                 write=print, bridge_factory=TemporalActivities):
        if type(test_mode) is not bool:raise ValueError("test_mode must be bool")
        positive(timer_scale)
        if (test_now is not None or timer_scale!=1) and not test_mode:
            raise ValueError("Synthetic clock/timer scale require explicit test mode")
        if test_now is not None:timestamp(test_now)
        self.bundle, self.initial, self.journal = bundle, initial, journal
        self.address, self.start_local = address, start_local
        self.test_mode, self.test_now, self.timer_scale = test_mode, test_now, timer_scale
        self.output, self.save, self.write = Path(output) if output else None, save, write
        self.bridge_factory = bridge_factory
        self.handle = self.result = None
        self.closed = False
        self.stack = AsyncExitStack()
        self.cached = {"state": None, "done": False, "audit": []}
        self.execution = {}
        self._timer_history = None
        self._timer_checked_at = float("-inf")
        self.timer_error = None

    async def __aenter__(self):
        try:
            if self.start_local:
                env = await asyncio.wait_for(WorkflowEnvironment.start_local(download_dest_dir="/tmp", ui=False), 60)
                await self.stack.enter_async_context(env)
                client = env.client
            else:
                client = await asyncio.wait_for(Client.connect(self.address), 5)
            self.address = client.service_client.config.target_host
            if not await client.service_client.check_health(): raise RuntimeError("Temporal Service health check failed")
            identity = "chain-v03-" + uuid4().hex
            self.execution = {"backend": "temporal", "address": self.address, "namespace": client.namespace,
                "workflow_id": identity, "task_queue": identity, "owned_in_memory_service": self.start_local,
                "test_mode": self.test_mode, "test_timer_scale": self.timer_scale,
                "close_policy": "CLIENT_AND_EMBEDDED_WORKER_STOP; no cancel/terminate Signal"}
            bridge = self.bridge_factory(self.journal)
            await self.stack.enter_async_context(Worker(client, task_queue=identity, workflows=[Workflow],
                                        activities=[bridge.resolve, bridge.invoke, bridge.notify, bridge.effect]))
            argument = {"bundle": self.bundle, "initial": self.initial}
            if self.test_mode:
                argument.update(test_mode=True, test_now=self.test_now, test_timer_scale=self.timer_scale)
            self.handle = await client.start_workflow(Workflow.run, argument, id=identity, task_queue=identity)
            self.result = asyncio.create_task(self.handle.result())
            async with asyncio.timeout(30):
                while (await self.snapshot()).get("state") is None:
                    if self.result.done(): await self.result
                    await asyncio.sleep(.02)
            self.write("REAL_TEMPORAL_ADDRESS | " + self.address)
            if self.output:self.save(self.output / "temporal_connection.json", self.execution)
            return self
        except BaseException:
            self.closed = True
            await self.stack.aclose()
            raise

    async def snapshot(self):
        if self.handle is None or self.closed: return copy.deepcopy(self.cached)
        self.cached = await self.handle.query("snapshot")
        return copy.deepcopy(self.cached)

    async def timer_status(self):
        """Observe durable timers; a failed display lookup never changes the run."""
        if self.handle is None or self.closed:return []
        if monotonic()-self._timer_checked_at>=.5:
            self._timer_checked_at=monotonic()
            try:
                async with asyncio.timeout(3):
                    self._timer_history=await self.handle.fetch_history()
                self.timer_error=None
            except Exception as exc:
                self.timer_error=f"{type(exc).__name__}: {exc}"
        if self._timer_history is None:return []
        return temporal_timer_views(self._timer_history,now=datetime.now(timezone.utc),
                                    timer_scale=self.timer_scale,verified=self.timer_error is None)

    async def submit_event(self, value): await self.handle.signal("submit_event", value)
    async def submit_decision(self, value): await self.handle.signal("submit_decision", value)
    async def request_hitl_resume(self, value): await self.handle.signal("request_hitl_resume", value)
    async def request_context(self, value): await self.handle.signal("request_context", value)

    async def advance_test_time(self, at):
        if not self.test_mode: raise ValueError("Clock advancement requires explicit test mode")
        at = timestamp(at).isoformat()
        await self.handle.signal("advance_test_time", at)
        async with asyncio.timeout(15):
            while True:
                snapshot = await self.snapshot()
                if any(e["type"] == "TEST_CLOCK_ADVANCED" and timestamp(e["time"]) == timestamp(at)
                       for e in snapshot.get("audit", [])): return
                if any(e["type"] == "TEST_CLOCK_REJECTED" for e in snapshot.get("audit", [])):
                    raise ValueError("Workflow rejected synthetic clock advancement")
                if self.result and self.result.done(): await self.result; raise RuntimeError("Workflow ended during clock advancement")
                await asyncio.sleep(.02)

    async def __aexit__(self, *_):
        try:
            if self.handle and self.output:
                async with asyncio.timeout(30):
                    history = await self.handle.fetch_history()
                    raw = history.to_json()
                    import json
                    self.save(self.output / "temporal_history.json", json.loads(raw))
                    description = await self.handle.describe()
                    self.execution.update(run_id=description.run_id, workflow_status=description.status.name,
                                          history_hash=canonical_hash(json.loads(raw)))
                    self.save(self.output / "temporal_execution.json", self.execution)
                    report = {"status": "FAILED", "workflow_id": history.workflow_id,
                              "history_hash": self.execution["history_hash"], "history_event_count": len(history.events),
                              "workflow_status": description.status.name, "error": None}
                    try:
                        await Replayer(workflows=[Workflow]).replay_workflow(history)
                        report["status"] = "PASSED"
                    except Exception as exc:
                        report["error"] = f"{type(exc).__name__}: {exc}"
                        raise
                    finally:
                        self.save(self.output / "replay.json", report)
                    self.write("REAL_HISTORY_REPLAY | " + report["status"] + " | " + description.status.name)
        finally:
            self.closed = True
            if self.result:
                self.result.cancel()
                await asyncio.gather(self.result, return_exceptions=True)
            await self.stack.aclose()
