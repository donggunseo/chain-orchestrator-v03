"""v0.3 Temporal adapter; plugins and source/fixture/SQLite I/O stay in Activities."""
import asyncio
from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError, ApplicationError

from .engine import Engine
from .validation import fields, positive, timestamp


@workflow.defn(name="ChainOrchestratorV03")
class ConfigurableClinicalWorkflow:
    def __init__(self):
        self.engine = None
        self.inbox = []
        self.tasks = {}
        self.test_now = None
        self.timer_scale = 1.0

    @workflow.signal
    def submit_event(self, event: dict):
        self.inbox.append({"kind": "external", "event": event})

    @workflow.signal
    def submit_decision(self, decision: dict):
        self.inbox.append({"kind": "decision", "decision": decision})

    @workflow.signal
    def request_context(self, request: dict):
        self.inbox.append({"kind": "context", "request": request})

    @workflow.signal
    def advance_test_time(self, at: str):
        """Synthetic test driver only; ordinary runs keep Workflow's own clock."""
        self.inbox.append({"kind":"test_clock","at":at})

    @workflow.query
    def snapshot(self) -> dict:
        return self.engine.snapshot() if self.engine else {"state": None}

    def _now(self):
        return self.test_now if self.test_now else workflow.now().isoformat()

    @workflow.run
    async def run(self, argument: dict) -> dict:
        try:
            fields(argument, {"bundle", "initial"}, {"test_mode", "test_now", "test_timer_scale"}, "Workflow argument")
            if "test_mode" in argument and type(argument["test_mode"]) is not bool:
                raise ValueError("test_mode must be bool")
            if "test_now" in argument or "test_timer_scale" in argument:
                if argument.get("test_mode") is not True:
                    raise ValueError("Synthetic clock/timer scale require explicit test mode")
            if "test_now" in argument:
                timestamp(argument["test_now"])
                self.test_now = argument["test_now"]
            self.timer_scale = argument.get("test_timer_scale", 1.0)
            positive(self.timer_scale)
            self.engine = Engine(argument["bundle"], argument["initial"], now=self._now())
            self._schedule(self.engine.start(self._now()))
        except (ValueError, KeyError, TypeError) as exc:
            raise ApplicationError(f"Invalid v0.3 startup: {exc}", non_retryable=True) from exc
        try:
            while not self.engine.done:
                await workflow.wait_condition(lambda: bool(self.inbox))
                message = self.inbox.pop(0)
                kind = message["kind"]
                if kind == "external":
                    commands = self.engine.receive(message["event"], self._now())
                elif kind == "decision":
                    commands = self.engine.decide(message["decision"], self._now())
                elif kind == "context":
                    try:
                        request = fields(message["request"], {"request_id", "scope"}, where="Context signal")
                        commands = self.engine.request_context(request["request_id"], request["scope"], self._now())
                    except ValueError as exc:
                        self.engine._log("CONTEXT_REQUEST_REJECTED", reason=str(exc))
                        commands = []
                elif kind == "timer":
                    commands = self.engine.timer_fired(message["timer"], self._now())
                elif kind == "test_clock":
                    try:
                        if self.test_now is None:
                            raise ValueError("Clock advancement requires explicit test_mode and test_now")
                        at=timestamp(message["at"]).isoformat()
                        self.engine._time(at)
                    except (ValueError, TypeError) as exc:
                        self.engine._log("TEST_CLOCK_REJECTED",reason=str(exc))
                    else:
                        self.test_now=at
                        self.engine._log("TEST_CLOCK_ADVANCED")
                    commands=[]
                elif kind == "fatal":
                    raise ApplicationError(message["error"], non_retryable=True)
                else:
                    commands = self.engine.complete(message["id"], message["reply"], self._now())
                self._schedule(commands)
            return self.engine.finish()
        finally:
            remaining = [task for task in self.tasks.values() if not task.done()]
            for task in remaining:
                task.cancel()
            if remaining:
                await asyncio.gather(*remaining, return_exceptions=True)

    def _schedule(self, commands):
        self.tasks = {key:task for key,task in self.tasks.items() if not task.done()}
        for command in commands:
            self.tasks[command["id"]] = asyncio.create_task(self._execute(command))

    async def _execute(self, command):
        if command["kind"] == "timer":
            await workflow.sleep(command["seconds"] * self.timer_scale)
            self.inbox.append({"kind": "timer", "timer": command})
            return
        try:
            retry = RetryPolicy(initial_interval=timedelta(seconds=1),
                                maximum_attempts=self.engine.policy["maximum_attempts"])
            names = {"resolve":"chain.resolve_event_v03", "agent":"chain.run_agent_v03", "notify":"chain.notify_v03",
                     "effect":"chain.apply_effect_v03"}
            timeout = command.get("timeout_s", 10)
            reply = await workflow.execute_activity(names[command["kind"]], command, result_type=dict,
                    start_to_close_timeout=timedelta(seconds=timeout),
                    schedule_to_close_timeout=timedelta(seconds=timeout * self.engine.policy["maximum_attempts"] + 5),
                    retry_policy=retry)
        except ActivityError as exc:
            reply = {"error": str(exc)}
        except Exception as exc:
            self.inbox.append({"kind":"fatal", "error":f"{type(exc).__name__}: {exc}"})
            return
        self.inbox.append({"kind":"completion", "id":command["id"], "reply":reply})
