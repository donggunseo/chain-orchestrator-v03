"""Async command/completion queue for the same pure reducer as Temporal.

External publishers and console input have independent tasks. No scheduling,
fixture selection, plugin imports or clinical-name dispatch occur here.
"""
import asyncio
import copy
from datetime import datetime, timedelta, timezone

from chain_demo.engine import Engine
from chain_demo.validation import fields, positive, timestamp


class _WallClock:
    synthetic=False
    def datetime(self):return datetime.now(timezone.utc)
    def now(self):return self.datetime().isoformat()
    async def sleep_until(self,at):
        from chain_demo.validation import timestamp
        await asyncio.sleep(max(0,(timestamp(at)-self.datetime()).total_seconds()))


class AsyncLocalRuntime:
    def __init__(self,bundle,initial,activities,*,clock=None,test_mode=False,timer_scale=1.0):
        self.clock=clock or _WallClock();positive(timer_scale)
        if type(test_mode) is not bool:raise ValueError("test_mode must be bool")
        if (self.clock.synthetic or timer_scale!=1) and test_mode is not True:
            raise ValueError("Synthetic clock/timer scale require explicit test mode")
        self.engine=Engine(bundle,initial,now=self.clock.now());self.activities=activities
        self.timer_scale=timer_scale;self.inbox=asyncio.Queue();self.tasks={}
        self.consumer=None;self.result=None;self.closed=False
        self._timer_due={}

    async def timer_status(self):
        """Display current timers in this runtime's actual clock; do not fire them."""
        if self.closed or self.engine.done:return []
        now=self.clock.datetime();result=[]
        active={key:value for key,value in self.engine.timers.items()
                if value["generation"]==self.engine.generation}
        self._timer_due={key:due for key,due in self._timer_due.items() if key in active}
        for key,command in active.items():
            if key not in self._timer_due:continue
            remaining=max(0,(timestamp(self._timer_due[key])-now).total_seconds())
            label="의료진 재확인" if command["purpose"]=="hitl_retry" else "State 대기 알림"
            if command.get("checkpoint"):label+=" · "+command["checkpoint"]
            result.append({"id":key,"label":label,"seconds":command["seconds"]*self.timer_scale,
                           "original_seconds":command["seconds"],"remaining":remaining,
                           "clock_kind":"LOCAL_SYNTHETIC" if self.clock.synthetic else "LOCAL_WALL",
                           "status":"OVERDUE" if remaining==0 else "WAITING"})
        return result

    async def __aenter__(self):
        if self.consumer or self.closed:raise RuntimeError("Runtime cannot restart")
        self.result=asyncio.get_running_loop().create_future()
        self._schedule(self.engine.start(self.clock.now()))
        self.consumer=asyncio.create_task(self._run())
        return self

    async def __aexit__(self,*_):await self.close()

    async def snapshot(self):return self.engine.snapshot()

    async def _submit(self,kind,value):
        if self.closed or not self.consumer or self.consumer.done():raise RuntimeError("Runtime is not accepting inputs")
        receipt=asyncio.get_running_loop().create_future()
        await self.inbox.put({"kind":kind,"value":copy.deepcopy(value),"receipt":receipt})
        await receipt
        return {"status":"SUBMITTED_AWAITING_ENGINE_EFFECTS"}

    async def submit_event(self,event):return await self._submit("external",event)
    async def submit_decision(self,decision):return await self._submit("decision",decision)
    async def request_context(self,request):return await self._submit("context",request)

    async def _run(self):
        try:
            while not self.engine.done:
                message=await self.inbox.get();kind=message["kind"];value=message["value"]
                try:
                    if kind=="external":commands=self.engine.receive(value,self.clock.now())
                    elif kind=="decision":commands=self.engine.decide(value,self.clock.now())
                    elif kind=="context":
                        request=fields(value,{"request_id","scope"},where="Context request")
                        commands=self.engine.request_context(request["request_id"],request["scope"],self.clock.now())
                    elif kind=="timer":commands=self.engine.timer_fired(value,self.clock.now())
                    else:commands=self.engine.complete(message["id"],value,self.clock.now())
                    self._schedule(commands)
                except Exception as exc:
                    if "receipt" in message:message["receipt"].set_exception(exc)
                    else:raise
                else:
                    if "receipt" in message:message["receipt"].set_result(None)
            self.result.set_result(self.engine.finish())
        except asyncio.CancelledError:raise
        except Exception as exc:self.result.set_exception(exc)
        finally:
            self.closed=True
            for task in self.tasks.values():task.cancel()
            for message in list(self.inbox._queue):
                if "receipt" in message and not message["receipt"].done():
                    message["receipt"].set_exception(RuntimeError("Runtime stopped"))

    def _schedule(self,commands):
        self.tasks={key:task for key,task in self.tasks.items() if not task.done()}
        for command in commands:
            due=(self.clock.datetime()+timedelta(seconds=command.get("seconds",0)*self.timer_scale)).isoformat()
            if command["kind"]=="timer":self._timer_due[command["id"]]=due
            self.tasks[command["id"]]=asyncio.create_task(self._execute(command,due))

    async def _execute(self,command,due):
        if command["kind"]=="timer":
            await self.clock.sleep_until(due)
            await self.inbox.put({"kind":"timer","value":command});return
        names={"resolve":"resolve","agent":"invoke","notify":"notify"};reply=None
        for attempt in range(self.engine.policy["maximum_attempts"]):
            try:
                reply=await asyncio.wait_for(getattr(self.activities,names[command["kind"]])(copy.deepcopy(command)),
                                            timeout=command.get("timeout_s",10))
                break
            except asyncio.CancelledError:raise
            except Exception as exc:
                reply={"error":f"{type(exc).__name__}: {exc}"}
                if attempt+1<self.engine.policy["maximum_attempts"]:
                    await self.clock.sleep_until((self.clock.datetime()+timedelta(seconds=1)).isoformat())
        await self.inbox.put({"kind":"completion","id":command["id"],"value":reply})

    async def close(self):
        self.closed=True
        if self.consumer:
            self.consumer.cancel();await asyncio.gather(self.consumer,return_exceptions=True)
        for task in self.tasks.values():task.cancel()
        await asyncio.gather(*self.tasks.values(),return_exceptions=True);self.tasks.clear()
        self._timer_due.clear()
        if self.result and not self.result.done():self.result.cancel()
