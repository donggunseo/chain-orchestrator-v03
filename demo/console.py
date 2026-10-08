"""Fixed-evidence input at Client boundaries; never performs Workflow I/O.

One pending line read is retained as prompts change. Selectable stdin reads are
cancellable; arbitrary injected blocking callbacks cannot be forcibly stopped,
but are never duplicated or canceled merely because a HITL timer expires.
"""
import asyncio
import copy
from datetime import datetime, timezone
import json
import os
import select
import sys
from threading import Event
from uuid import uuid4

from chain_demo.contracts import validate_hitl_request, validate_hitl_decision, validate_hitl_resume_shape
from chain_demo.hitl_documents import validate_source_documents
from .presentation import (CONFIRMATION_LABELS, OPTION_LABELS, ROLE_LABELS, label,
                           show_documents, show_evidence)


def _clock():
    return datetime.now(timezone.utc).isoformat()


def _bundle(record):
    record = copy.deepcopy(record)
    if record.get("status") != "OPEN":raise ValueError("Console requires an open HITL request")
    if not isinstance(record.get("snapshot"), dict):
        raise ValueError("HITL 판단 근거 Snapshot이 없거나 형식이 올바르지 않습니다.")
    validate_hitl_request(record["request"], record["snapshot"], record["results"])
    if "source_documents" in record:
        validate_source_documents(record["source_documents"], record["request"], record["snapshot"])
    return record


def show_request(record, write=print):
    record = _bundle(record)
    lines = [];show_evidence(record, lines.append)
    write("\n".join(lines))
    return record


def _actor_form(roles, write):
    while True:
        write("역할: " + ", ".join(f"{i+1}={label(role, ROLE_LABELS)}" for i, role in enumerate(roles)))
        role = (yield "모의 의료진 역할 번호/코드: ").strip().upper()
        if role.isdigit() and 1 <= int(role) <= len(roles):role = roles[int(role)-1]
        if role in roles:break
        write("허용된 역할을 입력해 주세요.")
    staff = (yield "모의 의료진 ID [DEMO-CLINICIAN]: ").strip() or "DEMO-CLINICIAN"
    return {"role":role, "staff_id":staff}


def _decision_form(record, write, clock):
    record = show_request(record, write);request = record["request"]
    while True:
        for i, option in enumerate(request["options"], 1):write(f"{i}. {label(option, OPTION_LABELS)}")
        choice = (yield "선택 번호/코드 (json=고정 자료 전체, docs=출처 원문, q=중단): ").strip()
        if choice.lower() == "q":raise KeyboardInterrupt()
        if choice.lower() == "ready":
            write("이미 새 HITL이 열렸습니다. 이 요청의 선택지를 입력해 주세요.");continue
        if choice.lower() == "json":
            write("[고정 자료 원문] snapshot = 판단 근거 │ " +
                  ("source_documents = 출처 원문 열람자료" if "source_documents" in record
                   else "출처 원문 열람자료가 없는 구형 요청"))
            write(json.dumps(record, ensure_ascii=False, indent=2, sort_keys=True));continue
        if choice.lower() == "docs":
            lines = [];show_documents(record, lines.append)
            write("\n".join(lines));continue
        if choice.isdigit() and 1 <= int(choice) <= len(request["options"]):choice = request["options"][int(choice)-1]
        else:choice = choice.upper()
        if choice not in request["options"]:write("허용된 선택지를 입력해 주세요.");continue
        confirmed = []
        if choice in request["confirmations_on_decisions"]:
            for item in request["required_confirmations"]:
                if (yield label(item, CONFIRMATION_LABELS) + "? [y/N]: ").strip().lower() not in {"y", "yes"}:break
                confirmed.append(item)
            if len(confirmed) != len(request["required_confirmations"]):
                write("확인되지 않은 항목이 있어 해당 결정을 전송하지 않았습니다.");continue
        break
    actor = yield from _actor_form(request["roles"], write)
    while True:
        reason = (yield "결정/보류 사유 (필수): ").strip()
        if reason:break
        write("사유를 입력해 주세요.")
    decision = {"contract_schema":"chain-hitl-decision/v0.3", "request_id":request["request_id"],
        "evidence_snapshot_id":request["evidence_snapshot_id"], "decision":choice,
        "actor":actor, "confirmed_items":confirmed,
        "evidence_viewed":[request["evidence_snapshot_id"], *request["result_ids"]],
        "comment":reason, "recorded_at":clock()}
    validate_hitl_decision(decision, request)
    return decision


def _resume_form(wait, request, write, clock):
    write("HITL_WAITING | " + wait["prior_request_id"] + " | 현재 상태를 유지하며 자동 재요청을 기다립니다.")
    if wait["status"] == "FAILED":write("HITL 준비에 실패했습니다. 준비 상태를 확인하거나 ready로 다시 요청할 수 있습니다.")
    while True:
        choice = (yield "보류 중: ready=지금 다시 결정하기, q=콘솔 입력 종료: ").strip().lower()
        if choice == "q":raise KeyboardInterrupt()
        if choice == "ready":break
        write("ready 또는 q를 입력해 주세요.")
    actor = yield from _actor_form(request["roles"], write)
    command = {"contract_schema":"chain-hitl-resume/v0.3", "command_id":uuid4().hex,
        "prior_request_id":wait["prior_request_id"], "actor":actor, "requested_at":clock()}
    validate_hitl_resume_shape(command)
    return command


def _closed(write):
    write("INPUT_CLOSED_NO_DECISION — 응답이나 재요청을 만들거나 전송하지 않았습니다.")


def collect_decision(record, *, input_fn=input, write=print, clock=_clock):
    """Synchronous form for callers that own their input lifetime."""
    form = _decision_form(record, write, clock)
    try:
        prompt = next(form)
        while True:
            answer = input_fn(prompt)
            if answer.strip().lower() == "q":raise KeyboardInterrupt()
            try:prompt = form.send(answer)
            except StopIteration as result:return result.value
    except (EOFError, KeyboardInterrupt):
        _closed(write)
        return None
    finally:form.close()


class StdinReader:
    """One buffered pipe/TTY consumer with selectable, cancellable async reads."""
    def __init__(self, stop=None, write=print):
        self.stop = stop if stop is not None else Event()
        self.write = write;self.buffer = b"";self.eof = False

    def _line(self):
        if b"\n" in self.buffer:
            line, self.buffer = self.buffer.split(b"\n", 1)
            return line.decode(sys.stdin.encoding or "utf8").rstrip("\r")
        if self.eof:
            if self.buffer:
                line, self.buffer = self.buffer, b""
                return line.decode(sys.stdin.encoding or "utf8")
            raise EOFError
        return None

    def _available(self, timeout):
        if select.select([sys.stdin.fileno()], [], [], timeout)[0]:
            data = os.read(sys.stdin.fileno(), 4096)
            if data:self.buffer += data
            else:self.eof = True

    def __call__(self, prompt):
        self.write(prompt)
        while not self.stop.is_set():
            line = self._line()
            if line is not None:return line
            self._available(.1)
        raise EOFError

    async def read_async(self, prompt):
        self.write(prompt)
        while not self.stop.is_set():
            line = self._line()
            if line is not None:return line
            self._available(0)
            await asyncio.sleep(.01)
        raise EOFError


class _InputLines:
    """Own exactly one line task, including while a wait form is replaced."""
    def __init__(self, input_fn, write):
        self.input_fn = StdinReader(write=write) if input_fn is input else input_fn
        self.write = write;self.task = None;self.prompt = None;self.discard_next = False

    def start(self, prompt):
        if self.task is None:
            self.prompt = prompt
            reader = getattr(self.input_fn, "read_async", None)
            self.task = asyncio.create_task(reader(prompt) if reader else asyncio.to_thread(self._read_blocking, prompt))
        elif prompt != self.prompt:
            self.prompt = prompt
            if not self.discard_next:self.write(prompt)

    def _read_blocking(self, prompt):
        try:return self.input_fn(prompt)
        except (EOFError, KeyboardInterrupt):raise _InputClosed from None

    def take(self):
        task, self.task = self.task, None
        self.prompt = None
        return task.result()

    async def discard_actor_input(self):
        if self.task is None:return
        if getattr(self.input_fn, "read_async", None):
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None;self.prompt = None
        else:
            self.discard_next = True
            self.write("이전 재요청의 역할/ID 입력은 전송하지 않습니다. 해당 입력이 끝나면 새 선택 안내를 표시합니다.")

    async def close(self):
        if self.task is not None:
            # Cancels selectable stdin immediately. An injected blocking callback
            # may finish later; no replacement read is started after shutdown.
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)


class _InputClosed(Exception):
    """Carry injected input interruption without interrupting the event loop."""


async def serve_console(query, submit, *, resume=None, input_fn=input, write=print, clock=_clock,
                        poll_interval=.1, on_request=None):
    """Runtime-supplied Client callbacks; transport ACK is not engine acceptance.

    Open decision forms remain bound to their original Snapshot. A wait form can
    yield to a new request immediately, retaining its single pending line read.
    EOF/q stops this component; neither cancels nor approves the Workflow.
    """
    if poll_interval <= 0:raise ValueError("Positive console poll interval required")
    awaiting = {};resume_awaiting = {};resume_reported = {};lines = _InputLines(input_fn, write)
    form = None;kind = None;identity = None;prompt = None;warned = False;wait_status = None
    try:
        while True:
            snapshot = await query()
            for request_id, offset in list(awaiting.items()):
                outcome = next((e for e in snapshot.get("audit", [])[offset:] if e.get("request_id") == request_id
                    and e["type"] in {"HITL_REJECTED", "HITL_DECISION_RECORDED"}), None)
                if outcome:
                    write(("DECISION_REJECTED" if outcome["type"] == "HITL_REJECTED" else "DECISION_ACCEPTED") +
                        " | " + request_id + " | " + (outcome.get("reason", "") if outcome["type"] == "HITL_REJECTED"
                        else "엔진이 응답을 수락했습니다 · " + label(outcome["decision"], OPTION_LABELS)))
                    if outcome["type"] == "HITL_DECISION_RECORDED" and outcome.get("decision") in {"DEFER", "HOLD"}:
                        write("현재 상태를 유지하며 재요청 타이머를 기다립니다. 준비되면 ready로 먼저 재요청할 수 있습니다. 독립적인 병원 Event와 Agent 응답은 계속 처리됩니다.")
                    del awaiting[request_id]
            for command_id in list(resume_awaiting):
                command = snapshot.get("hitl_resume_commands", {}).get(command_id)
                if command and command.get("status") != resume_reported.get(command_id):
                    status = command["status"];resume_reported[command_id] = status
                    diagnostic = command.get("reason", command.get("error"))
                    write("RESUME_" + status + " | " + command_id + " | " + {
                        "ACCEPTED":"엔진이 재요청을 수락했습니다 · 근거 준비 중",
                        "OPEN":"새 HITL이 열렸습니다", "FAILED":"HITL 근거 준비 실패",
                        "REJECTED":"재요청이 거절되었습니다", "CLOSED":"대상 보류가 종료되었습니다",
                    }.get(status, status) + (" · " + str(diagnostic) if diagnostic else ""))
                    if status in {"OPEN", "FAILED", "REJECTED", "CLOSED"}:del resume_awaiting[command_id]
            if snapshot.get("done"):return
            record = next((copy.deepcopy(r) for r in snapshot.get("open_hitl", {}).values()
                if r["status"] == "OPEN" and r["generation"] == snapshot.get("generation")
                and r["request"]["request_id"] not in awaiting), None)
            wait = next((copy.deepcopy(w) for w in snapshot.get("hitl_waits", {}).values()
                if w["generation"] == snapshot.get("generation") and w["status"] in {"WAITING", "FAILED"}), None)
            if kind == "decision":
                historical = snapshot.get("hitl_history", {}).get(identity, {})
                if historical.get("status") in {"INVALIDATED", "CLOSED"} and not warned:
                    write(("REQUEST_INVALIDATED" if historical["status"] == "INVALIDATED" else "REQUEST_CLOSED") +
                          " | " + identity + " | 입력 중인 답은 원래 요청에 결속됩니다.")
                    warned = True
            elif record is not None:
                if form:form.close()
                if kind == "resume":
                    if not prompt.startswith("보류 중:"):await lines.discard_actor_input()
                    write("HITL_REOPENED | 새 HITL이 열렸습니다. 새 질문의 선택지를 확인해 주세요.")
                identity = record["request"]["request_id"];kind = "decision";warned = False
                if on_request:on_request(copy.deepcopy(record["request"]))
                form = _decision_form(record, write, clock);prompt = next(form)
                lines.start(prompt)
            elif resume is not None and wait is not None:
                if kind != "resume" or identity != wait["prior_request_id"] or wait_status != wait["status"]:
                    if form:form.close()
                    identity = wait["prior_request_id"];kind = "resume";wait_status = wait["status"]
                    request = snapshot["hitl_history"][identity]["request"]
                    form = _resume_form(wait, request, write, clock);prompt = next(form)
                    lines.start(prompt)
            elif kind == "resume":
                if not prompt.startswith("보류 중:"):await lines.discard_actor_input()
                form.close();form = None;kind = None;identity = None
                write("HITL_REOPENING | 근거 준비 중 · 새 질문이 열리면 입력을 이어갑니다.")
            if lines.task is not None and lines.task.done():
                if form is None:
                    # EOF/q remain effective during preparation; any other line
                    # waits for the next form so it cannot trigger a stale resume.
                    try:answer = lines.task.result()
                    except (EOFError, KeyboardInterrupt, _InputClosed):_closed(write);return
                    if answer.strip().lower() == "q":lines.take();_closed(write);return
                else:
                    try:
                        answer = lines.take()
                        if answer.strip().lower() == "q":_closed(write);return
                        if lines.discard_next:
                            lines.discard_next = False
                            lines.start(prompt)
                            continue
                        try:prompt = form.send(answer)
                        except StopIteration as result:
                            value = result.value
                            if kind == "decision":
                                await submit(value);awaiting[identity] = len(snapshot.get("audit", []))
                                write("DECISION_SUBMITTED_AWAITING_ENGINE | " + identity + " | 응답 전송됨 · 엔진 수락 확인 대기")
                            else:
                                await resume(value);resume_awaiting[value["command_id"]] = True
                                write("RESUME_SUBMITTED_AWAITING_ENGINE | " + value["command_id"] + " | 재요청 전송됨 · 엔진 수락 확인 대기")
                            form = None;kind = None;identity = None
                        else:lines.start(prompt)
                    except (EOFError, KeyboardInterrupt, _InputClosed):_closed(write);return
            await asyncio.sleep(poll_interval)
    finally:
        if form:form.close()
        await lines.close()


async def serve_temporal_console(handle, **kwargs):
    """Use an existing Workflow handle without starting a CLI or Simulator."""
    async def query():return await handle.query("snapshot")
    async def submit(decision):await handle.signal("submit_decision", decision)
    async def resume(command):await handle.signal("request_hitl_resume", command)
    return await serve_console(query, submit, resume=resume, **kwargs)
