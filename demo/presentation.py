"""Pure demo display; never changes evidence, decisions or engine timers.

Readable labels live here, outside the runtime and its Workflow. The diagnostic
JSON command remains the authoritative unabridged representation of a request.
"""
import math


FIELD_LABELS = {
    "patient_id": "환자 ID", "encounter_id": "내원 ID", "episode_id": "에피소드 ID",
    "site_id": "기관 ID", "age": "나이", "sex": "성별", "bed": "병상",
    "arrival_time": "도착 시각", "lkw": "마지막 정상 확인 시각",
    "glucose": "혈당", "sbp": "수축기 혈압", "dbp": "이완기 혈압",
    "weight_kg": "체중", "platelet_count": "혈소판 수", "inr": "INR",
    "nihss": "NIHSS", "anticoagulant": "항응고제 정보",
    "ct_order_id": "CT 오더 ID", "ncct_order_id": "NCCT 결과의 오더 ID",
    "ncct_completed": "NCCT 완료 정보", "screening_result": "Screening 결과",
    "assessment": "의사결정지원 결과", "mode": "실행 모드", "mock_only": "Mock 출력",
    "basis": "결과 근거", "missing_information": "미확보 항목",
}
STATUS_LABELS = {
    "AVAILABLE": "확보", "COMPLETED": "자료 완료", "UNKNOWN": "미확보",
    "PENDING": "결과 대기", "RETRACTED": "철회", "ERROR": "오류",
    "CONFLICT": "출처 간 충돌", "INVALIDATED": "무효화", "NO_EVIDENCE": "근거 없음",
    "UNCONFIRMED": "미확인", "CONFIRMED": "확인됨",
    "PHYSICIAN_READ_REQUIRED": "의료진 직접 판독 필요",
}
OPTION_LABELS = {
    "PROCEED_TO_CT": "CT 경로 진행 승인", "NOT_STROKE_PATHWAY": "해당 경로 종료",
    "DEFER": "결정 보류 · 현재 상태 유지",
    "THROMBOLYSIS_YES": "YES · 시행 계획 승인",
    "THROMBOLYSIS_NO": "NO · 시행 계획 없이 종료",
    "HOLD": "결정 보류 · 현재 상태 유지",
}
ROLE_LABELS = {"NEUROLOGIST": "신경과 의료진", "EM_PHYSICIAN": "응급의학과 의료진"}
CONFIRMATION_LABELS = {
    "NCCT_NO_HEMORRHAGE_PHYSICIAN_READ": "NCCT를 직접 판독하고 출혈 없음 확인",
    "LKW_RECONFIRMED_WITH_FAMILY": "보호자와 마지막 정상 확인 시각 재확인",
    "BP_RECHECK_BELOW_185_110": "혈압 재측정 및 185/110 미만 확인",
    "NO_ANTICOAGULANT_RECONFIRMED": "항응고제 복용 없음 재확인",
}
CLOCK_LABELS = {
    "LOCAL_SYNTHETIC": "Local 가상 시계", "LOCAL_WALL": "Local 실제 시계",
    "TEMPORAL_DURABLE": "실제 Temporal 타이머",
}


def safe_text(value, *, multiline=False):
    """Escape terminal control characters without altering the stored value."""
    text = str(value)
    return "".join(char if (ord(char) >= 32 and ord(char) != 127) or (multiline and char == "\n")
                   else "\\n" if char == "\n" else f"\\x{ord(char):02x}" for char in text)


def label(code, labels=FIELD_LABELS):
    code = safe_text(code)
    return f"{labels[code]} ({code})" if code in labels else code


def value_text(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    return safe_text(value)


def source_text(ref):
    parts = [safe_text(ref.get("system", "?")), safe_text(ref.get("record_id", "?")),
             "v" + safe_text(ref.get("version", "?"))]
    if "field" in ref:
        parts.append(safe_text(ref["field"]))
    return " · ".join(parts)


def _time_text(value):
    return safe_text(value).replace("T", " ", 1)


def show_facts(facts, write):
    """Display exact values and provenance; units are never inferred from names."""
    for name, fact in sorted(facts.items()):
        value = fact.get("value")
        if name.startswith("document:"):
            lines = str(value).splitlines() if value is not None else []
            preview = next((line.strip() for line in lines if line.strip()), "본문 없음")
            shown = f"{safe_text(preview[:100])} · {len(str(value)) if value is not None else 0}자 (docs로 전문 보기)"
            title = f"문서 {safe_text(name.split(':', 1)[1])}"
        else:
            shown = "구조화된 값" if isinstance(value, (dict, list)) else value_text(value)
            if "unit" in fact:
                shown += " " + safe_text(fact["unit"])
            elif isinstance(value, (int, float)) and not isinstance(value, bool):
                shown += " (단위 미제공)"
            title = label(name)
        status = label(fact.get("status", "UNKNOWN"), STATUS_LABELS)
        confirmation = " · " + label(fact["confirmation_status"], STATUS_LABELS) if "confirmation_status" in fact else ""
        write(f"  {title}: {shown} │ {status}{confirmation}")
        if not name.startswith("document:") and isinstance(value, (dict, list)):
            _show_value("값", value, write, indent="    ")
        dependencies = fact.get("dependencies", [fact.get("source_ref", {})])
        write("    출처: " + "; ".join(source_text(ref) for ref in dependencies))
        write("    기록/측정: " + _time_text(fact.get("source_time", "미제공")) +
              " │ CHAIN 인지: " + _time_text(fact.get("known_at", "미제공")))


def _show_value(name, value, write, *, indent="  "):
    if isinstance(value, dict):
        write(indent + label(name) + ":")
        for key, child in value.items():
            _show_value(key, child, write, indent=indent + "  ")
    elif isinstance(value, list):
        write(indent + label(name) + (":" if value else ": 없음"))
        for item in value:
            if isinstance(item, (dict, list)):
                _show_value("항목", item, write, indent=indent + "  ")
            else:
                write(indent + "  · " + value_text(item))
    else:
        write(indent + label(name) + ": " + value_text(value))


def show_agent_result(output, write):
    write("[Agent 결과] " + safe_text(output.get("agent_id", "?")) +
          " │ 요청 " + safe_text(output.get("request_id", "?")))
    result = output.get("result", {})
    for key, value in result.items():
        if key in {"structured_context", "evidence_package"} and isinstance(value, dict):
            write("  " + ("요청 범위의 구조화 항목" if key == "structured_context" else "근거 항목") +
                  ": " + ", ".join(label(name) for name in sorted(value)))
            missing = [(name, fact.get("status", "UNKNOWN")) for name, fact in value.items()
                       if isinstance(fact, dict) and fact.get("status") not in {"AVAILABLE", "COMPLETED"}]
            if missing:
                write("  미확보/확인 필요: " + ", ".join(label(name) + " · " + label(status, STATUS_LABELS)
                                                        for name, status in missing))
        elif key == "missing_information" and isinstance(value, list):
            write("  미확보 항목: " + (", ".join(label(name) for name in value) if value else "없음 (Agent가 반환한 목록)"))
        elif key == "cache" and isinstance(value, dict):
            if "hit" in value:
                write("  캐시 적중 여부(cache.hit): " + value_text(value["hit"]) + " (Agent 응답)")
            if "computed_at" in value:
                write("  계산 시각: " + _time_text(value["computed_at"]))
        else:
            _show_value(key, value, write)
    write("  입력 Snapshot: " + safe_text(output.get("input_snapshot_id", "미제공")))


def show_evidence(record, write):
    request = record["request"]
    write("\n" + "─" * 64)
    write("SYNTHETIC / MOCK — 고정된 HITL 근거를 확인합니다.")
    # This stable prefix is also used by actual-stdin regression probes.
    write(f"{record['state']} | {request['checkpoint']} | Request ID: {request['request_id']}")
    write("Evidence Snapshot: " + request["evidence_snapshot_id"])
    write("[이 요청에 고정된 판단 근거 — 이후 실시간 업데이트와 별도로 유지됩니다]")
    show_facts(record["snapshot"]["facts"], write)
    if "source_documents" in record:
        entries = record["source_documents"]["entries"]
        counts = {status:sum(entry["status"] == status for entry in entries)
                  for status in ("AVAILABLE", "NO_DOCUMENT", "UNAVAILABLE")}
        write("[출처 원문 열람자료 — 판단 근거와 구분] " +
              f"문서 {counts['AVAILABLE']}건 │ 문서가 없는 출처 {counts['NO_DOCUMENT']}건 │ 원문 미확보 {counts['UNAVAILABLE']}건")
    write("─" * 64)
    for output in record["results"]:
        show_agent_result(output, write)
    write("질문: " + safe_text(request["question"]))
    write("명령: docs = 고정 출처 원문 │ json = 판단 근거·출처 원문 전체 │ q = 입력 중단")


def show_documents(record, write):
    """Render only the fixed source bundle, with a legacy Snapshot fallback."""
    if "source_documents" in record:
        bundle = record["source_documents"]
        write("[출처 원문 열람자료 — 판단 근거와 구분]")
        write("자료 ID: " + safe_text(bundle["bundle_id"]))
        if "frozen_at" in bundle:write("고정 시각: " + _time_text(bundle["frozen_at"]))
        shown = set()
        if not bundle["entries"]:
            write("이 요청의 근거가 참조한 외부 출처 원문이 없습니다.")
        for entry in bundle["entries"]:
            ref = entry["source_ref"]
            key = (ref["system"], ref["record_id"], ref["version"])
            if key in shown:continue
            shown.add(key)
            if entry["status"] == "AVAILABLE":
                _show_document("출처 원문", "document:" + ref["record_id"], entry["document"], write)
            else:
                description = "문서가 없는 출처" if entry["status"] == "NO_DOCUMENT" else "원문 미확보"
                write("\n[" + description + "] " + source_text(ref))
                write("상태: " + entry["status"] + " │ 사유: " + safe_text(entry["reason"]))
            write("참조된 근거 항목: " + ", ".join(label(name) for name in entry["referenced_fields"]))
        write("json의 snapshot은 판단 근거, source_documents는 고정된 출처 원문 열람자료입니다.")
        return
    documents = [(name, fact) for name, fact in sorted(record["snapshot"]["facts"].items())
                 if name.startswith("document:")]
    write("[구형 요청 — 판단 근거 Snapshot에 포함된 문서만 표시]")
    if not documents:
        write("판단 근거 Snapshot은 있습니다. 해당 요청 범위에는 문서 전문이 포함되지 않았습니다.")
    for name, fact in documents:
        _show_document("고정 문서 전문", name, fact, write)
    write("json으로 이 요청의 판단 근거 원문을 확인할 수 있습니다.")


def _show_document(title, name, fact, write):
    write("\n[" + title + "] " + safe_text(name))
    write("상태: " + label(fact.get("status", "UNKNOWN"), STATUS_LABELS) +
          " │ 출처: " + source_text(fact.get("source_ref", {})))
    write("기록/측정: " + _time_text(fact.get("source_time", "미제공")) +
          " │ CHAIN 인지: " + _time_text(fact.get("known_at", "미제공")))
    value = fact.get("value")
    if isinstance(value, (dict, list)):
        _show_value("문서", value, write)
    else:
        write(safe_text(value_text(value) if value is None else value, multiline=True))


def duration(seconds):
    total = max(0, math.ceil(float(seconds)))
    hours, rest = divmod(total, 3600)
    minutes, seconds = divmod(rest, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}" if hours else f"{minutes:02d}:{seconds:02d}"


class DemoPresenter:
    """Observe runtime snapshots without acquiring input or advancing time."""
    def __init__(self, write=print):
        self.write = write
        self._audit_offset = 0
        self._timers = {}

    def observe(self, snapshot, timers=None):
        audit = snapshot.get("audit", [])
        for event in audit[self._audit_offset:]:
            self._show_event(event, snapshot)
        self._audit_offset = len(audit)
        active = set()
        for timer in timers or []:
            active.add(timer["id"])
            self._show_timer(timer)
        # Absence does not prove firing; the runtime owns that distinction.
        for timer_id in set(self._timers) - active:
            self.write("TIMER_ENDED | " + safe_text(timer_id) + " | 관측 목록에서 종료/해제됨")
            del self._timers[timer_id]

    def _show_event(self, event, snapshot):
        kind = event.get("type")
        descriptions = {
            "STATE_ENTERED": "상태 진입", "CONTEXT_APPLIED": "새 원천자료 반영",
            "HITL_REQUESTED": "고정 근거로 의료진 결정 요청", "HITL_INVALIDATED": "참조 근거 변경으로 요청 무효화",
            "AGENT_EXECUTION_REQUESTED": "Agent 실행 요청", "AGENT_RESULT_ACCEPTED": "Agent 결과 수락",
            "HITL_DECISION_RECORDED": "의료진 응답 수락", "HITL_REJECTED": "의료진 응답 거절",
            "HITL_RESUME_ACCEPTED": "의료진 재요청 수락 · 근거 준비 시작",
            "HITL_RESUME_OPENED": "같은 단계에 새 의료진 질문 생성",
            "HITL_RESUME_REJECTED": "의료진 재요청 거절",
            "HITL_RESUME_FAILED": "재요청 근거 준비 실패",
            "AGENT_FAILED_OR_REJECTED": "Agent 오류/결과 거절", "CONTEXT_RESOLUTION_FAILED": "자료 조회/반영 실패",
            "STATE_TIMEOUT": "상태 대기시간 경과 · 자동 결정 없음",
            "STRUCTURED_CONTEXT_SERVED": "요청 범위의 구조화 Context 반환",
            "NOTIFICATION_FAILED_OR_REJECTED": "Mock 알림 기록 실패",
        }
        if kind not in descriptions:
            return
        identifier = event.get("event_id", event.get("request_id", event.get("state", "?")))
        details = descriptions[kind] + " · " + _time_text(event.get("time", ""))
        if kind == "STATE_ENTERED":
            details += " · 현재 " + safe_text(event.get("state"))
        elif kind == "CONTEXT_APPLIED":
            details += " · Context v" + safe_text(event.get("context_version"))
            if "source_ref" in event:
                details += " · " + source_text(event["source_ref"])
        elif kind.startswith("AGENT_"):
            details += " · " + safe_text(event.get("alias", "?")) + " / " + safe_text(event.get("mode", "?"))
        if event.get("decision"):
            details += " · " + label(event["decision"], OPTION_LABELS)
        if event.get("reason"):
            details += " · " + safe_text(event["reason"])
        self.write(f"AUDIT | {kind} | {safe_text(identifier)} | {details}")
        if kind == "AGENT_RESULT_ACCEPTED":
            record = snapshot.get("agent_outputs", {}).get(event.get("request_id"))
            if record:
                show_agent_result(record["output"], self.write)

    def _show_timer(self, timer):
        timer_id = timer["id"]
        seconds = max(0, float(timer["seconds"]))
        status = timer.get("status", "WAITING")
        last = self._timers.get(timer_id)
        clock = CLOCK_LABELS.get(timer["clock_kind"], safe_text(timer["clock_kind"]))
        if timer["remaining"] is None:
            if last and last["status"] == status and last["remaining"] is None:
                return
            self._timers[timer_id] = {"remaining": None, "status": status}
            self.write("TIMER | " + safe_text(timer_id) + " | ⏱ " + safe_text(timer["label"]) +
                       f" · {clock} · 현재 상태 확인 지연 · 남은 시간 미확인")
            return
        remaining = max(0, float(timer["remaining"]))
        threshold = 1 if seconds <= 30 else 5
        if last and last["status"] == status and ((last["remaining"] == remaining) or
                (remaining > 0 and last["remaining"] is not None and abs(last["remaining"] - remaining) < threshold)):
            return
        self._timers[timer_id] = {"remaining": remaining, "status": status}
        elapsed = 1 - min(1, remaining / seconds) if seconds else 1
        filled = min(12, max(0, int(elapsed * 12)))
        bar = "■" * filled + "□" * (12 - filled)
        message = ("만료 처리 대기 · 발생 확인 전" if remaining == 0 and status in {"WAITING", "OVERDUE"}
                   else {"FIRED": "타이머 발생 확인", "CANCELED": "취소 확인"}.get(status, "대기 중"))
        extra = ""
        if "original_seconds" in timer and float(timer["original_seconds"]) != seconds:
            extra = " · 원래 설정 " + duration(timer["original_seconds"])
        self.write("TIMER | " + safe_text(timer_id) + " | ⏱ " + safe_text(timer["label"]) +
                   f" · {clock} [{bar}] 남은 {duration(remaining)} / 전체 {duration(seconds)} · {message}{extra}")
