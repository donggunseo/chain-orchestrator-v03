"""Read-only timer views for the demo; never drive Workflow time or decisions."""
from datetime import timedelta

from temporalio.api.enums.v1 import EventType


def temporal_timer_views(history, *, now, timer_scale=1.0, verified=True):
    """Use actual History starts/completions, not clinical or test-clock guesses."""
    active = {}
    closed = {
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT,
        EventType.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
    }
    for event in history.events:
        if event.event_type == EventType.EVENT_TYPE_TIMER_STARTED:
            attributes = event.timer_started_event_attributes
            duration = attributes.start_to_fire_timeout.ToTimedelta().total_seconds()
            started = event.event_time.ToDatetime(tzinfo=now.tzinfo)
            active[attributes.timer_id] = (started + timedelta(seconds=duration), duration)
        elif event.event_type == EventType.EVENT_TYPE_TIMER_FIRED:
            active.pop(event.timer_fired_event_attributes.timer_id, None)
        elif event.event_type == EventType.EVENT_TYPE_TIMER_CANCELED:
            active.pop(event.timer_canceled_event_attributes.timer_id, None)
        elif event.event_type in closed:
            active.clear()
    result = []
    for identifier, (deadline, duration) in active.items():
        remaining = max(0.0, (deadline - now).total_seconds())
        result.append({
            "id": identifier,
            "label": "Temporal Workflow 타이머",
            "seconds": duration,
            "original_seconds": duration / timer_scale,
            "remaining": remaining if verified else None,
            "clock_kind": "TEMPORAL_DURABLE",
            "status": "UNVERIFIED" if not verified else "OVERDUE" if remaining == 0 else "WAITING",
        })
    return result
