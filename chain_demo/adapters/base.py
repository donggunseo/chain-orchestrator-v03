"""Mock publication boundary: read source bytes at release, then publish, then emit."""
import copy
from pathlib import Path
from threading import RLock

from ..contracts import validate_event
from ..data_io import read_json
from ..source_contracts import identity, validate_record
from ..validation import canonical_hash, text, timestamp
from .store import NotPublishedError


class MockSourceAdapter:
    systems = frozenset()

    def __init__(self, source_root, store):
        self._root = Path(source_root).resolve()
        self._store = store
        self._outbox = {}
        self._lock = RLock()

    def publish(self, path, *, not_before, at, event_id, emit=None):
        text(event_id)
        if timestamp(at) < timestamp(not_before):
            raise NotPublishedError("Publication release has not occurred; source was not opened")
        relative = Path(path)
        if relative.is_absolute() or ".." in relative.parts or relative.suffix != ".json":
            raise ValueError("Source path must be relative JSON inside source root")
        actual = (self._root / relative).resolve()
        if not actual.is_relative_to(self._root):
            raise ValueError("Source path escapes root")
        release = (str(relative), not_before)
        with self._lock:
            cached = self._outbox.get(event_id)
            if cached:
                if cached[0] != release:
                    raise ValueError("Event ID reused for a different publication")
                event = copy.deepcopy(cached[1])
                if timestamp(at) < timestamp(event["emitted_time"]):
                    raise NotPublishedError("Retry cannot precede original publication/emission")
            else:
                record = read_json(actual)
                validate_record(record)
                if record["source_ref"]["system"] not in self.systems:
                    raise ValueError("Wrong source system for this Adapter")
                entry = self._store.publish(record, at=at)
                payload = {"data_ref": copy.deepcopy(record["source_ref"]), "content_hash": record["content_hash"]}
                event = {"contract_schema": "chain-event/v0.3", **identity(record),
                         "event_id": event_id, "event_type": record["event_type"],
                         "source_ref": copy.deepcopy(record["source_ref"]), "sequence_no": 1,
                         "source_event_time": record["source_time"], "published_at": entry["published_at"],
                         "emitted_time": at, "received_at": at, "payload": payload,
                         "payload_hash": canonical_hash(payload)}
                if "source_sequence_no" in record:
                    event["source_sequence_no"] = record["source_sequence_no"]
                validate_event(event)
                self._outbox[event_id] = (release, copy.deepcopy(event))
        # A transport failure leaves published bytes and an identical retry Event.
        if emit is not None:
            emit(copy.deepcopy(event))
        return event
