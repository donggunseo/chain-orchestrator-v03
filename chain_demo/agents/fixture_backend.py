"""Exact input/time match; source templates and future scenario files are never read."""
import copy
from pathlib import Path

from ..agent_requests import input_commitment
from ..data_io import read_json
from ..validation import canonical_hash, fields, hash_value, text, timestamp
from .services import FixtureError


class FixtureBackend:
    def __init__(self, root, manifest_path, *, allowed_files=None):
        self.root = Path(root).resolve()
        self.allowed_files = None if allowed_files is None else frozenset(allowed_files)
        self.path = self._path(manifest_path, self.root)

    def _path(self, name, parent):
        path = Path(text(name))
        resolved = (parent / path).resolve()
        if path.is_absolute() or ".." in path.parts or not resolved.is_relative_to(self.root):
            raise FixtureError("FIXTURE_INVALID_PATH")
        if self.allowed_files is not None and str(resolved.relative_to(self.root)) not in self.allowed_files:
            raise FixtureError("FIXTURE_NOT_INSTALLED")
        return resolved

    def select(self, request, snapshot):
        try:
            manifest = read_json(self.path)
            fields(manifest, {"fixture_schema", "synthetic", "entries"}, where="fixture manifest")
            if manifest["fixture_schema"] != "chain-fixtures/v0.3" or manifest["synthetic"] is not True:
                raise ValueError("Only explicit synthetic fixtures supported")
            if not isinstance(manifest["entries"], list):
                raise ValueError("Fixture entries must be a list")
            ids, matches = set(), []
            commitment = input_commitment(snapshot, request["scope"], request["mode"])
            for entry in manifest["entries"]:
                fields(entry, {"fixture_id", "agent_id", "agent_version", "mode", "input_hash", "evaluation_time",
                               "output", "output_hash"}, where="fixture entry")
                for key in ("fixture_id", "agent_id", "agent_version", "mode", "output"):
                    text(entry[key])
                hash_value(entry["input_hash"]); hash_value(entry["output_hash"])
                timestamp(entry["evaluation_time"])
                if entry["fixture_id"] in ids:
                    raise ValueError("Duplicate fixture ID")
                ids.add(entry["fixture_id"])
                if (entry["agent_id"] == request["agent_id"] and entry["agent_version"] == request["agent_version"]
                        and entry["mode"] == request["mode"] and entry["input_hash"] == commitment
                        and timestamp(entry["evaluation_time"]) == timestamp(request["evaluated_at"])):
                    matches.append(entry)
            if not matches:
                raise FixtureError("FIXTURE_NOT_FOUND")
            if len(matches) != 1:
                raise FixtureError("FIXTURE_AMBIGUOUS")
            entry = matches[0]
            output = read_json(self._path(entry["output"], self.path.parent))
            if canonical_hash(output) != entry["output_hash"]:
                raise ValueError("Fixture output hash mismatch")
            return copy.deepcopy(output)
        except FixtureError:
            raise
        except (ValueError, OSError, KeyError, TypeError) as exc:
            raise FixtureError("FIXTURE_INVALID") from exc
