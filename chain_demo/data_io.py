"""Source/initial JSON reads outside Workflow; never preload an episode directory."""
import json
from pathlib import Path

from .source_contracts import validate_initial


def _unique_pairs(pairs):
    obj = {}
    for key, value in pairs:
        if key in obj:
            raise ValueError(f"Duplicate JSON key: {key}")
        obj[key] = value
    return obj


def read_json(path):
    return json.loads(Path(path).read_text(encoding="utf8"), object_pairs_hook=_unique_pairs)


def load_initial(path):
    """Read only the requested initial file, with a closed eight-field schema."""
    obj = read_json(path)
    validate_initial(obj)
    return obj
