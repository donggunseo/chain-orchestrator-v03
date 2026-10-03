"""Common project paths, implementation version and strict YAML loading."""
from __future__ import annotations

from pathlib import Path
import yaml


ROOT = Path(__file__).resolve().parents[1]
IMPLEMENTATION_VERSION = "0.3-cleanup1"


class _UniqueKeyLoader(yaml.SafeLoader):
    """Safe YAML without silent duplicate-key overwrite or implicit merge behavior."""


def _unique_mapping(loader, node, deep=False):
    result = {}
    for key_node, value_node in node.value:
        if key_node.tag == "tag:yaml.org,2002:merge":
            raise ValueError("YAML merge keys are unsupported; declare fields explicitly")
        key = loader.construct_object(key_node, deep=True)
        if not isinstance(key, str):
            raise ValueError("YAML mapping keys must be strings")
        if key in result:
            raise ValueError(f"Duplicate YAML key: {key}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeyLoader.add_constructor("tag:yaml.org,2002:map", _unique_mapping)


def _read_yaml(path):
    return yaml.load(path.read_text(encoding="utf8"), Loader=_UniqueKeyLoader)
