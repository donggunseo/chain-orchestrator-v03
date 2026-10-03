"""Small, closed expression language. Never eval() YAML or clinical text."""
from __future__ import annotations
from typing import Any

MISSING = object()
OPERATORS = {"all", "any", "eq", "ne", "in", "gte", "exists", "contains_all"}


def lookup(value: Any, path: str, default: Any = MISSING) -> Any:
    for key in path.split("."):
        if isinstance(value, dict) and key in value:
            value = value[key]
        elif key == "length" and isinstance(value, (list, dict, str)):
            value = len(value)
        else:
            return default
    return value


def resolve(value: Any, env: dict) -> Any:
    if isinstance(value, str) and value.startswith("$"):
        return lookup(env, value[1:])
    return value


def validate_expression(expr: Any) -> None:
    if not isinstance(expr, dict) or len(expr) != 1:
        raise ValueError(f"Guard must be a one-operator mapping, not prose: {expr!r}")
    op, values = next(iter(expr.items()))
    if op not in OPERATORS:
        raise ValueError(f"Unknown guard operator: {op}")
    if op in ("all", "any"):
        if not isinstance(values, list) or not values:
            raise ValueError(f"{op} requires nonempty expressions")
        for item in values:
            validate_expression(item)
    elif op == "exists":
        if not isinstance(values, str) or not values.startswith("$"):
            raise ValueError("exists requires a $path")
    elif not isinstance(values, list) or len(values) != 2:
        raise ValueError(f"{op} requires two operands")


def evaluate(expr: dict, env: dict) -> bool:
    validate_expression(expr)
    op, values = next(iter(expr.items()))
    if op in ("all", "any"):
        results = [evaluate(v, env) for v in values]
        return all(results) if op == "all" else any(results)
    if op == "exists":
        value = resolve(values, env)
        return value is not MISSING and value is not None
    a, b = [resolve(v, env) for v in values]
    # Missing fields must never satisfy even a != comparison.
    if a is MISSING or b is MISSING:
        return False
    try:
        if op == "eq":
            return type(a) is type(b) and a == b
        if op == "ne":
            return type(a) is type(b) and a != b
        if op == "gte":
            return type(a) in (int, float) and type(b) in (int, float) and a >= b
        if op == "in":
            return isinstance(b, list) and a in b
        if op == "contains_all":
            return isinstance(a, list) and isinstance(b, list) and all(x in a for x in b)
    except (TypeError, ValueError):
        return False
    return False


def matches(pattern: dict, event: dict) -> bool:
    for path, expected in pattern.items():
        actual = lookup(event, path)
        if actual is MISSING:
            return False
        if isinstance(expected, list):
            if actual not in expected:
                return False
        elif type(actual) is not type(expected) or actual != expected:
            return False
    return True
