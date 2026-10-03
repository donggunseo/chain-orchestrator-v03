"""Installed manifest checks outside Workflow; no dynamic plugin import in step 1."""
from __future__ import annotations

import ast
from pathlib import Path
import hashlib
import re

from .validation import boolean, canonical_hash, fields, hash_value, strings, text


def validate_registry(registry):
    fields(registry, {"registry_schema", "implementations"}, where="Registry")
    if registry["registry_schema"] != "chain-plugins/v1" or not isinstance(registry["implementations"], dict):
        raise ValueError("Unsupported Registry schema")
    for plugin_id, manifest in registry["implementations"].items():
        text(plugin_id)
        fields(manifest, {"version", "entrypoint", "contract", "installed", "status", "agent_id",
                          "agent_version", "data_scopes", "actions", "files", "manifest_hash"}, where="Plugin manifest")
        for key in ("version", "agent_id", "agent_version", "entrypoint", "contract", "status"):
            text(manifest[key], key)
        if re.fullmatch(r"[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*:[A-Za-z_]\w*", manifest["entrypoint"]) is None:
            raise ValueError("Plugin entrypoint must be module:function")
        if manifest["contract"] != "chain-agent/v0.3":
            raise ValueError("Unsupported Plugin contract")
        boolean(manifest["installed"])
        if manifest["status"] not in {"APPROVED", "PENDING", "REVOKED"}:
            raise ValueError("Unsupported Plugin approval status")
        strings(manifest["data_scopes"]); strings(manifest["actions"])
        if not isinstance(manifest["files"], dict) or not manifest["files"]:
            raise ValueError("Plugin manifest must identify installed files")
        for name, checksum in manifest["files"].items():
            text(name); hash_value(checksum)
            path = Path(name)
            if path.is_absolute() or ".." in path.parts:
                raise ValueError("Plugin file must stay inside installation root")
        hash_value(manifest["manifest_hash"])
        if manifest["manifest_hash"] != canonical_hash({k: v for k, v in manifest.items() if k != "manifest_hash"}):
            raise ValueError("Plugin manifest hash mismatch")


def validate_bindings(workflow, runtime, registry, *, contract="chain-agent/v0.3"):
    validate_registry(registry)
    for alias, spec in workflow["agents"].items():
        manifest = registry["implementations"].get(spec["implementation"])
        if not manifest or not manifest["installed"]:
            raise ValueError(f"{alias}: implementation not installed")
        if manifest["status"] != "APPROVED":
            raise ValueError(f"{alias}: implementation not approved")
        if manifest["agent_id"] != spec["agent_id"] or manifest["agent_version"] != spec["version"]:
            raise ValueError(f"{alias}: implementation Agent identity/version mismatch")
        approval = runtime["approved_agents"].get(spec["agent_id"])
        if not approval or spec["version"] not in approval["versions"]:
            raise ValueError(f"{alias}: Policy does not approve Agent version")
        if not set(spec["data_scopes"]).issubset(set(manifest["data_scopes"]) & set(approval["data_scopes"])):
            raise ValueError(f"{alias}: data scope exceeds manifest/Policy")
        if not set(spec["actions"]).issubset(set(manifest["actions"]) & set(runtime["allowed_actions_for_agents"])):
            raise ValueError(f"{alias}: actions exceed manifest/Policy")
        if set(spec["actions"]) & set(runtime["forbidden_actions_for_agents"]):
            raise ValueError(f"{alias}: forbidden actions")
        if manifest["contract"] != contract:
            raise ValueError(f"{alias}: unsupported Agent contract for this runtime")


def verify_installation(registry, root):
    """Check declared bytes and a function declaration; never execute installed code."""
    validate_registry(registry)
    root = Path(root).resolve()
    for plugin_id, manifest in registry["implementations"].items():
        if not manifest["installed"]:
            continue
        for name, checksum in manifest["files"].items():
            path = (root / name).resolve()
            if not path.is_relative_to(root) or not path.is_file():
                raise ValueError(f"{plugin_id}: installed file missing or outside root: {name}")
            actual = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
            if actual != checksum:
                raise ValueError(f"{plugin_id}: installed file hash mismatch: {name}")
        module, function = manifest["entrypoint"].split(":")
        name = module.replace(".", "/") + ".py"
        if name not in manifest["files"]:
            raise ValueError(f"{plugin_id}: entrypoint file absent from manifest")
        tree = ast.parse((root / name).read_text(encoding="utf8"))
        if not any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) and node.name == function for node in tree.body):
            raise ValueError(f"{plugin_id}: entrypoint function not declared")
