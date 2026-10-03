"""Generic registered-module dispatch at Worker/Activity boundaries only."""
import copy
import importlib
import hashlib
from pathlib import Path

from ..agent_config import validate_agent_bundle
from ..config import ROOT
from ..contracts import validate_agent_request, validate_agent_result
from ..registry import verify_installation
from ..validation import fields
from .fixture_backend import FixtureBackend
from .services import AgentServices, FixtureError, SummaryCache


class AgentRuntime:
    def __init__(self, bundle, *, root=ROOT):
        self.bundle = copy.deepcopy(bundle)
        self.root = Path(root).resolve()
        validate_agent_bundle(self.bundle)
        verify_installation(self.bundle["registry"], self.root)
        self.cache = SummaryCache()

    def _verify_code(self):
        for name, checksum in self.bundle["run_manifest"]["implementation"]["files"].items():
            path = (self.root / name).resolve()
            if not path.is_relative_to(self.root) or not path.is_file():
                raise ValueError("Pinned implementation file missing/outside root")
            if "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest() != checksum:
                raise ValueError("Pinned implementation file changed")

    def invoke(self, job):
        fields(job, {"catalog_hash", "agent", "request", "snapshot"}, where="Agent job")
        if job["catalog_hash"] != self.bundle["bundle_hash"]:
            raise ValueError("Agent job does not match pinned bundle")
        request, snapshot = copy.deepcopy(job["request"]), copy.deepcopy(job["snapshot"])
        validate_agent_request(request, snapshot)
        if "evaluated_at" not in request or set(snapshot["facts"]) != set(request["scope"]):
            raise ValueError("Worker accepts only scoped inputs with explicit evaluation time")
        spec = self.bundle["catalog"]["agents"].get(job["agent"])
        if not spec:
            raise ValueError("Unknown registered Agent")
        manifest = self.bundle["registry"]["implementations"][spec["implementation"]]
        if (request["agent_id"] != spec["agent_id"] or request["agent_version"] != spec["version"]
                or request["manifest_hash"] != manifest["manifest_hash"] or request["mode"] not in spec["modes"]):
            raise ValueError("Request identity/version/mode does not match registration")
        validate_agent_bundle(self.bundle)
        self._verify_code()
        verify_installation(self.bundle["registry"], self.root)
        module_name, function_name = manifest["entrypoint"].split(":")
        module = importlib.import_module(module_name)
        if Path(module.__file__).resolve() != self.root / (module_name.replace(".", "/") + ".py"):
            raise ValueError("Imported module differs from installed manifest")
        entrypoint = getattr(module, function_name)
        if not callable(entrypoint):
            raise ValueError("Registered entrypoint is not callable")
        backend = spec["backend"]
        result = {"contract_schema": "chain-agent-result/v0.3",
                  **{k:request[k] for k in ("request_id", "agent_id", "agent_version", "manifest_hash", "input_snapshot_id")},
                  "status": "SUCCESS", "result": None, "evidence": copy.deepcopy(request["dependencies"]),
                  "produced_time": request["evaluated_at"]}
        try:
            services = AgentServices(self.cache, FixtureBackend(self.root, backend["manifest"], allowed_files=manifest["files"])
                                     if backend["kind"] == "fixture" else None)
            result["result"] = entrypoint(request, snapshot, services)
        except (FixtureError, ValueError) as exc:
            result.update(status="FAILED", result=None, evidence=[],
                          error=exc.code if isinstance(exc, FixtureError) else "AGENT_INVALID_OUTPUT")
        validate_agent_result(result, request)
        return result
