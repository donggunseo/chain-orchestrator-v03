"""Requested structured Context only; no NLP or State-specific summary prose."""
import copy

from ..validation import canonical_hash
from .services import FixtureError


def invoke(request, snapshot, services):
    if request["mode"] != "context":
        raise FixtureError("AGENT_UNSUPPORTED_MODE")
    key = canonical_hash({"snapshot": snapshot["snapshot_id"], "scope": sorted(request["scope"]),
                          "agent_id": request["agent_id"], "version": request["agent_version"],
                          "manifest": request["manifest_hash"], "mode": request["mode"]})
    def build():
        facts = copy.deepcopy(snapshot["facts"])
        return {"structured_context": facts,
                "missing_information": sorted(k for k,f in facts.items() if f["value"] is None
                                              or f["status"] in {"UNKNOWN", "PENDING", "CONFLICT", "UNAVAILABLE"}),
                "computed_at": request["evaluated_at"]}
    payload, hit = services.cache.get_or_create(key, build)
    computed = payload.pop("computed_at")
    payload["cache"] = {"key": key, "hit": hit, "computed_at": computed}
    return payload
