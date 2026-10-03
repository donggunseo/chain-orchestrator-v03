"""Pure request preparation; no plugin loading, fixture I/O, or State branches."""
import copy

from .contracts import make_snapshot, validate_agent_request, validate_snapshot
from .validation import canonical_hash, strings, text, timestamp


def scope_snapshot(snapshot, scope):
    validate_snapshot(snapshot)
    names = sorted(strings(scope, "scope", nonempty=True))
    facts = snapshot["facts"]
    earliest = min((f["known_at"] for f in facts.values()), key=timestamp, default=snapshot["known_at"])
    anchor = facts.get("arrival_time", {}).get("known_at", earliest)
    episode = str(facts.get("episode_id", {}).get("value", snapshot["snapshot_id"]))
    selected = {}
    for name in names:
        selected[name] = copy.deepcopy(facts.get(name, {
            "value": None, "status": "UNKNOWN", "source_ref": {
                "system": "CHAIN_MISSING", "record_id": episode, "version": 1, "field": name},
            "source_time": anchor, "known_at": anchor}))
    # Unrelated Context additions must not alter a scoped input commitment.
    known_at = max((f["known_at"] for f in selected.values()), key=timestamp)
    return make_snapshot(selected, known_at)


def input_commitment(snapshot, scope, mode):
    return canonical_hash({"facts": snapshot["facts"], "scope": sorted(scope), "mode": mode})


def make_job(bundle, alias, snapshot, scope, *, request_id, mode, evaluated_at):
    spec = bundle["catalog"]["agents"][alias]
    manifest = bundle["registry"]["implementations"][spec["implementation"]]
    return make_scoped_job(bundle["bundle_hash"], alias, spec, manifest["manifest_hash"], snapshot, scope,
                           request_id=request_id, mode=mode, evaluated_at=evaluated_at)


def make_scoped_job(catalog_hash, alias, spec, manifest_hash, snapshot, scope, *, request_id, mode, evaluated_at):
    """Public metadata only; Engine never receives backend or installation paths."""
    validate_snapshot(snapshot)
    if timestamp(evaluated_at) < timestamp(snapshot["known_at"]):
        raise ValueError("Agent evaluation precedes current Context")
    if mode not in spec["modes"]:
        raise ValueError("Unsupported Agent mode")
    scoped = scope_snapshot(snapshot, scope)
    refs = {}
    for fact in scoped["facts"].values():
        for ref in fact.get("dependencies", [fact["source_ref"]]):
            refs[canonical_hash(ref)] = copy.deepcopy(ref)
    request = {"contract_schema": "chain-agent-request/v0.3", "request_id": text(request_id),
               "agent_id": spec["agent_id"], "agent_version": spec["version"],
               "manifest_hash": manifest_hash, "input_snapshot_id": scoped["snapshot_id"],
               "mode": mode, "scope": sorted(scope), "dependencies": [refs[k] for k in sorted(refs)],
               "evaluated_at": evaluated_at}
    validate_agent_request(request, scoped)
    return {"catalog_hash": catalog_hash, "agent": alias, "request": request, "snapshot": scoped}
