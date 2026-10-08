"""Pure exact-version source text for HITL viewing, separate from decision input.

This bundle never expands the Agent scope or HITL invalidation dependencies.
Only normalized records already admitted to the Context ledger are used.
"""
import copy

from .contracts import make_snapshot, source_ref, validate_snapshot
from .validation import canonical_hash, fields, hash_value, json_value, strings, text, timestamp


def _source_key(ref):
    return ref["system"], ref["record_id"], ref["version"]


def _groups(request, snapshot):
    validate_snapshot(snapshot)
    if not isinstance(request, dict) or not {"request_id", "evidence_snapshot_id", "dependencies"} <= request.keys():
        raise ValueError("Source documents require a HITL Request")
    text(request["request_id"])
    if request["evidence_snapshot_id"] != snapshot["snapshot_id"]:
        raise ValueError("Source documents require the fixed evidence Snapshot")
    refs = request["dependencies"]
    if not isinstance(refs, list):raise ValueError("HITL dependencies must be a list")
    permitted = {canonical_hash(ref) for fact in snapshot["facts"].values()
                 for ref in fact.get("dependencies", [fact["source_ref"]])}
    seen, grouped = set(), {}
    for ref in refs:
        source_ref(ref, field=True)
        key = canonical_hash(ref)
        if key in seen or key not in permitted:
            raise ValueError("Source document reference outside fixed HITL evidence")
        seen.add(key)
        grouped.setdefault(_source_key(ref), set()).add(ref["field"])
    return grouped


def freeze_source_documents(request, snapshot, source_records, *, frozen_at):
    """Select historical records by exact source identity and evidence cutoff.

    source_records maps (system, record_id, version) to facts and applied_at.
    A later record/version is never used as a substitute for an unavailable one.
    """
    grouped = _groups(request, snapshot)
    cutoff = timestamp(snapshot["known_at"])
    if timestamp(frozen_at) < cutoff:raise ValueError("Source documents precede their evidence Snapshot")
    entries = []
    for key, names in sorted(grouped.items()):
        ref = dict(zip(("system", "record_id", "version"), key))
        entry = {"source_ref": ref, "referenced_fields": sorted(names)}
        record = source_records.get(key)
        if record is None:
            entry.update(status="UNAVAILABLE", reason="SOURCE_NOT_RECORDED")
        elif timestamp(record["applied_at"]) > cutoff:
            entry.update(status="UNAVAILABLE", reason="DOCUMENT_NOT_KNOWN_AT_EVIDENCE_TIME")
        else:
            name = "document:" + ref["record_id"]
            document = record["facts"].get(name)
            if document is None:
                entry.update(status="NO_DOCUMENT", reason="NO_DOCUMENT")
            else:
                document = copy.deepcopy(document)
                # Completion receipt and Context admission can occur at different
                # times. The admitted time is the availability boundary here.
                document["known_at"] = record["applied_at"]
                if document["status"] != "AVAILABLE" or not isinstance(document["value"], str) or not document["value"].strip():
                    entry.update(status="UNAVAILABLE", reason="DOCUMENT_UNAVAILABLE")
                else:
                    entry.update(status="AVAILABLE", document=document)
        entries.append(entry)
    bundle = {"contract_schema": "chain-hitl-documents/v0.3", "request_id": request["request_id"],
        "evidence_snapshot_id": snapshot["snapshot_id"], "frozen_at": frozen_at,
        "purpose": "SOURCE_REFERENCE", "entries": entries}
    bundle["bundle_id"] = canonical_hash(bundle)
    validate_source_documents(bundle, request, snapshot)
    return bundle


def validate_source_documents(bundle, request, snapshot):
    """Validate identity, scope, time and content without extending clinical gates."""
    grouped = _groups(request, snapshot)
    json_value(bundle, "HITL source documents")
    fields(bundle, {"contract_schema", "request_id", "evidence_snapshot_id", "frozen_at", "purpose",
                    "entries", "bundle_id"}, where="HITL source documents")
    if bundle["contract_schema"] != "chain-hitl-documents/v0.3" or bundle["purpose"] != "SOURCE_REFERENCE":
        raise ValueError("Unsupported HITL source document contract/purpose")
    if bundle["request_id"] != request["request_id"] or bundle["evidence_snapshot_id"] != snapshot["snapshot_id"]:
        raise ValueError("Source documents belong to a different HITL Request/Snapshot")
    if timestamp(bundle["frozen_at"]) < timestamp(snapshot["known_at"]):
        raise ValueError("Source document freeze precedes the evidence Snapshot")
    if not isinstance(bundle["entries"], list):raise ValueError("Source document entries must be a list")
    seen = set()
    for entry in bundle["entries"]:
        fields(entry, {"source_ref", "referenced_fields", "status"}, {"document", "reason"}, "Source document entry")
        source_ref(entry["source_ref"])
        key = _source_key(entry["source_ref"])
        names = strings(entry["referenced_fields"], "referenced_fields", nonempty=True)
        if key in seen or key not in grouped or set(names) != grouped[key]:
            raise ValueError("Source document entry must match exact fixed HITL provenance")
        seen.add(key)
        if entry["status"] == "AVAILABLE":
            if "document" not in entry or "reason" in entry:raise ValueError("Available source document requires its text")
            document = entry["document"]
            name = "document:" + entry["source_ref"]["record_id"]
            make_snapshot({name: document}, snapshot["known_at"])
            if document["source_ref"] != {**entry["source_ref"], "field": name}:
                raise ValueError("Source document identity/version mismatch")
            if document["status"] != "AVAILABLE":raise ValueError("Source document text is not available")
            text(document["value"], "source document text")
            if timestamp(document["source_time"]) > timestamp(document["known_at"]):
                raise ValueError("Source document precedes publication of its text")
        else:
            if "document" in entry or "reason" not in entry:raise ValueError("Unavailable source document requires a reason, no replacement text")
            allowed = {"NO_DOCUMENT": {"NO_DOCUMENT"}, "UNAVAILABLE": {"SOURCE_NOT_RECORDED",
                "DOCUMENT_NOT_KNOWN_AT_EVIDENCE_TIME", "DOCUMENT_UNAVAILABLE"}}
            if entry["status"] not in allowed or entry["reason"] not in allowed[entry["status"]]:
                raise ValueError("Unknown source document status/reason")
    if seen != grouped.keys():raise ValueError("Source document bundle omits fixed HITL sources")
    hash_value(bundle["bundle_id"])
    if bundle["bundle_id"] != canonical_hash({k:v for k,v in bundle.items() if k != "bundle_id"}):
        raise ValueError("Source document bundle content hash mismatch")
