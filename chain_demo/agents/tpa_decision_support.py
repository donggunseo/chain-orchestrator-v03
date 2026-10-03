"""Evidence-only virtual results; SUCCESS never means clinical eligibility."""
from ..validation import fields
from .services import FixtureError


def invoke(request, snapshot, services):
    if request["mode"] not in {"interim", "final"}:
        raise FixtureError("AGENT_UNSUPPORTED_MODE")
    output = services.backend.select(request, snapshot)
    fields(output, {"mode", "evidence_package", "assessment", "mock_only"}, where="tPA fixture")
    missing = any(f["value"] is None or f["status"] in {"UNKNOWN", "PENDING", "CONFLICT", "UNAVAILABLE"}
                  for f in snapshot["facts"].values())
    expected = "PENDING_MOCK" if missing else "STRUCTURED_MOCK_ONLY"
    if (output["mock_only"] is not True or output["mode"] != request["mode"]
            or output["evidence_package"] != snapshot["facts"] or output["assessment"] != expected):
        raise FixtureError("FIXTURE_INVALID_OUTPUT")
    return output
