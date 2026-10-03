"""Synthetic output-file backend; no clinical classifier or NLP implementation."""
from ..validation import fields, strings
from .services import FixtureError


def invoke(request, snapshot, services):
    if request["mode"] != "screening":
        raise FixtureError("AGENT_UNSUPPORTED_MODE")
    output = services.backend.select(request, snapshot)
    fields(output, {"screening_result", "mock_only", "basis"}, where="Screening fixture")
    if output["screening_result"] not in {"POSITIVE", "NEGATIVE"} or output["mock_only"] is not True:
        raise FixtureError("FIXTURE_INVALID_OUTPUT")
    strings(output["basis"], nonempty=True)
    return output
