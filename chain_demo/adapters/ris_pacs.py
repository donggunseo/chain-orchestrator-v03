from .base import MockSourceAdapter


class RISPACSAdapter(MockSourceAdapter):
    systems = frozenset({"RIS_PACS"})
