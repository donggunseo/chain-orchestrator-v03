from .base import MockSourceAdapter


class OCSAdapter(MockSourceAdapter):
    systems = frozenset({"OCS"})
