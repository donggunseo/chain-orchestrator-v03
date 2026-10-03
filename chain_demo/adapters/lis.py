from .base import MockSourceAdapter


class LISAdapter(MockSourceAdapter):
    systems = frozenset({"LIS"})
