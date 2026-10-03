from .base import MockSourceAdapter


class ManualAdapter(MockSourceAdapter):
    systems = frozenset({"MANUAL_INPUT"})
