from .base import MockSourceAdapter


class EMRAdapter(MockSourceAdapter):
    systems = frozenset({"ER_EMR"})
