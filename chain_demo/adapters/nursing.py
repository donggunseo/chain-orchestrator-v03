from .base import MockSourceAdapter


class NursingAdapter(MockSourceAdapter):
    systems = frozenset({"NURSING_EMR"})
