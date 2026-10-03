"""Pure concrete field dependency closure and same-field version lineage."""
import copy

from .validation import canonical_hash


class EvidenceIndex:
    def __init__(self,facts):
        self.nodes={canonical_hash(f["source_ref"]):copy.deepcopy(f) for f in facts.values()}

    def contains(self,ref):
        return canonical_hash(ref) in self.nodes

    def lineage(self,target):
        if not self.contains(target):raise ValueError("Revision target field/version was never known")
        return [copy.deepcopy(f["source_ref"]) for _,f in sorted(self.nodes.items())
                if all(f["source_ref"][k]==target[k] for k in ("system","record_id","field"))
                and f["source_ref"]["version"]<=target["version"]]

    def prepare(self,facts):
        """Validate and flatten in a temporary graph; failed input has no effects."""
        prepared=copy.deepcopy(facts)
        nodes={**self.nodes,**{canonical_hash(f["source_ref"]):f for f in prepared.values()}}
        def expand(ref,path):
            key=canonical_hash(ref)
            if key in path:raise ValueError("Cyclic source dependencies")
            if key not in nodes:raise ValueError("Source dependency was not yet known")
            fact=nodes[key];found={key:copy.deepcopy(ref)}
            for dependency in fact.get("dependencies",[]):
                if dependency!=ref:found.update(expand(dependency,path|{key}))
            return found
        for fact in prepared.values():
            if "dependencies" in fact:
                refs=expand(fact["source_ref"],set())
                fact["dependencies"]=[refs[k] for k in sorted(refs)]
        return prepared

    def commit(self,facts):
        self.nodes.update({canonical_hash(f["source_ref"]):copy.deepcopy(f) for f in facts.values()})
