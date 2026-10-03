"""Worker-owned v0.3 Agent catalog, executable Policy and installed Registry."""
import copy
import hashlib
from pathlib import Path
from importlib.metadata import PackageNotFoundError, version

from .config import ROOT, IMPLEMENTATION_VERSION, _read_yaml
from .registry import validate_bindings, verify_installation
from .validation import canonical_hash, fields, strings, text


def validate_agent_bundle(bundle):
    fields(bundle, {"catalog", "registry", "policy", "run_manifest", "bundle_hash"}, where="Agent bundle")
    catalog = fields(bundle["catalog"], {"catalog_schema", "version", "agents"}, where="Agent catalog")
    if catalog["catalog_schema"] != "chain-agents/v0.3":
        raise ValueError("Unsupported Agent catalog schema")
    text(catalog["version"])
    policy=fields(bundle["policy"],{"engine_schema","policy_id","policy_version","site_id","runtime"},where="Agent Policy")
    if policy["engine_schema"]!="chain-policy/v0.3":raise ValueError("Unsupported Agent Policy schema")
    for key in ("policy_id","policy_version","site_id"):text(policy[key])
    fields(policy["runtime"],{"maximum_attempts","approved_agents","allowed_actions_for_agents","forbidden_actions_for_agents",
                             "transition_rules","notification_templates","hitl_rules"},where="Agent Policy runtime")
    if not isinstance(catalog["agents"], dict) or not catalog["agents"]:
        raise ValueError("Agent catalog requires registered implementations")
    for alias, spec in catalog["agents"].items():
        text(alias)
        fields(spec, {"agent_id", "version", "implementation", "data_scopes", "actions", "modes", "backend"}, where="Agent spec")
        for key in ("agent_id", "version", "implementation"):
            text(spec[key])
        for key in ("data_scopes", "actions", "modes"):
            strings(spec[key], nonempty=True)
        backend = fields(spec["backend"], {"kind"}, {"manifest"}, "Agent backend")
        if backend["kind"] == "structured":
            if "manifest" in backend:
                raise ValueError("Structured backend does not read fixtures")
        elif backend["kind"] == "fixture":
            text(backend.get("manifest"), "fixture manifest")
        else:
            raise ValueError("Unsupported Agent backend")
    validate_bindings(catalog, bundle["policy"]["runtime"], bundle["registry"], contract="chain-agent/v0.3")
    manifest = bundle["run_manifest"]
    if (manifest["registry_hash"] != canonical_hash(bundle["registry"])
            or manifest["policy"]["hash"] != canonical_hash(bundle["policy"])
            or manifest["implementation"]["hash"] != canonical_hash(manifest["implementation"]["files"])):
        raise ValueError("Agent run manifest does not match pinned configuration")
    if canonical_hash({k:v for k,v in bundle.items() if k != "bundle_hash"}) != bundle["bundle_hash"]:
        raise ValueError("Agent bundle hash mismatch")


def load_agent_bundle(*,catalog_path=None,policy_path=None,registry_path=None):
    catalog=_read_yaml(Path(catalog_path) if catalog_path else ROOT/"config/agents_v03.yaml")
    policy=_read_yaml(Path(policy_path) if policy_path else ROOT/"config/policy_v03.yaml")
    registry=_read_yaml(Path(registry_path) if registry_path else ROOT/"config/plugins.yaml")
    verify_installation(registry,ROOT)
    code={str(path.relative_to(ROOT)):"sha256:"+hashlib.sha256(path.read_bytes()).hexdigest()
          for path in sorted((ROOT/"chain_demo").rglob("*.py"))}
    try:sdk_version=version("temporalio")
    except PackageNotFoundError:sdk_version=None
    manifest={"manifest_schema":"chain-run/v0.3","workflow":{"version":catalog["version"],"hash":canonical_hash(catalog)},
              "policy":{"version":policy["policy_version"],"hash":canonical_hash(policy)},"registry_hash":canonical_hash(registry),
              "implementation":{"version":IMPLEMENTATION_VERSION,"files":code,"hash":canonical_hash(code)},
              "temporal_sdk_version":sdk_version}
    bundle = {"catalog":catalog,"registry":registry,"policy":policy,"run_manifest":manifest}
    bundle["bundle_hash"] = canonical_hash(bundle)
    validate_agent_bundle(bundle)
    return copy.deepcopy(bundle)
