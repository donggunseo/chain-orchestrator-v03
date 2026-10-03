"""Worker/client configuration loader; only public metadata enters the Engine."""
import copy
from pathlib import Path

from .agent_config import load_agent_bundle, validate_agent_bundle
from .config import ROOT, _read_yaml
from .orchestration_schema import validate_engine_bundle
from .validation import canonical_hash, fields


def project_engine_bundle(workflow, agents):
    validate_agent_bundle(agents)
    workflow=copy.deepcopy(workflow)
    for alias,setting in workflow["agents"].items():
        fields(setting,{"scopes","timeout_s","needs_context","outputs"},where="Workflow Agent settings")
        spec=agents["catalog"]["agents"][alias]
        manifest=agents["registry"]["implementations"][spec["implementation"]]
        setting.update({key:copy.deepcopy(spec[key]) for key in ("agent_id","version","modes","data_scopes","actions")})
        setting["manifest_hash"]=manifest["manifest_hash"]
    original=agents["policy"];runtime=original["runtime"]
    policy={"policy_id":original["policy_id"],"version":original["policy_version"],"site_id":original["site_id"],
            **{key:copy.deepcopy(runtime[key]) for key in ("maximum_attempts","approved_agents","allowed_actions_for_agents",
                 "forbidden_actions_for_agents","transition_rules","notification_templates","hitl_rules")}}
    bundle={"engine_schema":"chain-orchestrator/v0.3","workflow":workflow,"policy":policy,
            "catalog_hash":agents["bundle_hash"],"run_manifest":copy.deepcopy(agents["run_manifest"])}
    # Pin the new executable Workflow separately from the preserved legacy one.
    bundle["run_manifest"]["workflow"]={"version":workflow["version"],"hash":canonical_hash(workflow)}
    bundle["bundle_hash"]=canonical_hash(bundle)
    validate_engine_bundle(bundle)
    return bundle


def load_orchestration_bundle(*,workflow_path=None,catalog_path=None,policy_path=None,registry_path=None):
    agents=load_agent_bundle(catalog_path=catalog_path,policy_path=policy_path,registry_path=registry_path)
    workflow=_read_yaml(Path(workflow_path) if workflow_path else ROOT / "config/workflow_v03.yaml")
    return {"engine":project_engine_bundle(workflow,agents),"agents":agents}
