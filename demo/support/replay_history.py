"""Replay saved real History without a Service; write only a new report path."""
import argparse
import asyncio
from pathlib import Path

from temporalio.client import WorkflowHistory
from temporalio.worker import Replayer

from chain_demo.data_io import read_json
from demo.run import save_json
from chain_demo.temporal_workflow import ConfigurableClinicalWorkflow
from chain_demo.validation import canonical_hash


async def replay(directory, report_path):
    report_path = Path(report_path)
    if report_path.exists(): raise FileExistsError("Replay report already exists")
    directory = Path(directory)
    execution = read_json(directory / "temporal_execution.json")
    raw = read_json(directory / "temporal_history.json")
    checksum = canonical_hash(raw)
    if checksum != execution["history_hash"]: raise ValueError("Saved History hash differs from execution evidence")
    history = WorkflowHistory.from_json(execution["workflow_id"], raw)
    report = {"status": "FAILED", "history_hash": checksum, "workflow_id": execution["workflow_id"],
              "history_event_count": len(history.events), "workflow_status": execution["workflow_status"],
              "source": "SAVED_JSON_HISTORY", "service_used": False, "error": None}
    try:
        await Replayer(workflows=[ConfigurableClinicalWorkflow]).replay_workflow(history)
        report["status"] = "PASSED"
    except Exception as exc:
        report["error"] = f"{type(exc).__name__}: {exc}"
        raise
    finally: save_json(report_path, report)
    print("REPLAY_SAVED_HISTORY", report["status"], report["history_event_count"], report["workflow_status"])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory")
    parser.add_argument("--report", required=True, help="New output JSON path; never overwrite")
    args = parser.parse_args()
    try: asyncio.run(replay(args.directory, args.report))
    except (ValueError, OSError) as exc: parser.error(str(exc))


if __name__ == "__main__": main()
