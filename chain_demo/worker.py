"""Worker entrypoint; v0.3 defaults to an empty published Store, not preloaded sources."""
import argparse
import asyncio
import logging

from temporalio.client import Client
from temporalio.worker import Worker

from .config import ROOT
from .data_io import load_initial
from .adapters.store import PublishedStore
from .agents.runtime import AgentRuntime
from .notifier import MockNotifier
from .orchestration_activities import OrchestrationActivities
from .orchestration_config import load_orchestration_bundle
from .temporal_workflow import ConfigurableClinicalWorkflow


def create_worker(client, *, task_queue, configuration, store, receipt_path, write=print, effects=None):
    runtime = AgentRuntime(configuration["agents"])
    notifier = MockNotifier(receipt_path, configuration["engine"]["policy"]["notification_templates"], write=write)
    activities = OrchestrationActivities(configuration["engine"], runtime, store, notifier, effects=effects)
    return Worker(client, task_queue=task_queue, workflows=[ConfigurableClinicalWorkflow],
                  activities=[activities.resolve, activities.invoke, activities.notify, activities.effect])


async def serve(address, task_queue, *, initial_path, receipt_path):
    client = await Client.connect(address)
    initial = load_initial(initial_path)
    store = PublishedStore(initial)  # Publish through source adapters; no future file preload.
    worker = create_worker(client, task_queue=task_queue, configuration=load_orchestration_bundle(),
                           store=store, receipt_path=receipt_path)
    print(f"Worker ready: {address} / task_queue={task_queue} / v0.3", flush=True)
    await worker.run()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--address", default="localhost:7233")
    parser.add_argument("--task-queue")
    parser.add_argument("--initial", default=str(ROOT/"episodes/stroke_reference_001_v03/initial.json"))
    parser.add_argument("--receipt-db", help="new run's SQLite path; required for v0.3")
    args = parser.parse_args()
    if not args.receipt_db:
        parser.error("--receipt-db is required for v0.3; use a new run path")
    queue = args.task_queue or "chain-v03"
    logging.basicConfig(level=logging.INFO)
    asyncio.run(serve(args.address, queue, initial_path=args.initial, receipt_path=args.receipt_db))


if __name__ == "__main__":
    main()
