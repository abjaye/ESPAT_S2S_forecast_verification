"""Runs the shared Dask cluster in a background process.

Don't edit or run this file directly: start the cluster from cluster.ipynb,
which holds all the settings (project, workers, memory, ...) and passes them
to this script. Because the cluster runs in its own process, restarting or
shutting down notebook kernels (including cluster.ipynb) does not stop it.

In any notebook, connect to the running cluster with:

    from dask_cluster import get_client
    client = get_client()

The process keeps running until client.shutdown() is called from any
notebook, then cancels the worker jobs and exits.
"""
import json
import os
import signal
import sys
import time

from dask.distributed import Client
from distributed.core import Status

SCHEDULER_FILE = os.path.expandvars("$SCRATCH/dask/scheduler.json")


def get_client():
    """Connect to the cluster started by cluster.ipynb."""
    if os.path.exists(SCHEDULER_FILE):
        try:
            return Client(scheduler_file=SCHEDULER_FILE, timeout="10s")
        except OSError:
            pass
    raise RuntimeError("No dask cluster is running. Start one by running cluster.ipynb.")


def main(settings):
    from dask_jobqueue import PBSCluster

    dask_dir = settings["DASK_DIR"]
    scheduler_file = os.path.join(dask_dir, "scheduler.json")
    worker_log_dir = os.path.join(dask_dir, "dask_logs")
    os.makedirs(worker_log_dir, exist_ok=True)

    # A plain `kill <pid>` should also clean up the worker jobs
    signal.signal(signal.SIGTERM, lambda *args: sys.exit(0))

    cluster = PBSCluster(
        account=settings["PROJECT"],
        queue=settings["QUEUE"],
        job_name="dask-worker",
        cores=settings["CORES"],
        processes=1,
        memory=f"{settings['MEMORY_GB']}GB",
        resource_spec=f"select=1:ncpus={settings['CORES']}:mem={settings['MEMORY_GB']}GB",
        walltime=settings["WALLTIME"],
        interface="ext",
        local_directory=os.path.join(dask_dir, "pbs.$PBS_JOBID/dask/spill"),
        job_extra_directives=[
            f"-o {worker_log_dir}/",
            f"-e {worker_log_dir}/",
            "-m n",  # do not send mail
        ],
    )
    cluster.scale(jobs=settings["N_WORKERS"])
    client = Client(cluster)
    client.write_scheduler_file(scheduler_file)

    print(f"Scheduler: {client.scheduler.address}")
    print(f"Dashboard: {client.dashboard_link}")
    print("\nWorker job script:\n" + cluster.job_script(), flush=True)

    try:
        # Wait until someone calls client.shutdown(), which closes the scheduler
        while cluster.scheduler.status not in (Status.closing, Status.closed, Status.failed):
            time.sleep(10)
        print("Scheduler shut down; cleaning up.", flush=True)
    finally:
        # Remove the scheduler file first, so notebooks never find a dead cluster
        if os.path.exists(scheduler_file):
            os.remove(scheduler_file)
        # Cancel any worker jobs still in PBS. Time-limited so cleanup can't hang;
        # if it does time out, workers exit on their own within about a minute
        # of losing the scheduler, which ends their PBS jobs.
        try:
            cluster.close(timeout=60)
        except Exception as e:
            print(f"cluster.close() did not finish cleanly: {e!r}", flush=True)
        print("Cluster stopped.", flush=True)
        os._exit(0)  # don't wait on leftover dask background threads


if __name__ == "__main__":
    main(json.loads(sys.argv[1]))
