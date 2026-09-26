#!/usr/bin/env python3

import logging
import os
import re
import signal
import subprocess
import sys
import threading
import time
import uuid
from typing import TYPE_CHECKING, Optional

import pymongo.errors

from mcrit.config.McritConfig import McritConfig
from mcrit.minhash.MinHasher import MinHasher
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

if TYPE_CHECKING:
    from mcrit.storage.StorageInterface import StorageInterface

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

# Grace period for the stdout/stderr reader threads to drain their pipes after the child has
# exited. They see EOF as soon as the last writer closes, so this only expires if something
# still holds the inherited descriptors (e.g. a surviving grandchild of the job process) - in
# which case the worker must give up on the output rather than block its poll loop forever.
OUTPUT_READER_JOIN_TIMEOUT = 30
# how often a job process's memory is measured against QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY
MEMORY_POLL_INTERVAL = 1.0


def _canMeasureProcessMemory() -> bool:
    return os.path.exists(f"/proc/{os.getpid()}/statm")


def _processTree(pid: int) -> list:
    """The pid and every process descending from it, from the parent pids in /proc."""
    children = {}
    for entry in os.listdir("/proc"):
        if not entry.isdigit():
            continue
        try:
            with open(f"/proc/{entry}/stat") as stat:
                # the command name is in parentheses and may contain spaces; the parent pid follows it
                parent = int(stat.read().rsplit(")", 1)[1].split()[1])
        except (OSError, IndexError, ValueError):
            continue
        children.setdefault(parent, []).append(int(entry))
    tree, pending = [], [pid]
    while pending:
        current = pending.pop()
        tree.append(current)
        pending.extend(children.get(current, []))
    return tree


def _residentBytes(pids: list) -> int:
    page_size = os.sysconf("SC_PAGE_SIZE")
    total = 0
    for pid in pids:
        try:
            with open(f"/proc/{pid}/statm") as statm:
                total += int(statm.read().split()[1]) * page_size
        except (OSError, IndexError, ValueError):
            continue
    return total


def _killProcessTree(console_handle) -> None:
    """Kill a job process and what it started, e.g. a hashing pool, which would otherwise outlive it."""
    descendants = _processTree(console_handle.pid)[1:] if _canMeasureProcessMemory() else []
    console_handle.kill()
    for pid in descendants:
        try:
            os.kill(pid, signal.SIGKILL)
        except OSError:
            pass
    console_handle.wait()


class SpawningWorker(Worker):
    _warned_about_memory_limit = False

    def __init__(self, queue=None, config=None, storage: Optional["StorageInterface"] = None, profiling=False):
        self._worker_id = f"Worker-{uuid.uuid4()}"
        LOGGER.info(f"Starting as spawning worker: {self._worker_id}")
        if config is None:
            config = McritConfig()

        if not queue:
            queue = QueueFactory().getQueue(config, consumer_id=self._worker_id)

        if profiling:
            print("[!] Running as profiled application.")
            profiling_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "profiler"))
            os.makedirs(profiling_path, exist_ok=True)
        else:
            profiling_path = None
        super().__init__(queue=queue, config=config, storage=storage, profiling=profiling)

        self.config = config
        self._storage_config = config.STORAGE_CONFIG
        self._minhash_config = config.MINHASH_CONFIG
        self._shingler_config = config.SHINGLER_CONFIG
        self._queue_config = config.QUEUE_CONFIG
        self.minhasher = MinHasher(config.MINHASH_CONFIG, config.SHINGLER_CONFIG)
        if storage:
            self._storage = storage
        else:
            self._storage = StorageFactory.getStorage(config)

    def __enter__(self):
        return self

    def __exit__(self, *args):
        # TODO unregister our worker_id from all in-progress jobs found in the queue
        try:
            self.queue.unregisterWorker()
            self.queue.release_all_jobs()
        except pymongo.errors.PyMongoError:
            # best-effort teardown: if the database is unreachable while we exit, orphaned
            # locks are reclaimed later by release_orphaned_jobs/clean rather than turning
            # the shutdown itself into a crash
            LOGGER.error("Could not release jobs on shutdown, queue cleanup will reclaim them.", exc_info=True)

    #### NO REDIRECTION: SPAWM SINGLE JOB WORKERS INSTEAD ###

    def _jobCommand(self, job):
        # sys.executable rather than "python", which may not be on PATH or may be another interpreter
        return [sys.executable, "-m", "mcrit", "singlejobworker", "--job_id", str(job.job_id)]

    def _awaitJobProcess(self, console_handle, job):
        """Wait for a job process, stopping it at QUEUE_SPAWNINGWORKER_CHILDREN_TIMEOUT or, when
        QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY is set, once its processes hold more memory than that.

        The memory is the resident size of the job's whole process tree, taken from /proc once per
        MEMORY_POLL_INTERVAL: a job hashing with a process pool spreads over several processes, and a
        limit on any one of them would neither bound the job nor account for its shared libraries
        the way resident memory does (#69).
        """
        deadline = time.monotonic() + self._queue_config.QUEUE_SPAWNINGWORKER_CHILDREN_TIMEOUT
        memory_limit = self._queue_config.QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY
        if memory_limit > 0 and not _canMeasureProcessMemory():
            if not SpawningWorker._warned_about_memory_limit:
                LOGGER.warning("QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY needs /proc (Linux); job processes run without a memory limit.")
                SpawningWorker._warned_about_memory_limit = True
            memory_limit = 0
        while True:
            remaining = deadline - time.monotonic()
            try:
                console_handle.wait(timeout=max(0.0, min(remaining, MEMORY_POLL_INTERVAL) if memory_limit > 0 else remaining))
                return
            except subprocess.TimeoutExpired:
                pass
            if time.monotonic() >= deadline:
                LOGGER.error(f"Job {str(job.job_id)} running as child from SpawningWorker timed out during processing.")
                _killProcessTree(console_handle)
                return
            if memory_limit > 0:
                used = _residentBytes(_processTree(console_handle.pid))
                if used > memory_limit:
                    LOGGER.error(f"Job {str(job.job_id)} holds {used} bytes of memory, more than QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY ({memory_limit}); stopping it.")
                    _killProcessTree(console_handle)
                    return

    def _executeJobPayload(self, job_payload, job):
        # instead of execution within our own context, spawn a new process as worker for this job payload
        console_handle = subprocess.Popen(self._jobCommand(job), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # extract result_id from console_output
        result_id = None
        stdout_lines = []

        def reader(pipe, label, accum):
            try:
                for line in iter(pipe.readline, b""):
                    decoded_line = line.decode("utf-8", errors="replace").rstrip()
                    if decoded_line:
                        LOGGER.info("%s logs from subprocess: %s", label, decoded_line)
                    if accum is not None:
                        accum.append(decoded_line)
            except Exception:
                LOGGER.exception("Exception in subprocess reader thread")
            finally:
                pipe.close()

        t1 = threading.Thread(target=reader, args=(console_handle.stdout, "STDOUT", stdout_lines))
        t2 = threading.Thread(target=reader, args=(console_handle.stderr, "STDERR", None))
        t1.daemon = True
        t2.daemon = True
        t1.start()
        t2.start()

        # the reader threads own the pipes; communicate() would race them for the same file
        # descriptors (observed as OSError EBADF when a child dies mid-read), so only wait for the
        # exit code here and let the readers drain the output
        self._awaitJobProcess(console_handle, job)

        t1.join(timeout=OUTPUT_READER_JOIN_TIMEOUT)
        t2.join(timeout=OUTPUT_READER_JOIN_TIMEOUT)
        if t1.is_alive() or t2.is_alive():
            # proceeding with a possibly truncated stdout can cost us the result_id, which routes
            # the job through the error path and retries it - the right trade against hanging here
            LOGGER.error(
                "Output readers for job %s did not finish within %d s after the child exited; continuing with the output collected so far.",
                str(job.job_id),
                OUTPUT_READER_JOIN_TIMEOUT,
            )

        if stdout_lines:
            # Search backwards for result_id in case there are trailing empty lines or other output
            for line in reversed(stdout_lines):
                if line.strip():
                    match = re.match("(?P<result_id>[0-9a-fA-F]{24})", line.strip())
                    if match:
                        result_id = match.group("result_id")
                        break
        return result_id, console_handle.returncode

    def _executeJob(self, job):
        if time.time() - self.t_last_cleanup >= self.queue.clean_interval:
            try:
                self.queue.clean()
            except pymongo.errors.PyMongoError:
                # periodic housekeeping must not take the worker down with it; the next
                # interval retries once the database is reachable again
                LOGGER.error("Queue cleanup failed, deferring to next interval.", exc_info=True)
            self.t_last_cleanup = time.time()
        try:
            result_id = None
            with job as j:
                LOGGER.info("Processing Remote Job: %s", job)
                result_id, child_returncode = self._executeJobPayload(j["payload"], job)
                if result_id:
                    # result should have already been persisted by the child process, we repeat it here to close the job for the queue
                    job.result = result_id
                    LOGGER.info("Finished Remote Job with result_id: %s", result_id)
                else:
                    # raising routes the job through Job.__exit__'s error path, which returns
                    # it to the queue with attempts_left decremented - falling through would
                    # have __exit__ complete() it, reporting a dead child as a finished job
                    raise RuntimeError(f"child worker exited (returncode {child_returncode}) without producing a result_id")
        except Exception:
            # the failure may include the Job.__exit__ error() write itself (e.g. the
            # database went away mid-job), in which case the job is still locked by us
            # with its release lost - reconcile before the next claim
            self._needs_lock_reconcile = True
            LOGGER.error("Error occurred while executing job: %s", job, exc_info=True)

    def run(self):
        self._alive = True
        self._needs_lock_reconcile = False
        backoff_seconds = 1
        while self._alive:
            try:
                if self._needs_lock_reconcile:
                    # a previous failure may have left a job locked by this consumer with
                    # its error() release lost; nothing else reclaims a live worker's locks
                    # (orphan release only covers unregistered consumers), so release our
                    # own before claiming anew. Runs BEFORE next() so it can never release
                    # a job we just claimed. No-op when the release already landed.
                    self.queue.release_all_jobs()
                    self._needs_lock_reconcile = False
                job = self.queue.next()
                if job:
                    LOGGER.debug("Found job")
                    self._executeJob(job)
                else:
                    time.sleep(0.1)
                backoff_seconds = 1
            except pymongo.errors.PyMongoError:
                # a transient database outage (e.g. mongod restart during maintenance) must
                # suspend the worker, not kill it: pymongo re-establishes the connection on
                # its own once the server is back, so keep polling with a capped backoff
                LOGGER.error("Queue polling failed, retrying in %d s.", backoff_seconds, exc_info=True)
                time.sleep(backoff_seconds)
                backoff_seconds = min(backoff_seconds * 2, 30)
