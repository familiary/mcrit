#!/usr/bin/python

import signal
import threading
import time
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pymongo.errors

from mcrit.queue.QueueRemoteCalls import JobTimeoutExceeded
from mcrit.SpawningWorker import SpawningWorker
from mcrit.Worker import Worker

from .context import config


class WorkerResilienceTestSuite(unittest.TestCase):
    """#106: a transient database outage must suspend the worker loop, not kill it, and a
    child that dies without producing a result must fail the job instead of completing it."""

    def _makeWorker(self):
        queue = MagicMock()
        queue.clean_interval = 10**9
        worker = SpawningWorker(queue=queue, config=config, storage=MagicMock())
        return worker, queue

    def testRunLoopSurvivesPollingErrors(self):
        worker, queue = self._makeWorker()
        calls = []

        def next_side_effect():
            calls.append(len(calls))
            if len(calls) == 1:
                raise pymongo.errors.ServerSelectionTimeoutError("mongod restarting")
            if len(calls) == 2:
                raise pymongo.errors.NotPrimaryError("interrupted at shutdown")
            worker.terminate()
            return None

        queue.next.side_effect = next_side_effect
        with patch("mcrit.SpawningWorker.time.sleep"):
            run_thread = threading.Thread(target=worker.run)
            run_thread.start()
            run_thread.join(timeout=10)
        self.assertFalse(run_thread.is_alive(), "run loop must terminate cleanly")
        self.assertEqual(len(calls), 3, "polling must continue across transient errors")

    def testDeadChildFailsJobInsteadOfCompleting(self):
        worker, queue = self._makeWorker()
        worker.t_last_cleanup = 10**12  # keep clean() out of this test
        job = MagicMock()
        job.__enter__ = MagicMock(return_value={"payload": {}})
        # capture what exception (if any) reaches the job context manager's __exit__
        exit_types = []
        job.__exit__ = MagicMock(side_effect=lambda t, v, tb: exit_types.append(t))
        with patch.object(SpawningWorker, "_executeJobPayload", return_value=(None, 1)):
            worker._executeJob(job)
        self.assertEqual(len(exit_types), 1)
        self.assertIsNotNone(exit_types[0], "a dead child must route the job through the error path, not complete()")
        self.assertTrue(issubclass(exit_types[0], RuntimeError))

    def testStuckOutputReadersDoNotBlockTheWorker(self):
        # the reader threads own the pipes and see EOF when the last writer closes them, but a
        # surviving grandchild of the job process keeps the inherited descriptors open - joining
        # them unbounded would hang the poll loop, so the join is capped and the worker moves on
        worker, _queue = self._makeWorker()
        job = MagicMock()
        release_readers = threading.Event()

        class BlockingPipe:
            def readline(self):
                # blocks like a pipe with no writer closing it, until the test releases it
                release_readers.wait()
                return b""

            def close(self):
                pass

        class ExitedChildWithLivePipes:
            returncode = 0

            def __init__(self):
                self.stdout = BlockingPipe()
                self.stderr = BlockingPipe()

            def wait(self, timeout=None):
                return self.returncode

            def kill(self):
                pass

        result = {}

        def call():
            result["returned"] = worker._executeJobPayload({}, job)

        with (
            patch("mcrit.SpawningWorker.subprocess.Popen", return_value=ExitedChildWithLivePipes()),
            patch("mcrit.SpawningWorker.OUTPUT_READER_JOIN_TIMEOUT", 0.2),
        ):
            call_thread = threading.Thread(target=call)
            call_thread.start()
            call_thread.join(timeout=15)
            still_running = call_thread.is_alive()
            release_readers.set()
            call_thread.join(timeout=15)
        self.assertFalse(still_running, "_executeJobPayload must not block on readers that cannot finish")
        # no result_id could be parsed, so the caller fails the job and the queue retries it
        self.assertEqual((None, 0), result["returned"])

    def testSuccessfulChildStillCompletes(self):
        worker, queue = self._makeWorker()
        worker.t_last_cleanup = 10**12
        job = MagicMock()
        job.__enter__ = MagicMock(return_value={"payload": {}})
        exit_types = []
        job.__exit__ = MagicMock(side_effect=lambda t, v, tb: exit_types.append(t))
        with patch.object(SpawningWorker, "_executeJobPayload", return_value=("aabbccddeeff001122334455", 0)):
            worker._executeJob(job)
        self.assertEqual(exit_types, [None], "a child that produced a result_id must complete the job")


class PlainWorkerJobTimeoutTestSuite(unittest.TestCase):
    """The plain worker's heartbeat keeps a job that never ends claimed, so the worker itself has to
    stop it: one request for a job running for days held a `mcrit worker` for days."""

    def _worker(self, timeout):
        worker_config = deepcopy(config)
        worker_config.QUEUE_CONFIG.QUEUE_WORKER_JOB_TIMEOUT = timeout
        queue = MagicMock()
        queue.clean_interval = 10**9
        return Worker(queue=queue, config=worker_config, storage=MagicMock()), queue

    def _job(self, name, exits):
        job = MagicMock()
        job.__enter__.return_value = {"payload": name}
        job.__exit__.side_effect = lambda exc_type, exc, tb: exits.append((name, exc_type))
        return job

    def _run(self, timeout, work, time_limited=True):
        """Run a "slow" and a "next" job through the worker loop, each executing work(name)."""
        worker, queue = self._worker(timeout)
        exits = []
        handed_out = iter([self._job("slow", exits), self._job("next", exits)])

        def next_job():
            job = next(handed_out, None)
            if job is None:
                worker.terminate()
            return job

        def decode(job_payload):
            return MagicMock(progressor=False, time_limited=time_limited, side_effect=lambda: work(job_payload)), [], {}

        queue.next.side_effect = next_job
        with patch.object(Worker, "_decodeJobPayload", side_effect=decode), patch.object(Worker, "_storeJobResult", return_value=None):
            started = time.monotonic()
            worker.run()
        return exits, time.monotonic() - started, signal.getitimer(signal.ITIMER_REAL)

    def testAJobOverTheLimitFailsAndTheWorkerTakesTheNextOne(self):
        def work(name):
            # bounded, so a deadline that never fires fails the test instead of hanging it
            give_up = time.monotonic() + 20
            while name == "slow" and time.monotonic() < give_up:
                pass
            return {}

        exits, seconds, timer = self._run(1, work)
        self.assertEqual(("slow", JobTimeoutExceeded), exits[0], "the job's context must see the timeout, so it is failed and returned to the queue")
        self.assertEqual(("next", None), exits[1], "the worker must go on to the next job")
        self.assertLess(seconds, 30)
        self.assertEqual((0.0, 0.0), timer, "no alarm may be left armed after the job")

    def testTheJobsOwnExceptionHandlersDoNotSwallowIt(self):
        def work(name):
            give_up = time.monotonic() + 20
            while name == "slow" and time.monotonic() < give_up:
                try:
                    time.sleep(0.05)
                except Exception:
                    pass
            return {}

        exits, _, _ = self._run(1, work)
        self.assertEqual([("slow", JobTimeoutExceeded), ("next", None)], exits)

    def testAMaintenanceJobIsNotInterrupted(self):
        exits, _, timer = self._run(1, lambda name: time.sleep(1.5) or {}, time_limited=False)
        self.assertEqual([("slow", None), ("next", None)], exits)
        self.assertEqual((0.0, 0.0), timer)

    def testOnlyJobsThatReadTheIndexAreTimed(self):
        timed = {name for name, method in vars(Worker).items() if getattr(method, "time_limited", False)}
        remote = {name for name, method in vars(Worker).items() if getattr(method, "remote", False)}
        self.assertEqual(
            {
                "getMatchesForSmdaReport",
                "getMatchesForMappedBinary",
                "getMatchesForUnmappedBinary",
                "getMatchesForSample",
                "getMatchesForSampleVs",
                "getMatchesForSampleVsGroup",
                "combineMatchesToCross",
                "getUniqueBlocks",
            },
            timed,
        )
        # the jobs that write minhashes and band postings in steps are never cut between them
        self.assertTrue(
            {
                "addBinarySample",
                "updateMinHashes",
                "updateMinHashesForSample",
                "rebuildIndex",
                "recalculatePicHashes",
                "recalculateMinHashes",
                "repairMinHashes",
                "rebuildBandDfIndex",
                "doDbCleanup",
                "deleteSample",
            }
            <= remote - timed
        )

    def testAQueueConfigWithoutTheSettingGetsTheDefault(self):
        # docker-mcrit ships its own QueueConfig, which has no QUEUE_WORKER_JOB_TIMEOUT
        worker, queue = self._worker(1)
        worker._queue_config = SimpleNamespace()
        armed = []
        handed_out = iter([self._job("only", [])])

        def next_job():
            job = next(handed_out, None)
            if job is None:
                worker.terminate()
            return job

        queue.next.side_effect = next_job
        decoded = (MagicMock(progressor=False, time_limited=True, side_effect=lambda: armed.append(signal.getitimer(signal.ITIMER_REAL)[0]) or {}), [], {})
        with patch.object(Worker, "_decodeJobPayload", return_value=decoded), patch.object(Worker, "_storeJobResult", return_value=None):
            worker.run()
        self.assertEqual(1, len(armed))
        self.assertGreater(armed[0], 3000)

    def testAWorkerOutsideTheMainThreadRunsJobsWithoutTheAlarm(self):
        results = []
        errors = []

        def in_thread():
            try:
                results.append(self._run(1, lambda name: time.sleep(1.2) or {}))
            except BaseException as error:  # noqa: BLE001
                errors.append(error)

        thread = threading.Thread(target=in_thread)
        thread.start()
        thread.join(60)
        self.assertEqual([], errors)
        exits, _, _ = results[0]
        self.assertEqual([("slow", None), ("next", None)], exits, "SIGALRM cannot be set outside the main thread: the jobs run unbounded")

    def testZeroIsNoLimit(self):
        exits, _, timer = self._run(0, lambda name: time.sleep(1.5) or {})
        self.assertEqual([("slow", None), ("next", None)], exits)
        self.assertEqual((0.0, 0.0), timer)

    def testOnlyTheWorkerLoopTimesJobs(self):
        # a LocalQueue runs jobs in the caller's thread through _executeJob, without the loop
        worker, _ = self._worker(1)
        exits = []
        with patch.object(Worker, "_decodeJobPayload", return_value=(MagicMock(progressor=False, side_effect=lambda: signal.getitimer(signal.ITIMER_REAL)), [], {})):
            with patch.object(Worker, "_storeJobResult", side_effect=lambda job, result: exits.append(result)):
                worker._executeJob(self._job("in-process", []))
        self.assertEqual([(0.0, 0.0)], exits)


if __name__ == "__main__":
    unittest.main()
