import os
import platform
import signal
import subprocess
import sys
import time
import unittest
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pymongo
import pytest
from smda.common.SmdaReport import SmdaReport

import mcrit.SpawningWorker as SpawningWorker_module
from mcrit.config.McritConfig import McritConfig
from mcrit.config.MinHashConfig import MinHashConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs import memory
from mcrit.matchers.MatcherInterface import MatcherInterface
from mcrit.matchers.MatcherQuery import MatcherQuery
from mcrit.matchers.MatcherSample import MatcherSample
from mcrit.SpawningWorker import SpawningWorker
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

from .context import config, getTestMongoServerAndPort

FIXTURES = os.path.dirname(os.path.abspath(__file__))
RESULT_ID = "89abcdef0123456789abcdef"
CAN_MEASURE = SpawningWorker_module._canMeasureProcessMemory()


def _running(pid):
    """Whether pid is a live process; a killed one may linger as a zombie until something reaps it."""
    try:
        with open(f"/proc/{pid}/stat") as stat:
            return stat.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return False


def load_report(name):
    return SmdaReport.fromFile(os.path.join(FIXTURES, name))


class CorpusFixture:
    """The matcher tests' corpus: two samples, a library and a third sample, all minhashed."""

    def _config(self, pichash_max_matches):
        mcrit_config = deepcopy(config)
        # a new MinHashConfig, not an edit: McritConfig holds it as a class attribute, shared by every copy
        mcrit_config.MINHASH_CONFIG = MinHashConfig(MINHASH_PICHASH_MAX_MATCHES=pichash_max_matches)
        return mcrit_config

    def _index(self, pichash_max_matches=0):
        index = MinHashIndex(config=self._config(pichash_max_matches))
        index._storage.clearStorage()
        worker = index.queue._worker
        sample_ids = [
            index._storage.addSmdaReport(load_report(name)).sample_id for name in ("example_report.smda", "example_report_2.smda", "library_report.smda", "example_report_3.smda")
        ]
        for sample_id in sample_ids:
            worker.updateMinHashesForSample(sample_id)
        return index, worker, sample_ids

    def _sameCorpusWithCutoff(self, pichash_max_matches):
        return self._index(pichash_max_matches)[0]


class AggregationOnlyTest(CorpusFixture, unittest.TestCase):
    """The pichash and minhash aggregations only need totals, so they skip the per-function lists (#69).

    Every call is checked against a full summary of the same matches: the totals have to be exactly
    what the full pass computes, on the matchers that build reports - sample and query matching.
    """

    def _checkingSummaries(self):
        original = MatcherInterface._summarizeMatches
        checked = []

        def summarize(matcher, sample_id, matches, aggregation_only):
            if aggregation_only:
                full = original(matcher, sample_id, deepcopy(matches), False)
                fast = original(matcher, sample_id, matches, True)
                self.assertEqual(full[2], fast[2])
                self.assertEqual(full[3], fast[3])
                self.assertEqual([], fast[0])
                checked.append(len(matches))
                return fast
            return original(matcher, sample_id, matches, aggregation_only)

        return patch.object(MatcherInterface, "_summarizeMatches", autospec=True, side_effect=summarize), checked

    def test_sample_and_query_matching_totals_match_the_full_summary(self):
        index, worker, sample_ids = self._index()
        patcher, checked = self._checkingSummaries()
        with patcher:
            MatcherSample(worker).getMatchesForSample(sample_ids[0])
            MatcherQuery(worker).getMatchesForSmdaReport(load_report("example_report.smda"))
        # pichash and minhash aggregation, per matcher, and with matches to compare
        self.assertEqual(4, len(checked))
        self.assertTrue(all(checked))

    def test_reports_are_unchanged(self):
        index, worker, sample_ids = self._index()
        original = MatcherInterface._summarizeMatches

        def always_full(matcher, sample_id, matches, aggregation_only):
            return original(matcher, sample_id, matches, False)

        fast = MatcherSample(worker).getMatchesForSample(sample_ids[0])
        with patch.object(MatcherInterface, "_summarizeMatches", autospec=True, side_effect=always_full):
            full = MatcherSample(worker).getMatchesForSample(sample_ids[0])
        for report in (fast, full):
            report["info"].pop("job", None)
        self.assertEqual(full, fast)


class QueryPicHashLookupTest(CorpusFixture, unittest.TestCase):
    def test_one_lookup_gives_what_one_lookup_per_pichash_gave(self):
        index, worker, _ = self._index()
        storage = index._storage
        pichashes = sorted({function_entry.pichash for function_entry in storage.getFunctions(0, 0) if function_entry.pichash})
        expected = {pichash: storage.getMatchesForPicHash(pichash) for pichash in pichashes}
        self.assertEqual(expected, storage.getMatchesForPicHashes(pichashes + [0x1234567890ABCDEF]))
        # the answer is the caller's to extend, not the storage's own sets
        answer = storage.getMatchesForPicHashes(pichashes[:1])
        answer[pichashes[0]].add((-1, -1, -1))
        self.assertNotIn((-1, -1, -1), storage.getMatchesForPicHash(pichashes[0]))

    def test_the_pichash_cutoff_applies_to_query_matching(self):
        index, worker, _ = self._index()
        storage = index._storage
        holders = {pichash: len(storage.getMatchesForPicHash(pichash)) for pichash in {fe.pichash for fe in storage.getFunctions(0, 0) if fe.pichash}}
        common = max(holders, key=lambda pichash: holders[pichash])
        self.assertGreater(holders[common], 1)
        capped = self._sameCorpusWithCutoff(holders[common] - 1)
        self.assertNotIn(common, capped._storage.getMatchesForPicHashes([common]))
        self.assertIn(common, index._storage.getMatchesForPicHashes([common]))
        # a hash held by exactly as many functions as the cutoff allows is kept
        at_cutoff = self._sameCorpusWithCutoff(holders[common])
        self.assertEqual(index._storage.getMatchesForPicHashes([common]), at_cutoff._storage.getMatchesForPicHashes([common]))
        # MemoryStorage applies it to sample matching too, as MongoDbStorage does
        self.assertEqual(set(), capped._storage.getPicHashMatchesByFunctionIds([next(iter(storage.getMatchesForPicHash(common)))[2]])[common])

    def test_query_report_pichash_matches_come_from_the_batched_lookup(self):
        index, worker, _ = self._index()
        with patch.object(index._storage, "getMatchesForPicHash", side_effect=AssertionError("queried one pichash at a time")):
            report = MatcherQuery(worker).getMatchesForSmdaReport(load_report("example_report.smda"))
        self.assertGreater(report["matches"]["aggregation"]["pichash"]["num_own_functions_matched"], 0)


@pytest.mark.mongo
class MongoQueryPicHashLookupTest(QueryPicHashLookupTest):
    """The same on MongoDbStorage, whose cutoff counts holders through the _pichash index."""

    def _config(self, pichash_max_matches):
        mcrit_config = super()._config(pichash_max_matches)
        server, port = getTestMongoServerAndPort()
        mcrit_config.STORAGE_CONFIG = StorageConfig(
            STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME="test_query_memory"
        )
        return mcrit_config

    def _sameCorpusWithCutoff(self, pichash_max_matches):
        # the same database, read with the cutoff set
        return MinHashIndex(config=self._config(pichash_max_matches))

    def tearDown(self):
        server, port = getTestMongoServerAndPort()
        pymongo.MongoClient(server, int(port)).drop_database("test_query_memory")


class ChildMemoryLimitTest(unittest.TestCase):
    JOB = SimpleNamespace(job_id="0123456789abcdef01234567")

    @staticmethod
    def _worker(limit, timeout=60):
        worker = SpawningWorker.__new__(SpawningWorker)
        worker._queue_config = SimpleNamespace(QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY=limit, QUEUE_SPAWNINGWORKER_CHILDREN_TIMEOUT=timeout)
        return worker

    def _run(self, worker, script):
        """Run script as the job process, the way the worker runs a job; returns (result_id, returncode, seconds)."""
        started = time.monotonic()
        with patch.object(worker, "_jobCommand", return_value=[sys.executable, "-c", script]), patch("mcrit.SpawningWorker.MEMORY_POLL_INTERVAL", 0.05):
            result_id, returncode = worker._executeJobPayload({}, self.JOB)
        return result_id, returncode, time.monotonic() - started

    def test_no_limit_by_default(self):
        self.assertEqual(0, McritConfig().QUEUE_CONFIG.QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY)
        with patch("mcrit.SpawningWorker._residentBytes", side_effect=AssertionError("measured without a limit")):
            # long enough for several polls, were there any
            self.assertEqual((RESULT_ID, 0), self._run(self._worker(0), f"import time; time.sleep(0.5); print('{RESULT_ID}')")[:2])

    @unittest.skipUnless(CAN_MEASURE, "needs /proc")
    def test_a_job_over_the_limit_is_stopped(self):
        with patch("mcrit.SpawningWorker.LOGGER") as logger:
            result_id, returncode, seconds = self._run(self._worker(256 * 2**20), "import time; b = bytearray(512 * 2**20); time.sleep(30)")
        self.assertIsNone(result_id)
        self.assertEqual(-signal.SIGKILL, returncode)
        self.assertLess(seconds, 20)
        self.assertIn("QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY", str(logger.error.call_args_list))

    @unittest.skipUnless(CAN_MEASURE, "needs /proc")
    def test_the_memory_of_processes_the_job_started_counts(self):
        # each process stays under the limit, together they are over it: a job hashing with a pool
        pool = "import subprocess, sys, time; code = 'import time; b = bytearray(200 * 2**20); time.sleep(30)'; "
        pool += "[subprocess.Popen([sys.executable, '-c', code]) for _ in range(2)]; time.sleep(30)"
        result_id, returncode, seconds = self._run(self._worker(300 * 2**20), pool)
        self.assertEqual(-signal.SIGKILL, returncode)
        self.assertLess(seconds, 20)

    @unittest.skipUnless(CAN_MEASURE, "needs /proc")
    def test_a_job_under_the_limit_completes(self):
        script = f"b = bytearray(64 * 2**20); print('{RESULT_ID}')"
        self.assertEqual((RESULT_ID, 0), self._run(self._worker(512 * 2**20), script)[:2])

    @unittest.skipUnless(CAN_MEASURE, "needs /proc")
    def test_a_timed_out_job_is_stopped_with_what_it_started(self):
        # the grandchild holds the output pipes: left alive, it would keep the readers waiting
        script = "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(60)']); time.sleep(60)"
        with patch("mcrit.SpawningWorker.LOGGER") as logger:
            result_id, returncode, seconds = self._run(self._worker(0, timeout=1), script)
        self.assertEqual(-signal.SIGKILL, returncode)
        self.assertLess(seconds, 20)
        self.assertIn("timed out", str(logger.error.call_args_list))

    @unittest.skipUnless(CAN_MEASURE, "needs /proc")
    def test_the_process_tree_and_its_memory(self):
        script = "import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', 'import time; b = bytearray(64 * 2**20); time.sleep(60)']); time.sleep(60)"
        parent = subprocess.Popen([sys.executable, "-c", script])
        try:
            deadline = time.monotonic() + 10
            while len(SpawningWorker_module._processTree(parent.pid)) < 2 and time.monotonic() < deadline:
                time.sleep(0.05)
            tree = SpawningWorker_module._processTree(parent.pid)
            self.assertEqual(parent.pid, tree[0])
            self.assertEqual(2, len(tree))
            self.assertNotIn(os.getpid(), tree)
            while SpawningWorker_module._residentBytes(tree[1:]) < 64 * 2**20 and time.monotonic() < deadline:
                time.sleep(0.05)
            self.assertGreater(SpawningWorker_module._residentBytes(tree), SpawningWorker_module._residentBytes(tree[:1]) + 64 * 2**20 - 1)
            self.assertEqual(0, SpawningWorker_module._residentBytes([2**22 + 1]))
        finally:
            SpawningWorker_module._killProcessTree(parent)
        # SIGKILL is delivered asynchronously, the grandchild is not ours to wait for
        deadline = time.monotonic() + 5
        while _running(tree[1]) and time.monotonic() < deadline:
            time.sleep(0.05)
        self.assertFalse(_running(tree[1]))

    def test_without_proc_the_limit_is_ignored_with_one_warning(self):
        worker = self._worker(2**20)
        with patch("mcrit.SpawningWorker._canMeasureProcessMemory", return_value=False), patch.object(SpawningWorker, "_warned_about_memory_limit", False):
            with patch("mcrit.SpawningWorker.LOGGER") as logger:
                self.assertEqual((RESULT_ID, 0), self._run(worker, f"print('{RESULT_ID}')")[:2])
                self.assertEqual((RESULT_ID, 0), self._run(worker, f"print('{RESULT_ID}')")[:2])
        self.assertEqual(1, logger.warning.call_count)

    def test_job_processes_run_on_this_interpreter(self):
        command = self._worker(0)._jobCommand(self.JOB)
        self.assertEqual([sys.executable, "-m", "mcrit", "singlejobworker", "--job_id", self.JOB.job_id], command)


class ReturnFreedMemoryTest(unittest.TestCase):
    def setUp(self):
        patcher = patch.object(memory, "_malloc_trim", None)
        patcher.start()
        self.addCleanup(patcher.stop)

    @unittest.skipUnless(sys.platform.startswith("linux") and platform.libc_ver()[0] == "glibc", "malloc_trim is glibc's")
    def test_glibc_hands_freed_memory_back(self):
        self.assertTrue(memory.return_freed_memory())

    def test_without_malloc_trim_it_is_skipped_and_not_looked_up_again(self):
        with patch("mcrit.libs.memory.ctypes.CDLL", side_effect=OSError("no libc")) as cdll:
            self.assertFalse(memory.return_freed_memory())
            self.assertFalse(memory.return_freed_memory())
        self.assertEqual(1, cdll.call_count)

    def test_a_worker_returns_the_memory_after_every_job_failed_or_not(self):
        worker = Worker.__new__(Worker)
        worker.queue = MagicMock(clean_interval=10**9)
        worker.t_last_cleanup = time.time()
        worker._needs_lock_reconcile = False
        job = MagicMock()
        job.__enter__.return_value = {"payload": {}}
        for outcome in ({"report": 1}, RuntimeError("job failed")):
            with patch.object(worker, "_executeJobPayload", side_effect=[outcome]), patch("mcrit.queue.QueueRemoteCalls.return_freed_memory") as returned:
                worker._executeJobImpl(job)
            self.assertEqual(1, returned.call_count)


if __name__ == "__main__":
    unittest.main()
