import os
import unittest
from datetime import datetime, timedelta
from types import SimpleNamespace
from unittest.mock import MagicMock

from smda.common.SmdaReport import SmdaReport

from mcrit.queue.LocalQueue import LocalQueue
from mcrit.storage.SampleEntry import SampleEntry
from mcrit.Worker import Worker

from .context import config

EXAMPLE_REPORT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "example_report.smda")

TTL = config.STORAGE_CONFIG.STORAGE_MONGODB_CLEANUP_TTL
OLD = datetime.now() - timedelta(seconds=TTL + 3600)
RECENT = datetime.now() - timedelta(seconds=60)


def _job(job_id, method, started_at=None, finished_at=None):
    return {"_id": job_id, "payload": {"method": method, "params": "[]"}, "started_at": started_at, "finished_at": finished_at, "created_at": started_at or RECENT, "result": None}


def _sample(sample_id, sha256, timestamp):
    return SimpleNamespace(sample_id=sample_id, sha256=sha256, timestamp=timestamp)


class DbCleanupTest(unittest.TestCase):
    """#68: the cleanup survives query jobs without a result, deletes what is older than the TTL,
    and removes the query data nothing refers to any more."""

    def _worker(self, jobs, results, query_samples, compact=False):
        queue = MagicMock()
        queue.clean_interval = 10**9
        storage = MagicMock()
        storage.getSamples.return_value = query_samples
        storage.deleteSample.return_value = True
        storage.deleteOrphanedQueryData.return_value = {"query_functions": 2, "query_xcfg": 1}
        storage.compactQueryCollections.return_value = {"query_functions": {"ok": 1.0}}
        worker = Worker(queue=queue, config=config, storage=storage)
        worker._storage_config = SimpleNamespace(STORAGE_MONGODB_CLEANUP_TTL=TTL, STORAGE_MONGODB_COMPACT_AFTER_CLEANUP=compact)
        setattr(
            worker, "getQueueData", lambda start, limit, method=None, state=None, **kwargs: [job for job in jobs if job["payload"]["method"] == method and job["state"] == state]
        )
        setattr(worker, "getResultForJob", lambda job_id: results.get(job_id))
        return worker, queue, storage

    def test_a_failed_job_without_a_result_is_deleted_when_old_and_kept_when_recent(self):
        jobs = [
            {**_job("old-failed", "getMatchesForUnmappedBinary", started_at=OLD), "state": "failed"},
            {**_job("new-failed", "getMatchesForMappedBinary", started_at=RECENT), "state": "failed"},
            {**_job("never-started", "getMatchesForSmdaReport"), "state": "failed"},
        ]
        worker, queue, storage = self._worker(jobs, {}, [])
        report = worker.doDbCleanup()
        self.assertEqual(sorted(["old-failed", "never-started"]), sorted(call.args[0] for call in queue.delete_job.call_args_list))
        self.assertEqual(2, report["num_query_jobs_deleted"])
        self.assertEqual(0, report["num_query_samples_deleted"])

    def test_a_recent_job_protects_its_sample_and_an_old_one_takes_it_along(self):
        report = SmdaReport.fromFile(EXAMPLE_REPORT)
        assert report is not None
        report.sha256 = 64 * "a"
        sample_dict = SampleEntry(report, sample_id=-5, family_id=0).toDict()
        jobs = [
            {**_job("old-finished", "getMatchesForUnmappedBinary", started_at=OLD, finished_at=OLD), "state": "finished"},
            {**_job("new-finished", "getMatchesForUnmappedBinary", started_at=RECENT, finished_at=RECENT), "state": "finished"},
        ]
        results = {"old-finished": {"info": {"sample": {**sample_dict, "sample_id": -7, "sha256": 64 * "b"}}}, "new-finished": {"info": {"sample": sample_dict}}}
        query_samples = [_sample(-5, 64 * "a", OLD), _sample(-7, 64 * "b", OLD), _sample(-9, 64 * "c", RECENT)]
        worker, queue, storage = self._worker(jobs, results, query_samples)
        report = worker.doDbCleanup()
        # -5 is old but its job is recent: protected; -7 is old and its job is old: deleted with the job; -9 is recent: kept
        self.assertEqual([-7], sorted(call.args[0] for call in storage.deleteSample.call_args_list))
        self.assertEqual(["old-finished"], [call.args[0] for call in queue.delete_job.call_args_list])
        self.assertEqual(1, report["num_query_samples_deleted"])

    def test_orphaned_query_data_is_removed_and_compaction_is_opt_in(self):
        worker, queue, storage = self._worker([], {}, [])
        report = worker.doDbCleanup()
        storage.deleteOrphanedQueryData.assert_called_once_with()
        self.assertEqual({"query_functions": 2, "query_xcfg": 1}, report["orphans"])
        storage.compactQueryCollections.assert_not_called()
        self.assertNotIn("compacted", report)
        worker, queue, storage = self._worker([], {}, [], compact=True)
        report = worker.doDbCleanup()
        storage.compactQueryCollections.assert_called_once_with()
        self.assertEqual({"query_functions": {"ok": 1.0}}, report["compacted"])


class LocalQueueDeletesAJobWithoutAResultTest(unittest.TestCase):
    def test_a_job_that_left_no_result_is_deleted(self):
        """The cleanup deletes old query jobs that failed before matching; in the memory queue
        deleting one looked its missing result up as a file and raised a TypeError (#68)."""
        queue = LocalQueue()
        queue._jobs["failed-job"] = {"_id": "failed-job", "result": None, "payload": {"method": "getMatchesForUnmappedBinary", "file_params": "{}"}}
        self.assertEqual(1, queue.delete_job("failed-job"))
        self.assertNotIn("failed-job", queue._jobs)


if __name__ == "__main__":
    unittest.main()
