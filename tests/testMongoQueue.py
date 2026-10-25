import json
import logging
import os
import time
import unittest
import unittest.mock
from datetime import datetime, timedelta
from typing import List, cast
from unittest import TestCase

import pymongo
import pytest

from mcrit.config.QueueConfig import QueueConfig
from mcrit.libs.mongoqueue import MongoQueue, _utcNow
from mcrit.queue.QueueRemoteCalls import _createJobPayload, get_descriptor, rearrange_params

from .context import getTestMongoServerAndPort

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


def _payload(method, *params, results_version=None, **kwparams):
    """A job payload built the way QueueRemoteCalls.remote_call_function builds one for a
    real call, e.g. _payload("getMatchesForSampleVs", 8, 12, band_matches_required=2)."""
    parsed_params, file_params = rearrange_params(list(params), dict(kwparams), [], [])
    descriptor = get_descriptor(method, parsed_params, {}, results_version=results_version)
    return _createJobPayload(method, parsed_params, file_params, descriptor)


@pytest.mark.mongo
class MongoQueueTest(TestCase):
    def setUp(self):
        self.client = pymongo.MongoClient(os.environ.get("TEST_MONGODB"))
        queue_config = QueueConfig()
        queue_config.QUEUE_SERVER, queue_config.QUEUE_PORT = getTestMongoServerAndPort()
        queue_config.QUEUE_MONGODB_DBNAME = "test_queue"
        queue_config.QUEUE_MONGODB_COLLECTION_NAME = "queue_1"
        self.queue_config = queue_config
        self.queue = MongoQueue(queue_config, "consumer_1")

    def tearDown(self):
        self.client.drop_database("test_queue")

    def assert_job_equal(self, job, data):
        for k, v in data.items():
            self.assertEqual(job.payload[k], v)

    def test_put_next(self):
        data = {"method": "test_method", "context_id": "alpha", "data": [1, 2, 3], "more-data": time.time()}
        self.queue.put(dict(data))
        job = self.queue.next()
        self.assert_job_equal(job, data)

    def test_an_unknown_file_id_has_no_metadata_and_no_content(self):
        unknown = "0" * 24
        self.assertIsNone(self.queue._grid_to_meta(unknown))
        self.assertIsNone(self.queue._grid_to_file(unknown))
        self.assertIsNone(self.queue._grid_to_dicts(unknown))
        known = self.queue._dicts_to_grid({"a": 1}, metadata={"result": True, "job": unknown})
        self.assertEqual({"result": True, "job": unknown}, self.queue._grid_to_meta(known))

    def test_get_empty_queue(self):
        job = self.queue.next()
        self.assertEqual(job, None)

    def test_priority(self):
        self.queue.put({"method": "test_method", "name": "alice"}, priority=1)
        self.queue.put({"method": "test_method", "name": "bob"}, priority=2)
        self.queue.put({"method": "test_method", "name": "mike"}, priority=0)

        self.assertEqual(
            ["bob", "alice", "mike"],
            [self.queue.next().payload["name"], self.queue.next().payload["name"], self.queue.next().payload["name"]],
        )

    def test_complete(self):
        data = {"method": "test_method", "context_id": "alpha", "data": [1, 2, 3], "more-data": datetime.now()}

        self.queue.put(data)
        self.assertEqual(self.queue.size(), 1)
        job = self.queue.next()
        job.complete()
        self.assertEqual(self.queue.size(), 0)

    def test_repair_uses_seconds_not_days(self):
        # timedelta()'s first positional argument is days, so the seconds-denominated
        # timeout (QUEUE_TIMEOUT, default 300) used to describe 300 DAYS and repair()
        # could never reclaim anything
        data = {"method": "test_method", "context_id": "alpha", "data": [1]}
        self.queue.put(data)
        job = self.queue.next()
        self.assertIsNotNone(job.job_id)
        # age the lock past the timeout without touching anything else
        stale_locked_at = datetime.now() - timedelta(seconds=self.queue.timeout + 60)
        collection = self.queue._getCollection()
        assert collection is not None
        collection.update_one({"_id": job.job_id}, {"$set": {"locked_at": stale_locked_at}})
        self.queue.repair()
        repaired = collection.find_one({"_id": job.job_id})
        assert repaired is not None
        self.assertIsNone(repaired["locked_by"])
        self.assertIsNone(repaired["locked_at"])
        self.assertEqual(repaired["attempts_left"], self.queue.max_attempts - 1)
        # and the job is claimable again
        self.assertIsNotNone(self.queue.next().job_id)

    def test_repair_leaves_fresh_locks_alone(self):
        data = {"method": "test_method", "context_id": "alpha", "data": [1]}
        self.queue.put(data)
        job = self.queue.next()
        self.queue.repair()
        collection = self.queue._getCollection()
        assert collection is not None
        untouched = collection.find_one({"_id": job.job_id})
        assert untouched is not None
        self.assertEqual(untouched["locked_by"], self.queue.consumer_id)
        self.assertIsNotNone(untouched["locked_at"])
        self.assertEqual(untouched["attempts_left"], self.queue.max_attempts)

    def test_progressor_does_not_resurrect_released_lock(self):
        # regression test for the half-locked starvation found while reproducing #106:
        # a progress heartbeat racing a concurrent release must not write locked_at back
        # onto a job whose locked_by was just cleared - the job would otherwise satisfy
        # neither "claimable" nor "locked" and starve forever
        data = {"method": "test_method", "context_id": "alpha", "data": [1]}
        self.queue.put(data)
        job = self.queue.next()
        job.release()
        job.progressor(count=0.5)
        collection = self.queue._getCollection()
        assert collection is not None
        document = collection.find_one({"_id": job.job_id})
        assert document is not None
        self.assertIsNone(document["locked_by"])
        self.assertEqual(document["progress"], 0.5)
        reclaimed = self.queue.next()
        self.assertIsNotNone(reclaimed.job_id)

    def test_next_claims_half_released_job(self):
        # jobs left half-released by older versions (locked_by None, locked_at set) must
        # remain claimable: locked_by is the authoritative lock, locked_at its heartbeat
        data = {"method": "test_method", "context_id": "alpha", "data": [1]}
        self.queue.put(data)
        job = self.queue.next()
        collection = self.queue._getCollection()
        assert collection is not None
        collection.update_one({"_id": job.job_id}, {"$set": {"locked_by": None}})
        reclaimed = self.queue.next()
        self.assertIsNotNone(reclaimed.job_id)
        self.assertEqual(reclaimed.job_id, job.job_id)

    def test_release(self):
        data = {"method": "test_method", "context_id": "alpha", "data": [1, 2, 3], "more-data": time.time()}

        self.queue.put(data)
        job = self.queue.next()
        job.release()
        self.assertEqual(self.queue.size(), 1)
        job = self.queue.next()
        self.assert_job_equal(job, data)

    def test_error(self):
        pass

    def test_progress(self):
        pass

    def test_stats(self):

        for i in range(5):
            data = {"method": "test_method", "context_id": "alpha", "data": [1, 2, 3], "more-data": time.time()}
            self.queue.put(data)
        job = self.queue.next()
        job.error("problem")
        stats = self.queue.stats()
        self.assertEqual({"available": 5, "total": 5, "locked": 0, "errors": 0}, stats)

    def test_ensure_indices(self):
        collection = self.queue._getCollection()
        assert collection is not None
        index_information = collection.index_information()
        self.assertIn("locked_by_1_finished_at_1_priority_-1_created_at_1", index_information)
        self.assertEqual(
            [("locked_by", 1), ("finished_at", 1), ("priority", -1), ("created_at", 1)],
            index_information["locked_by_1_finished_at_1_priority_-1_created_at_1"]["key"],
        )

    def test_jobs_in_progress(self):
        self.queue.put({"method": "test_method", "name": "alice"})
        self.queue.put({"method": "test_method", "name": "bob"})
        self.assertEqual(0, len(list(self.queue._jobs_in_progress())))
        job = self.queue.next()
        jobs_in_progress = list(self.queue._jobs_in_progress())
        self.assertEqual(1, len(jobs_in_progress))
        self.assertEqual(job.job_id, jobs_in_progress[0]["_id"])

    def test_cached_job_prefers_finished_over_running(self):
        # fkie-cad/mcritweb#47: a force rematch that is still running must not shadow the finished job whose
        # result is what a repeated request can actually use
        payload = {"method": "test_method", "descriptor": "same-request"}
        finished_id = self.queue.put(dict(payload))
        self.queue.next().complete("result-1")
        running_id = self.queue.put(dict(payload))
        self.queue.next()  # locked by the consumer, in progress
        self.assertEqual(finished_id, self.queue.get_cached_job_id(payload))
        # once the newer job is finished as well, the newest finished result wins
        self.queue.get_job(running_id).complete("result-2")
        self.assertEqual(running_id, self.queue.get_cached_job_id(payload))

    def test_cached_job_ignores_failed_and_terminated(self):
        payload = {"method": "test_method", "descriptor": "same-request"}
        self.queue.max_attempts = 1
        self.queue._default_insert["attempts_left"] = 1
        failed_id = self.queue.put(dict(payload))
        self.queue.next().error("boom")
        self.assertEqual(0, self.queue.get_job(failed_id).attempts_left)
        self.assertIsNone(self.queue.get_cached_job_id(payload))
        terminated_id = self.queue.put(dict(payload))
        self.queue.next().terminate()
        self.assertTrue(self.queue.get_job(terminated_id)._is_terminated())
        self.assertIsNone(self.queue.get_cached_job_id(payload))
        queued_id = self.queue.put(dict(payload))
        self.assertEqual(queued_id, self.queue.get_cached_job_id(payload))

    def test_cached_job_ignores_one_marked_uncacheable(self):
        """A result that depended on state outside its arguments (UncacheableResult, #217) answers no later request."""
        payload = {"method": "test_method", "descriptor": "same-request"}
        cached_id = self.queue.put(dict(payload))
        self.queue.next().complete("result-1")
        uncacheable_id = self.queue.put(dict(payload))
        job = self.queue.next()
        job.mark_uncacheable()
        job.complete("result-2")
        # the newer finished job would win, were it not marked
        self.assertEqual(cached_id, self.queue.get_cached_job_id(payload))
        self.assertIs(False, self.queue._getCollection().find_one({"_id": uncacheable_id})["cacheable"])

    def test_put_records_username(self):
        with_user = self.queue.put({"method": "test_method"}, username="alice")
        without_user = self.queue.put({"method": "test_method"})
        self.assertEqual("alice", self.queue.get_job(with_user).username)
        self.assertIsNone(self.queue.get_job(without_user).username)
        # a document from before the field existed
        self.queue._getCollection().update_one({"_id": without_user}, {"$unset": {"username": ""}})
        self.assertIsNone(self.queue.get_job(without_user).username)

    def test_state_queries_agree_with_identify_job_state(self):
        # fkie-cad/mcritweb#57: the per-state queries replace a per-document classification in Python; over
        # every combination of the fields that classification reads, each document must be
        # selected by exactly the query of its state
        from itertools import product

        now = datetime.now()
        documents = []
        for started_at, locked_by, finished_at, terminated, attempts_left in product((None, now), (None, "worker"), (None, now), (False, True), (0, 1)):
            document = dict(self.queue._default_insert)
            document.update(
                {
                    "payload": {"method": "test_method", "params": "{}", "descriptor": "d"},
                    "created_at": now,
                    "started_at": started_at,
                    "locked_by": locked_by,
                    "finished_at": finished_at,
                    "terminated": terminated,
                    "attempts_left": attempts_left,
                }
            )
            documents.append(document)
        self.queue._getCollection().insert_many(documents)
        self.assertEqual(32, self.queue.get_job_count())
        # a document no branch claims (e.g. locked but never started) is "unknown" in Python
        # and is selected by no state query either
        unknown = {doc["_id"] for doc in documents if self.queue._identifyJobState(doc) == "unknown"}
        seen = set()
        for state in ("in_progress", "failed", "queued", "finished", "terminated"):
            expected = {doc["_id"] for doc in documents if self.queue._identifyJobState(doc) == state}
            selected = {job.job_id for job in self.queue.get_jobs(0, 0, state=state)}
            self.assertEqual(expected, selected, state)
            self.assertEqual(len(expected), self.queue.get_job_count(state=state), state)
            self.assertTrue(seen.isdisjoint(selected), state)
            seen |= selected
        self.assertEqual(32 - len(unknown), len(seen))
        self.assertTrue(seen.isdisjoint(unknown))

    def test_filter_and_username_select_before_paging(self):
        for i in range(5):
            self.queue.put({"method": "test_method", "params": '{"0": "apple %d"}' % i, "descriptor": "a%d" % i}, username="alice")
            self.queue.put({"method": "other_method", "params": '{"0": "pear %d"}' % i, "descriptor": "p%d" % i}, username="bob")
        self.assertEqual(5, self.queue.get_job_count(filter="Apple"))
        self.assertEqual(5, self.queue.get_job_count(filter="other_"))
        self.assertEqual(5, self.queue.get_job_count(username="bob"))
        self.assertEqual(0, self.queue.get_job_count(filter="apple", username="bob"))
        self.assertEqual(10, self.queue.get_job_count(state="queued"))
        self.assertEqual(["apple 4", "apple 3", "apple 2"], [json.loads(job.payload["params"])["0"] for job in self.queue.get_jobs(0, 3, filter="apple")])
        self.assertEqual(["apple 1", "apple 0"], [json.loads(job.payload["params"])["0"] for job in self.queue.get_jobs(3, 3, filter="apple")])
        self.assertEqual(["apple 0", "apple 1"], [json.loads(job.payload["params"])["0"] for job in self.queue.get_jobs(0, 2, filter="apple", ascending=True)])
        # a regex special character in the filter is literal
        self.assertEqual(0, self.queue.get_job_count(filter="apple.*"))

    def test_context_manager_error(self):
        self.queue.put({"method": "test_method", "context_id": "alpha", "data": [1, 2, 3], "more-data": time.time()})
        job = self.queue.next()
        try:
            with job as data:
                self.assertEqual(data["payload"]["method"], "test_method")
                # Item is returned to the queue on error
                raise SyntaxError
        except SyntaxError:
            pass

        job = self.queue.next()
        self.assertEqual(job.attempts_left, self.queue.max_attempts - 1)

    def _counters(self):
        return {c["name"]: c for c in self.queue._getQueueCounters().find({"name": "test_method"}, {"_id": 0})}.get("test_method", {})

    def test_delete_jobs_applies_every_date_filter(self):
        """finished_before was dropped whenever created_before was given too, which deleted queued and running jobs as well."""
        self.queue.put({"method": "test_method", "descriptor": "finished"})
        self.queue.next().complete()
        queued_id = self.queue.put({"method": "test_method", "descriptor": "queued"})
        running_id = self.queue.put({"method": "test_method", "descriptor": "running"})
        self.queue.next()
        later = datetime.now() + timedelta(days=1)
        self.assertEqual(1, self.queue.delete_jobs(created_before=later, finished_before=later))
        self.assertEqual({queued_id, running_id}, {job.job_id for job in self.queue.get_jobs(0, 0)})
        self.assertEqual(0, self.queue.delete_jobs(method="other_method", created_before=later, finished_before=later))
        self.assertEqual(2, self.queue.delete_jobs(method="test_method", created_before=later))

    def test_deleting_a_job_its_dependent_waits_for_lets_the_dependent_run(self):
        """A cross compare's combine job stayed queued forever once a child was deleted before it finished."""
        for delete in (lambda job_id: self.queue.delete_job(job_id), lambda job_id: self.queue.delete_jobs(method="child_method")):
            child_id = self.queue.put({"method": "child_method", "descriptor": "child"}, priority=-1)
            combine_id = self.queue.put({"method": "test_method", "descriptor": "combine"}, await_jobs=[str(child_id)])
            self.assertEqual([str(child_id)], self.queue._getCollection().find_one({"_id": combine_id})["unfinished_dependencies"])
            self.assertEqual(1, delete(child_id))
            claimed = self.queue.next()
            assert claimed is not None
            self.assertEqual(combine_id, claimed.job_id)
            claimed.complete()
        # a finished child already let its dependents go, and deleting it changes nothing for them
        child_id = self.queue.put({"method": "child_method", "descriptor": "child"})
        self.queue.next().complete()
        waiting_id = self.queue.put({"method": "test_method", "descriptor": "later"}, await_jobs=[str(child_id)])
        self.queue.delete_job(child_id)
        self.assertEqual(waiting_id, self.queue.next().job_id)

    def test_a_released_job_ignores_what_its_former_holder_writes_late(self):
        """complete(), error() and release() took effect on whatever the job had become since."""
        other = MongoQueue(self.queue_config, "consumer_2")
        self.queue.put({"method": "test_method", "descriptor": "d"})
        late = self.queue.next()
        late.release()
        current = other.next()
        assert current is not None
        self.assertEqual(late.job_id, current.job_id)
        late.error("late")
        late.release()
        self.assertIsNone(late.complete("late result"))
        document = self.queue._getCollection().find_one({"_id": late.job_id})
        self.assertEqual(
            ("consumer_2", None, None, self.queue.max_attempts - 1), (document["locked_by"], document["finished_at"], document["last_error"], document["attempts_left"])
        )
        current.complete("result")
        document = self.queue._getCollection().find_one({"_id": late.job_id})
        self.assertEqual("result", document["result"])
        self.assertEqual((0, 0, 1), (self._counters()["in_progress"], self._counters()["queued"], self._counters()["finished"]))

    def test_a_job_process_and_the_worker_that_spawned_it_count_one_attempt(self):
        """A SpawningWorker's job process errors the job, and the worker errored it again: two attempts
        per failure, a job ending at attempts_left -1 that counted as queued forever, and in between a
        released job the worker's second error() unlocked under whoever had taken it meanwhile."""
        self.queue.put({"method": "test_method", "descriptor": "d"})
        worker_side = self.queue.next()
        for attempt in range(1, self.queue.max_attempts + 1):
            job_process_side = self.queue.get_job(worker_side.job_id)
            job_process_side.error(f"failed in the job process {attempt}")
            worker_side.error("child worker exited without producing a result_id")
            document = self.queue._getCollection().find_one({"_id": worker_side.job_id})
            self.assertEqual((self.queue.max_attempts - attempt, f"failed in the job process {attempt}"), (document["attempts_left"], document["last_error"]))
            worker_side = self.queue.next()
            if attempt < self.queue.max_attempts:
                self.assertEqual(document["_id"], worker_side.job_id)
        self.assertIsNone(worker_side)
        self.assertEqual("failed", self.queue._identifyJobState(document))
        self.assertEqual((0, 0, 1), (self._counters()["in_progress"], self._counters()["queued"], self._counters()["failed"]))

    def test_a_job_finished_by_its_job_process_is_counted_once(self):
        self.queue.put({"method": "test_method", "descriptor": "d"})
        worker_side = self.queue.next()
        self.queue.get_job(worker_side.job_id).complete("result")
        worker_side.result = "result"
        # the usual way a spawning worker's job ends: nothing to warn about
        with unittest.mock.patch("mcrit.libs.mongoqueue.LOGGER.warning") as warning:
            worker_side.complete()
        warning.assert_not_called()
        self.assertEqual((0, 1), (self._counters()["in_progress"], self._counters()["finished"]))

    def test_a_job_process_that_died_before_notifying_has_its_dependents_released(self):
        """The job process finished the job and died before telling the jobs that wait on it; the
        spawning worker's own completion used to do that a second time, and still does."""
        child_id = self.queue.put({"method": "test_method", "descriptor": "child"}, priority=1)
        combine_id = self.queue.put({"method": "test_method", "descriptor": "combine"}, await_jobs=[str(child_id)])
        worker_side = self.queue.next()
        self.assertEqual(child_id, worker_side.job_id)
        job_process_side = self.queue.get_job(child_id)
        with unittest.mock.patch.object(self.queue, "_notify_dependent_jobs", side_effect=RuntimeError("died")):
            with self.assertRaises(RuntimeError):
                job_process_side.complete()
        self.assertIsNone(self.queue.next())
        worker_side.complete()
        self.assertEqual(combine_id, self.queue.next().job_id)

    def test_clear_leaves_the_files_of_other_queues_alone(self):
        other_config = QueueConfig()
        other_config.QUEUE_SERVER, other_config.QUEUE_PORT = self.queue_config.QUEUE_SERVER, self.queue_config.QUEUE_PORT
        other_config.QUEUE_MONGODB_DBNAME = self.queue_config.QUEUE_MONGODB_DBNAME
        other_config.QUEUE_MONGODB_COLLECTION_NAME = "queue_2"
        other = MongoQueue(other_config, "consumer_2")
        other_job_id = other.put({"method": "test_method", "descriptor": "d"})
        other_result = other._dicts_to_grid({"result": 1}, metadata={"result": True, "job": str(other_job_id)})
        own_job_id = self.queue.put({"method": "test_method", "descriptor": "d"})
        own_result = self.queue._dicts_to_grid({"result": 2}, metadata={"result": True, "job": str(own_job_id)})
        self.queue.clear()
        self.assertEqual({"result": 1}, other._grid_to_dicts(other_result))
        self.assertIsNone(self.queue._grid_to_dicts(own_result))

    def test_clear_drops_the_files_and_results_in_gridfs(self):
        """clear() dropped a collection named "fs", which GridFS doesn't use: POST /respawn left every result and query binary behind."""
        self.queue._file_to_grid(b"binary", metadata={"jobs": [], "tmp_lock": 0, "sha256": "a"})
        self.queue._dicts_to_grid({"result": 1}, metadata={"result": True, "job": "0" * 24})
        database = self.queue._getCollection().database
        self.assertEqual((2, 2), (database["fs.files"].count_documents({}), database["fs.chunks"].count_documents({})))
        self.queue.clear()
        self.assertEqual((0, 0), (database["fs.files"].count_documents({}), database["fs.chunks"].count_documents({})))
        # and the queue keeps working, files included
        self.assertIn("metadata.sha256_1", database["fs.files"].index_information())
        file_id = self.queue._file_to_grid(b"again")
        self.assertEqual(b"again", self.queue._grid_to_file(file_id, results_only=False))

    def test_context_manager_complete(self):
        self.queue.put({"method": "test_method", "context_id": "alpha", "data": [1, 2, 3], "more-data": time.time()})
        job = self.queue.next()
        with job as data:
            self.assertEqual(data["payload"]["method"], "test_method")
        job = self.queue.next()
        self.assertEqual(job, None)


@pytest.mark.mongo
class WorkerLivenessTest(TestCase):
    """Jobs held by a worker that died without unwinding are reclaimed, and never served
    as a cached result to an identical resubmission (#150)."""

    def setUp(self):
        self.client = pymongo.MongoClient(os.environ.get("TEST_MONGODB"))
        self.queue_config = QueueConfig()
        self.queue_config.QUEUE_SERVER, self.queue_config.QUEUE_PORT = getTestMongoServerAndPort()
        self.queue_config.QUEUE_MONGODB_DBNAME = "test_queue"
        self.queue_config.QUEUE_MONGODB_COLLECTION_NAME = "queue_liveness"
        self.timeout = 5
        self.worker_a = self._worker("Worker-a")
        self.worker_b = self._worker("Worker-b")
        self.server = self._worker("index")

    def tearDown(self):
        for queue in (self.worker_a, self.worker_b, self.server):
            queue._stopHeartbeatThread()
        self.client.drop_database("test_queue")

    def _worker(self, consumer_id):
        return MongoQueue(self.queue_config, consumer_id, timeout=self.timeout)

    def _registration(self):
        return self.worker_a._getQueueCounters().find_one({"name": "workers"}, {"_id": 0}) or {}

    def _document(self, job_id):
        document = self.worker_a._getCollection().find_one({"_id": job_id})
        assert document is not None
        return document

    def _age_heartbeat(self, consumer_id, seconds):
        age = timedelta(seconds=seconds)
        self.worker_a._getQueueCounters().update_one(
            {"name": "workers"}, {"$set": {f"heartbeats.{consumer_id}": datetime.now() - age, f"heartbeats_utc.{consumer_id}": _utcNow() - age}}
        )

    def _claimed_by(self, queue, descriptor="d"):
        queue.put({"method": "test_method", "descriptor": descriptor})
        job = queue.next()
        assert job is not None
        return job

    def test_registering_records_a_heartbeat_and_unregistering_removes_it(self):
        self.worker_a._getCollection()
        registration = self._registration()
        self.assertIn("Worker-a", registration["workers"])
        self.assertIsInstance(registration["heartbeats"]["Worker-a"], datetime)
        self.server._getCollection()
        self.assertNotIn("index", self._registration()["workers"])
        self.worker_a.unregisterWorker()
        registration = self._registration()
        self.assertNotIn("Worker-a", registration["workers"])
        self.assertNotIn("Worker-a", registration.get("heartbeats", {}))

    def test_a_worker_is_live_only_with_a_fresh_heartbeat(self):
        self.worker_a._getCollection()
        self.worker_b._getCollection()
        self.assertEqual({"Worker-a", "Worker-b"}, self.server._live_worker_ids())
        self._age_heartbeat("Worker-b", self.timeout + 1)
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids())
        # a registration without any heartbeat is a process from before heartbeats existed
        # (a rolling upgrade): it is stamped now and gets one timeout of grace before it is
        # judged like everybody else
        self.worker_a._getQueueCounters().update_one({"name": "workers"}, {"$unset": {"heartbeats.Worker-a": "", "heartbeats_utc.Worker-a": ""}})
        self.worker_a._stopHeartbeatThread()
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids())
        self.assertIsInstance(self._registration()["heartbeats"]["Worker-a"], datetime)
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids(now=_utcNow() + timedelta(seconds=self.timeout - 1)))
        self.assertEqual(set(), self.server._live_worker_ids(now=_utcNow() + timedelta(seconds=self.timeout + 1)))

    def test_the_heartbeat_keeps_going_while_a_job_runs(self):
        """jobs run synchronously in the poll loop, so a job longer than the timeout would look
        like a dead worker without the heartbeat thread"""
        self.worker_a._getCollection()
        self.assertTrue(self.worker_a._heartbeat_thread is not None and self.worker_a._heartbeat_thread.is_alive())
        self._age_heartbeat("Worker-a", self.timeout + 1)
        self.assertEqual(set(), self.server._live_worker_ids())
        # no poll happens here - the thread alone brings the heartbeat back within one interval
        time.sleep(self.worker_a.heartbeat_interval + 1)
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids())
        # a worker judged dead by mistake was unregistered by the reclaim; its next heartbeat
        # registers it again, so it keeps serving jobs
        self.server.release_orphaned_jobs()
        self._age_heartbeat("Worker-a", self.timeout + 1)
        self.server.release_orphaned_jobs()
        self.assertNotIn("Worker-a", self._registration().get("workers", []))
        time.sleep(self.worker_a.heartbeat_interval + 1)
        self.assertIn("Worker-a", self._registration()["workers"])
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids())
        self.worker_a.unregisterWorker()
        self.assertIsNone(self.worker_a._heartbeat_thread)

    def test_a_dead_workers_job_is_reclaimed_and_a_live_ones_is_kept(self):
        dead_job = self._claimed_by(self.worker_a, "dead")
        live_job = self._claimed_by(self.worker_b, "live")
        self._age_heartbeat("Worker-a", self.timeout + 1)  # Worker-a was SIGKILLed: still registered, no heartbeat
        self.server.release_orphaned_jobs()
        reclaimed = self._document(dead_job.job_id)
        self.assertIsNone(reclaimed["locked_by"])
        self.assertIsNone(reclaimed["locked_at"])
        self.assertEqual(self.worker_a.max_attempts - 1, reclaimed["attempts_left"])
        kept = self._document(live_job.job_id)
        self.assertEqual("Worker-b", kept["locked_by"])
        self.assertEqual(self.worker_b.max_attempts, kept["attempts_left"])
        # the dead registration is gone, the live one stays
        registration = self._registration()
        self.assertEqual(["Worker-b"], registration["workers"])
        self.assertEqual(["Worker-b"], list(registration["heartbeats"]))
        # and another worker picks the reclaimed job up
        self.assertEqual(dead_job.job_id, self.worker_b.next().job_id)

    def test_a_job_never_registered_for_is_reclaimed_too(self):
        job = self._claimed_by(self.worker_a)
        self.worker_a.unregisterWorker()
        self.server.release_orphaned_jobs()
        self.assertIsNone(self._document(job.job_id)["locked_by"])

    def test_reclaiming_the_last_attempt_fails_the_job_and_frees_its_dependents(self):
        job = self._claimed_by(self.worker_a, "last")
        self.worker_a._getCollection().update_one({"_id": job.job_id}, {"$set": {"attempts_left": 1}})
        self.worker_b.put({"method": "test_method", "descriptor": "waiting"}, await_jobs=[str(job.job_id)])
        self._age_heartbeat("Worker-a", self.timeout + 1)
        self.server.release_orphaned_jobs()
        failed = self._document(job.job_id)
        self.assertEqual(0, failed["attempts_left"])
        self.assertIsNone(failed["locked_by"])
        waiting = self.worker_b.next()
        assert waiting is not None
        self.assertEqual("waiting", waiting.payload["descriptor"])
        counters = {c["name"]: c for c in self.worker_a._getQueueCounters().find({"name": "test_method"})}
        self.assertEqual(1, counters["test_method"]["failed"])

    def _counters(self):
        return {c["name"]: c for c in self.worker_a._getQueueCounters().find({"name": "test_method"})}["test_method"]

    def test_releasing_all_jobs_moves_a_held_job_from_in_progress_to_queued(self):
        job = self._claimed_by(self.worker_a, "held")
        before = self._counters()
        self.assertEqual(1, before["in_progress"])
        self.assertEqual(0, before["queued"])
        self.worker_a.release_all_jobs()
        released = self._document(job.job_id)
        self.assertIsNone(released["locked_by"])
        self.assertEqual(self.worker_a.max_attempts - 1, released["attempts_left"])
        counters = self._counters()
        self.assertEqual(0, counters["in_progress"])
        self.assertEqual(1, counters["queued"])

    def test_releasing_all_jobs_on_the_last_attempt_fails_the_job_and_frees_its_dependents(self):
        job = self._claimed_by(self.worker_a, "last")
        self.worker_a._getCollection().update_one({"_id": job.job_id}, {"$set": {"attempts_left": 1}})
        self.worker_b.put({"method": "test_method", "descriptor": "waiting"}, await_jobs=[str(job.job_id)])
        self.worker_a.release_all_jobs()
        failed = self._document(job.job_id)
        self.assertEqual(0, failed["attempts_left"])
        self.assertIsNone(failed["locked_by"])
        counters = self._counters()
        self.assertEqual(1, counters["failed"])
        self.assertEqual(0, counters["in_progress"])
        waiting = self.worker_b.next()
        assert waiting is not None
        self.assertEqual("waiting", waiting.payload["descriptor"])

    def test_releasing_all_jobs_leaves_other_consumers_and_finished_jobs_alone(self):
        finished = self._claimed_by(self.worker_a, "finished")
        finished.complete()
        held_by_b = self._claimed_by(self.worker_b, "theirs")
        before = self._counters()
        self.worker_a.release_all_jobs()
        self.assertEqual(self.worker_a.consumer_id, self._document(finished.job_id)["locked_by"])
        self.assertEqual(self.worker_b.consumer_id, self._document(held_by_b.job_id)["locked_by"])
        self.assertEqual(self.worker_a.max_attempts, self._document(held_by_b.job_id)["attempts_left"])
        self.assertEqual(before, self._counters())

    def test_a_stranded_job_is_not_served_as_a_cached_result(self):
        job = self._claimed_by(self.worker_a, "shared")
        payload = {"descriptor": "shared"}
        # in flight on a live worker: served
        self.assertEqual(job.job_id, self.server.get_cached_job_id(payload))
        self._age_heartbeat("Worker-a", self.timeout + 1)
        # the same job, its worker dead: not served
        self.assertIsNone(self.server.get_cached_job_id(payload))
        # once reclaimed it waits in the queue, and a waiting job is served again
        self.server.release_orphaned_jobs()
        self.assertEqual(job.job_id, self.server.get_cached_job_id(payload))
        # a finished job is served whatever its worker's state
        self.worker_b.next().complete()
        self._age_heartbeat("Worker-b", self.timeout + 1)
        self.assertEqual(job.job_id, self.server.get_cached_job_id(payload))

    def _in_time_zone(self, zone):
        previous = os.environ.get("TZ")
        os.environ["TZ"] = zone
        time.tzset()

        def restore():
            if previous is None:
                os.environ.pop("TZ", None)
            else:
                os.environ["TZ"] = previous
            time.tzset()

        self.addCleanup(restore)

    def test_heartbeats_from_another_time_zone_are_judged_by_their_age(self):
        """Heartbeats were compared in local time: a worker west of the server looked dead and lost its
        jobs, one east of it looked alive hours after it died, and so did every worker over a DST change."""
        self._in_time_zone("UTC+10")  # POSIX: ten hours west of UTC
        self.worker_a._getCollection()
        self.worker_a._stopHeartbeatThread()  # so that only the aging below moves the heartbeat
        self._in_time_zone("UTC-10")  # the server, ten hours east of UTC
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids())
        self._age_heartbeat("Worker-a", self.timeout + 1)
        self.assertEqual(set(), self.server._live_worker_ids())

    def test_a_worker_of_an_older_release_is_judged_by_its_local_heartbeat(self):
        """Such a worker writes only the local-time heartbeat; it is compared as before, not mistaken for one without any."""
        self.worker_a._getCollection()
        self.worker_a._stopHeartbeatThread()
        counters = self.worker_a._getQueueCounters()
        counters.update_one({"name": "workers"}, {"$unset": {"heartbeats_utc.Worker-a": ""}})
        self._in_time_zone("UTC-10")
        counters.update_one({"name": "workers"}, {"$set": {"heartbeats.Worker-a": datetime.now()}})
        self.assertEqual({"Worker-a"}, self.server._live_worker_ids())
        counters.update_one({"name": "workers"}, {"$set": {"heartbeats.Worker-a": datetime.now() - timedelta(seconds=self.timeout + 1)}})
        self.assertEqual(set(), self.server._live_worker_ids())
        self.assertNotIn("Worker-a", self._registration().get("heartbeats_utc", {}))
        # a dead registration goes with both of its heartbeats
        self._age_heartbeat("Worker-a", self.timeout + 1)
        self.server.release_orphaned_jobs()
        registration = self._registration()
        self.assertNotIn("Worker-a", registration.get("heartbeats", {}))
        self.assertNotIn("Worker-a", registration.get("heartbeats_utc", {}))

    def test_a_utc_heartbeat_an_older_release_left_behind_is_dropped(self):
        """An older release drops only the local heartbeat of a dead worker."""
        self.worker_a._getQueueCounters().update_one({"name": "workers"}, {"$set": {"heartbeats_utc.Worker-gone": _utcNow()}}, upsert=True)
        self.server.release_orphaned_jobs()
        self.assertNotIn("Worker-gone", self._registration().get("heartbeats_utc", {}))

    def test_polling_refreshes_the_heartbeat_and_reclaims_periodically(self):
        stranded = self._claimed_by(self.worker_a)
        self.worker_b._getCollection()  # registering reclaims too, so Worker-a dies only afterwards
        self._age_heartbeat("Worker-a", self.timeout + 1)
        before = self._registration()["heartbeats"]["Worker-b"]
        # polling right after registering is inside the heartbeat interval: no write
        self.worker_b._last_reclaim = time.monotonic()
        self.assertIsNone(self.worker_b.next())
        self.assertEqual(before, self._registration()["heartbeats"]["Worker-b"])
        self.assertEqual("Worker-a", self._document(stranded.job_id)["locked_by"])
        # past the interval it heartbeats, and past the timeout it reclaims: the stranded
        # job is released and claimed in the same poll
        self.worker_b._last_heartbeat = 0.0
        self.worker_b._last_reclaim = 0.0
        claimed = self.worker_b.next()
        assert claimed is not None
        self.assertEqual(stranded.job_id, claimed.job_id)
        self.assertGreater(self._registration()["heartbeats"]["Worker-b"], before)


@pytest.mark.mongo
class MongoQueueSelectorTest(TestCase):
    """get_jobs' sample_ids and job_ids selectors (let /jobs select jobs by sample id and by
    job id): sample_ids selects a method's jobs by their first positional argument via an
    anchored regex on payload.descriptor, job_ids by _id, and both combine with method/state
    and with each other by AND."""

    def setUp(self):
        self.client = pymongo.MongoClient(os.environ.get("TEST_MONGODB"))
        queue_config = QueueConfig()
        queue_config.QUEUE_SERVER, queue_config.QUEUE_PORT = getTestMongoServerAndPort()
        queue_config.QUEUE_MONGODB_DBNAME = "test_queue"
        queue_config.QUEUE_MONGODB_COLLECTION_NAME = "queue_selectors"
        self.queue = MongoQueue(queue_config, "consumer_1")

    def tearDown(self):
        self.client.drop_database("test_queue")

    def test_sample_ids_selects_the_first_argument_only(self):
        job_8 = self.queue.put(_payload("getMatchesForSample", 8))
        job_9 = self.queue.put(_payload("getMatchesForSample", 9, band_matches_required=2))
        job_1 = self.queue.put(_payload("getMatchesForSample", 1))
        job_12 = self.queue.put(_payload("getMatchesForSample", 12))
        self.queue.put(_payload("test_method", 8))  # a different method: must not be selected

        # only the selected set is asserted here; the order has its own test below
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8, 9, 12])
        self.assertEqual({job_8, job_9, job_12}, {job.job_id for job in selected})
        # 1 is a text prefix of 12: [,}] after the id in the regex must keep them apart
        selected_one = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[1])
        self.assertEqual([job_1], [job.job_id for job in selected_one])

    def test_sample_ids_selects_jobs_with_a_results_version(self):
        # #241 appends the results version to the descriptor; the selector reads its first argument
        versioned_8 = self.queue.put(_payload("getMatchesForSample", 8, results_version=1))
        versioned_12 = self.queue.put(_payload("getMatchesForSample", 12, band_matches_required=2, results_version=1))
        unversioned_8 = self.queue.put(_payload("getMatchesForSample", 8))
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8])
        self.assertEqual({versioned_8, unversioned_8}, {job.job_id for job in selected})
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[12])
        self.assertEqual([versioned_12], [job.job_id for job in selected])

    def test_sample_ids_read_only_their_own_keys_of_the_descriptor_index(self):
        # a single regex with an alternation gets no index bounds and is tested against every key;
        # a literal prefix per id bounds each id to its own range. 1 and 12 are text prefixes of
        # 10-19 and of nothing here, so a prefix without its terminator would read ten more keys
        for sample_id in range(40):
            self.queue.put(_payload("getMatchesForSample", sample_id))
        query = self.queue._job_query(method="getMatchesForSample", sample_ids=[1, 12])
        stats = self.queue._getCollection().find(query).hint("payload.descriptor_1").explain()["executionStats"]
        self.assertEqual(2, stats["nReturned"])
        # the two matches, and at most one key past the end of each of the four ranges
        self.assertLessEqual(stats["totalKeysExamined"], 2 + 4)

    def test_sample_ids_only_matches_the_vs_jobs_first_argument(self):
        job_8_12 = self.queue.put(_payload("getMatchesForSampleVs", 8, 12))
        job_99_1 = self.queue.put(_payload("getMatchesForSampleVs", 99, 1))
        # getMatchesForSample is a prefix of getMatchesForSampleVs: selecting the plain method
        # must not also pick up the Vs job
        self.queue.put(_payload("getMatchesForSample", 8))
        selected_8 = self.queue.get_jobs(0, 100, method="getMatchesForSampleVs", sample_ids=[8])
        self.assertEqual([job_8_12], [job.job_id for job in selected_8])
        # 1 is job_99_1's SECOND argument only: selecting sample_ids=[1] must not match it
        selected_one = self.queue.get_jobs(0, 100, method="getMatchesForSampleVs", sample_ids=[1])
        self.assertEqual([], selected_one)
        selected_99 = self.queue.get_jobs(0, 100, method="getMatchesForSampleVs", sample_ids=[99])
        self.assertEqual([job_99_1], [job.job_id for job in selected_99])

    def test_sample_ids_accepts_negative_ids(self):
        job_neg = self.queue.put(_payload("getMatchesForSample", -5))
        self.queue.put(_payload("getMatchesForSample", 5))
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[-5])
        self.assertEqual([job_neg], [job.job_id for job in selected])

    def test_sample_ids_pages_after_the_selection(self):
        # the default (ascending=False) path sorts by _id descending, so paging order is
        # deterministic: newest first
        job_ids = [self.queue.put(_payload("getMatchesForSample", sid)) for sid in (8, 9, 12)]
        self.queue.put(_payload("getMatchesForSample", 20))  # not part of the selection
        page_1 = self.queue.get_jobs(0, 2, method="getMatchesForSample", sample_ids=[8, 9, 12])
        page_2 = self.queue.get_jobs(2, 2, method="getMatchesForSample", sample_ids=[8, 9, 12])
        self.assertEqual(list(reversed(job_ids)), [job.job_id for job in page_1] + [job.job_id for job in page_2])

    def test_sample_ids_honours_the_state_branch(self):
        finished_id = self.queue.put(_payload("getMatchesForSample", 8))
        queued_id = self.queue.put(_payload("getMatchesForSample", 9))
        self.queue.next().complete()  # claims and finishes the older job (id 8)
        selected_finished = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8, 9], state="finished")
        self.assertEqual([finished_id], [job.job_id for job in selected_finished])
        selected_queued = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8, 9], state="queued")
        self.assertEqual([queued_id], [job.job_id for job in selected_queued])

    def test_sample_ids_present_but_empty_selects_nothing(self):
        self.queue.put(_payload("getMatchesForSample", 8))
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[])
        self.assertEqual([], selected)

    def test_sample_ids_that_do_not_parse_are_dropped(self):
        # a direct caller may pass what the annotation rules out: only what int() takes is used
        job_8 = self.queue.put(_payload("getMatchesForSample", 8))
        selected = self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=cast(List[int], ["8", "x", None]))
        self.assertEqual([job_8], [job.job_id for job in selected])
        self.assertEqual([], self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=cast(List[int], ["x"])))

    def test_sample_ids_without_a_method_selects_nothing(self):
        # JobResource refuses this with 400; get_jobs itself has no method to anchor the
        # regex on and must not fall back to "every job", regardless of caller
        self.queue.put(_payload("getMatchesForSample", 8))
        selected = self.queue.get_jobs(0, 100, method=None, sample_ids=[8])
        self.assertEqual([], selected)

    def test_job_ids_selects_by_id(self):
        job_a = self.queue.put(_payload("getMatchesForSample", 1))
        job_b = self.queue.put(_payload("modifySample", 2))
        self.queue.put(_payload("getMatchesForSample", 3))
        selected = self.queue.get_jobs(0, 100, job_ids=[str(job_a), str(job_b)])
        self.assertEqual({job_a, job_b}, {job.job_id for job in selected})

    def test_job_ids_ignores_invalid_hex_and_unknown_ids_match_nothing(self):
        job_a = self.queue.put(_payload("getMatchesForSample", 1))
        unknown_but_valid_hex = "0" * 24
        selected = self.queue.get_jobs(0, 100, job_ids=[str(job_a), "not-a-valid-id", unknown_but_valid_hex])
        self.assertEqual([job_a], [job.job_id for job in selected])

    def test_job_ids_present_but_all_invalid_selects_nothing(self):
        self.queue.put(_payload("getMatchesForSample", 1))
        selected = self.queue.get_jobs(0, 100, job_ids=["not-a-valid-id"])
        self.assertEqual([], selected)

    def test_sample_ids_and_job_ids_combine_by_and(self):
        job_8_a = self.queue.put(_payload("getMatchesForSample", 8))
        job_9 = self.queue.put(_payload("getMatchesForSample", 9))
        job_8_b = self.queue.put(_payload("getMatchesForSample", 8, band_matches_required=2))
        selected = {job.job_id for job in self.queue.get_jobs(0, 100, method="getMatchesForSample", sample_ids=[8, 9], job_ids=[str(job_8_a), str(job_9)])}
        self.assertEqual({job_8_a, job_9}, selected)
        self.assertNotIn(job_8_b, selected)


if __name__ == "__main__":
    unittest.main()
