import logging
import os
import time
import unittest
from threading import Thread
from unittest import TestCase
from unittest.mock import patch

import pymongo
import pytest
from bson import ObjectId
from bson.json_util import dumps, loads

from mcrit.config.QueueConfig import QueueConfig
from mcrit.libs.mongoqueue import MongoQueue
from mcrit.queue.LocalQueue import LocalQueue
from mcrit.queue.QueueRemoteCalls import (
    NoProgressReporter,
    QueueRemoteCallee,
    QueueRemoteCaller,
    Remote,
    _createJobPayload,
    get_descriptor,
    rearrange_params,
    upload_file_params,
)

from .context import getTestMongoServerAndPort

LOG = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logging.disable(logging.CRITICAL)


class myWorker(QueueRemoteCallee):
    @Remote(
        file_locations=[0, 2, 1],
        json_locations=[3, 4],
        kwfile_locations=["eggs"],
        kwjson_locations=["foo"],
    )
    def test(self, *params, **kwparams):
        if "force_recalculation" in kwparams:
            raise AttributeError
        return {"args": dumps(params, sort_keys=True), "kwargs": dumps(kwparams, sort_keys=True)}

    @Remote(progress=True)
    def test_progress(self, progress_reporter=NoProgressReporter()):
        # if not progress_reporter:
        #    progress_reporter = NoProgressReporter()
        total_steps = 100

        # raise Exception("The job received a terminatation message while reporting progress")
        progress_reporter._report_interval = 0

        progress_reporter.set_total(total_steps)
        for _ in range(total_steps):
            print(total_steps)
            time.sleep(0.01)
            progress_reporter.step()
        return 0x42

    def function_that_isnt_remote(self, a, b, c):
        pass

    @Remote(results_version=1)
    def versioned(self, i):
        return i

    @Remote()
    def child_job(self, i):
        time.sleep(0.2)
        return i * i

    @Remote()
    def parent_job(self, child_jobs):
        results = []
        for id in child_jobs:
            results.append(self.getResultForJob(id))
        results = [i for i in results if i is not None]
        return sum(results)


# Define Caller Class
class Caller(QueueRemoteCaller(myWorker)):
    def start_child_and_parent_job(self):
        child_job_ids = []
        for i in range(9):
            child_job_ids.append(self.child_job(i, force_recalculation=True))
        # await_jobs param is managed by queue and not passed to function.
        # Maybe call it job_dependencies?
        # parent job will only be executed after all await_jobs were finished, terminated or failed.
        parent_job_id = self.parent_job(child_job_ids, await_jobs=child_job_ids)
        return parent_job_id


class RemoteCalleeTest(TestCase):
    def setUp(self):
        self.queue = LocalQueue()
        self.worker = myWorker(self.queue)

    def test_progress_local(self):
        self.worker.test_progress()


class LocalQueueRemoteCallTest(TestCase):
    def setUp(self):
        self.queue = LocalQueue()
        self.worker = myWorker(self.queue)
        self.queue.set_worker(self.worker)
        self.caller = Caller(self.queue)
        self.queue.clean_interval = 1e100

    # TODO do a real test here
    @pytest.mark.sleep
    def test_progress(self):
        id = self.caller.test_progress()
        time.sleep(0.2)
        job = self.queue.get_job(id)
        self.assertGreater(job._data["progress"], 0)

    @pytest.mark.sleep
    def test_termination(self):
        id = self.caller.test_progress()
        job = self.queue.get_job(id)
        status_termination = job.terminate()
        print("termination:", status_termination)
        print()
        time.sleep(0.1)
        self.assertTrue(job._is_terminated())

    def test_remote_calls(self):
        params_list = [[], [b"bim", b"bam", b"bum", ["hi", {"ha": "ho"}], {"answer": 42}, {"universe": "exists"}]]
        kwparams_list = [{}, dict(foo={"bar": False}, spam={19: "hi"}, orange=[2, 3, {5: "deep thought"}], eggs=b"bar")]
        for params in params_list:
            for kwparams in kwparams_list:
                job_id = self.caller.test(*params, **kwparams)
                result_id = self.caller.awaitResult(job_id)
                print("RID", result_id, job_id)
                result = self.queue._grid_to_dicts(result_id)
                self.assertEqual(result, self.worker.test(*params, **kwparams))
                self.assertEqual(str(self.queue._grid_to_meta(result_id)["job"]), job_id)
        self.queue.clear()

    @pytest.mark.sleep
    def test_clean_period(self):
        old_cache_time = self.queue.cache_time
        old_clean_interval = self.queue.clean_interval
        self.queue.cache_time = 0.0
        self.queue.clean()
        self.queue.clean_interval = 0.8

        params = []
        kwparams = {"foo": {"test_clean_period": True}}

        time.sleep(self.queue.clean_interval * 1.2)
        job_id_1 = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_1)

        job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_2)

        self.assertNotEqual(self.queue.get_job(job_id_1), None)

        time.sleep(self.queue.clean_interval * 1.2)
        job_id_1 = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_1)

        time.sleep(self.queue.clean_interval * 1.2)

        job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_2)

        self.assertEqual(self.queue.get_job(job_id_1), None)

        self.queue.cache_time = old_cache_time
        self.queue.clean_interval = old_clean_interval

    def test_multiple_cleans(self):
        params = [b"cde"]
        kwparams = {"test_queue_clean": True, "foo": "abc"}

        # start job 2 times
        job_id = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id)

        job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_2)

        self.queue.clean()
        self.queue.clean()
        self.queue.clean()

    def test_file_cache(self):
        def get_file_id(job_id):
            payload = self.queue.get_job(job_id).payload
            file_params = loads(payload["file_params"])
            return file_params["foo"]

        params = []
        kwparams = {"foo": {"test_file_cache": True}}
        job_id_1 = self.caller.test(*params, force_recalculation=False, **kwparams)
        job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)

        file_1 = get_file_id(job_id_1)
        file_2 = get_file_id(job_id_2)

        self.assertEqual(file_1, file_2)

    def test_job_cache(self):
        params_list = [[], [b"bim", b"bam", b"bum", ["hi", {"ha": "ho"}], {"answer": 42}, {"universe": "exists"}]]
        kwparams_list = [{}, dict(foo={"bar": False}, spam={19: "hi"}, orange=[2, 3, {5: "deep mind"}], eggs=b"bar")]
        for params in params_list:
            for kwparams in kwparams_list:
                job_id_1 = self.caller.test(*params, **kwparams)
                job_id_2 = self.caller.test(*params, **kwparams)
                self.assertEqual(job_id_1, job_id_2)
        self.queue.clear()
        for params in params_list:
            for kwparams in kwparams_list:
                job_id_1 = self.caller.test(*params, **kwparams)
                job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)
                self.assertNotEqual(job_id_1, job_id_2)
        self.queue.clear()

    def test_job_cache_keeps_results_versions_apart(self):
        # #241: a job computed before a release changed its kind of report is not handed out again
        params, _ = rearrange_params([7], {}, [], [])
        unversioned = str(self.queue.put(_createJobPayload("versioned", params, {}, get_descriptor("versioned", params, {}))))
        job_id_1 = self.caller.versioned(7)
        self.assertNotEqual(unversioned, job_id_1)
        self.caller.awaitResult(job_id_1)
        self.assertEqual(job_id_1, self.caller.versioned(7))
        with patch.object(myWorker.versioned, "results_version", 2):
            job_id_2 = self.caller.versioned(7)
            self.assertNotIn(job_id_2, (unversioned, job_id_1))
            self.caller.awaitResult(job_id_2)
            self.assertEqual(job_id_2, self.caller.versioned(7))
        self.assertEqual(job_id_1, self.caller.versioned(7))
        # and the descriptor still reads the way its positional readers expect
        descriptor = loads(self.queue.get_job(job_id_1).payload["descriptor"])
        self.assertEqual(["versioned", {"0": 7}, {}, {"results_version": 1}], descriptor)
        self.queue.clear()

    def _set_job_fields(self, job_id, **fields):
        self.queue._jobs[job_id].update(fields)

    def _unknown_job_id(self):
        return "no-such-job"

    def test_job_cache_prefers_finished_and_skips_failed(self):
        # fkie-cad/mcritweb#47: the cache answers with the newest finished job, falls back to an unfinished one
        # only when no finished job exists, and never answers with a failed or terminated job
        kwparams = {"test_cache_preference": True}
        job_id_1 = self.caller.test(**kwparams)
        self.caller.awaitResult(job_id_1)
        job_id_2 = self.caller.test(force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_2)
        self.assertNotEqual(job_id_1, job_id_2)
        self.assertEqual(job_id_2, self.caller.test(**kwparams))
        # the newer job still running: the older finished one is the usable result
        self._set_job_fields(job_id_2, finished_at=None)
        self.assertEqual(job_id_1, self.caller.test(**kwparams))
        # nothing usable left: a new job is created instead of pointing at a dead one
        self._set_job_fields(job_id_1, attempts_left=0)
        self._set_job_fields(job_id_2, terminated=True)
        job_id_3 = self.caller.test(**kwparams)
        self.assertNotIn(job_id_3, (job_id_1, job_id_2))
        # a lookup of an unknown job must not break the cache afterwards
        self.assertIsNone(self.queue.get_job(self._unknown_job_id()))
        self.assertEqual(job_id_3, self.caller.test(**kwparams))
        self.queue.clear()

    def test_function_access(self):
        params = {0: 1, 1: 2, 2: 3}
        payload1 = _createJobPayload("function_that_isnt_remote", params, {}, get_descriptor("function_that_isnt_remote", params, {}))
        job_id = self.queue.put(payload1)
        # self.caller.awaitResult(job_id)
        self.caller.getJob(job_id).last_error
        # self.assertTrue("NotImplementedError" in error_message)
        # with self.assertRaises(AttributeError):
        #    self.queue.put(payload2)

        # This is also done for mongo
        with self.assertRaises(AttributeError):
            self.caller.function_that_isnt_remote(1, 2, 3)
        with self.assertRaises(AttributeError):
            self.caller.function_that_doesnt_exist(1, 2, 3)

    def test_job_records_who_asked(self):
        # fkie-cad/mcritweb#37: the user a job was requested for is stored on the job, is not part of the
        # descriptor (another user's identical request is still served from the cache), and
        # is None when nobody was named
        kwparams = {"test_job_owner": True}
        job_id = self.caller.test(username="alice", **kwparams)
        self.assertEqual("alice", self.queue.get_job(job_id).username)
        self.assertEqual("alice", self.queue.get_job(job_id)._data["username"])
        self.assertEqual(job_id, self.caller.test(username="bob", **kwparams))
        self.assertEqual("alice", self.queue.get_job(job_id).username)
        forced_job_id = self.caller.test(username="bob", force_recalculation=True, **kwparams)
        self.assertEqual("bob", self.queue.get_job(forced_job_id).username)
        anonymous_job_id = self.caller.test(test_job_owner_anonymous=True)
        self.assertIsNone(self.queue.get_job(anonymous_job_id).username)

    def test_queue_listing_filters_before_paging(self):
        # fkie-cad/mcritweb#57: a text filter, a user and a state select the jobs first, and only then is the
        # page cut; the count is the count of that same selection
        for i in range(6):
            self.caller.test(needle="apple", index=i, username="alice" if i % 2 else "bob")
            self.caller.test(needle="pear", index=i, username="alice")
        for job in self.queue.get_jobs(0, 0):
            self.caller.awaitResult(str(job.job_id))
        self.assertEqual(12, self.caller.getQueueCount())
        self.assertEqual(6, self.caller.getQueueCount(filter="APPLE"))
        self.assertEqual(9, self.caller.getQueueCount(username="alice"))
        self.assertEqual(3, self.caller.getQueueCount(filter="apple", username="alice"))
        self.assertEqual(0, self.caller.getQueueCount(filter="banana"))
        self.assertEqual(12, self.caller.getQueueCount(state="finished"))
        self.assertEqual(0, self.caller.getQueueCount(state="queued"))
        self.assertEqual(0, self.caller.getQueueCount(state="no-such-state"))
        page_1 = self.caller.getQueueData(0, 4, filter="apple")
        page_2 = self.caller.getQueueData(4, 4, filter="apple")
        self.assertEqual(4, len(page_1))
        self.assertEqual(2, len(page_2))
        self.assertTrue(all("apple" in job["payload"]["params"] for job in page_1 + page_2))
        self.assertEqual(6, len({str(job["_id"]) for job in page_1 + page_2}))
        # newest first by default, oldest first when ascending
        numbers = [job["number"] for job in self.caller.getQueueData(0, 0, filter="apple")]
        self.assertEqual(sorted(numbers, reverse=True), numbers)
        numbers = [job["number"] for job in self.caller.getQueueData(0, 0, filter="apple", ascending=True)]
        self.assertEqual(sorted(numbers), numbers)
        self.assertEqual(3, len(self.caller.getQueueData(0, 0, filter="apple", username="alice")))
        self.assertEqual(12, len(self.caller.getQueueData(0, 0, state="finished")))
        # a parameter is matched as the listing shows it, not as an ASCII escape
        umlaut_job = self.caller.test(needle="Müller", index=99)
        self.caller.awaitResult(umlaut_job)
        self.assertEqual(1, self.caller.getQueueCount(filter="müller"))
        self.assertEqual(1, self.caller.getQueueCount(filter="Müller"))
        self.assertEqual(0, self.caller.getQueueCount(filter="u00fc"))
        self.queue.clear()

    def test_file_access(self):
        params = []
        kwparams = {"foo": {"test_file_access": True}}
        job_id = self.caller.test(*params, force_recalculation=False, **kwparams)
        payload = self.queue.get_job(job_id).payload
        file_params = loads(payload["file_params"])
        file = file_params["foo"]
        result = self.queue._grid_to_dicts(file)
        self.assertEqual("Access Not Allowed", result)

    def test_file_job_ref(self):
        params = []
        kwparams = {"foo": {"test_file_job_ref": True}}
        job_ids = []
        for i in range(5):
            kwparams["tested_job"] = i
            job_id = self.caller.test(*params, force_recalculation=False, **kwparams)
            job_ids.append(job_id)
            payload = self.queue.get_job(job_id).payload
            file_params = loads(payload["file_params"])
            file = file_params["foo"]
            meta = self.queue._grid_to_meta(file)
            print(meta)
            self.assertEqual(job_ids, meta["jobs"])
            self.assertEqual(0, meta["tmp_lock"])

    def test_file_tmp_lock(self):
        params = []
        kwparams = {"foo": {"test_tmp_lock": True}}
        job_id = self.caller.test(*params, force_recalculation=False, **kwparams)
        payload = self.queue.get_job(job_id).payload
        file_params = loads(payload["file_params"])
        hashes = loads(payload["descriptor"])[2]
        upload_file_params(self.caller, file_params, hashes)
        # lock should be 1 now
        file = file_params["foo"]
        meta = self.queue._grid_to_meta(file)
        self.assertEqual(1, meta["tmp_lock"])

    @pytest.mark.sleep
    def test_queue_clean(self):
        self.queue.cache_time = 0.1
        self.queue.clean_interval = 1e100

        params = [b"cde"]
        kwparams = {"test_queue_clean": True, "foo": "abc"}

        # start job 2 times
        job_id = self.caller.test(*params, force_recalculation=True, **kwparams)
        result_id = self.caller.awaitResult(job_id)

        job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)
        result_id_2 = self.caller.awaitResult(job_id_2)

        # extract file ids
        files = loads(self.queue.get_job(job_id).payload["file_params"])

        # start job again but with changed params
        time.sleep(self.queue.cache_time)
        params = []
        job_id_3 = self.caller.test(*params, force_recalculation=True, **kwparams)
        result_id_3 = self.caller.awaitResult(job_id_3)

        # clean. This should impact job 1 and 2, but not 3.
        # we expect parameter 0 to be deleted, but not foo
        self.queue.clean()

        # Check jobs
        self.assertEqual(self.queue.get_job(job_id), None)
        self.assertEqual(self.queue.get_job(job_id_2), None)
        self.assertNotEqual(self.queue.get_job(job_id_3), None)

        # check results
        self.assertEqual(self.queue._grid_to_file(result_id), None)
        self.assertEqual(self.queue._grid_to_file(result_id_2), None)
        self.assertNotEqual(self.queue._grid_to_file(result_id_3), None)

        # Check params
        self.assertNotEqual(self.queue._grid_to_file(files["foo"], results_only=False), None)
        self.assertEqual(self.queue._grid_to_file(files["0"], results_only=False), None)

        # Check foo, if jobs were cleaned:
        self.assertEqual(self.queue._grid_to_meta(files["foo"])["jobs"], [job_id_3])

    def test_job_dependencies(self):
        self.queue.clear()
        additional_worker = myWorker(self.queue)
        additional_worker_thread = Thread(target=additional_worker.run)
        additional_worker_thread.start()

        job_id = self.caller.start_child_and_parent_job()
        result_id = self.caller.awaitResult(job_id)
        result = self.caller.getResult(result_id)
        self.assertEqual(result, sum([i * i for i in range(9)]))

        additional_worker.terminate()
        self.queue.clear()


### Added mongo attribute
@pytest.mark.mongo
class MongoQueueRemoteCallTest(LocalQueueRemoteCallTest):
    def setUp(self):
        self.client = pymongo.MongoClient(os.environ.get("TEST_MONGODB"))
        queue_config = QueueConfig()
        queue_config.QUEUE_SERVER, queue_config.QUEUE_PORT = getTestMongoServerAndPort()
        queue_config.QUEUE_MONGODB_DBNAME = "test_queue_remote_calls"
        queue_config.QUEUE_MONGODB_COLLECTION_NAME = "queue_1"
        self.queue = MongoQueue(queue_config, "consumer_1")
        self.caller = Caller(self.queue)
        self.queue.clean_interval = 1e100
        self.worker = myWorker(self.queue)
        self.worker_thread = Thread(target=self.worker.run)
        self.worker_thread.start()

    def _set_job_fields(self, job_id, **fields):
        queue = self.queue
        assert isinstance(queue, MongoQueue)
        queue._getCollection().update_one({"_id": ObjectId(job_id)}, {"$set": fields})

    def _unknown_job_id(self):
        return str(ObjectId())

    @pytest.mark.sleep
    def test_termination(self):
        id = self.caller.test_progress()
        job = self.queue.get_job(id)
        time.sleep(0.1)
        status_termination = job.terminate()
        print("termination:", status_termination)
        print()
        time.sleep(0.5)
        self.assertTrue(job._is_terminated())
        job = self.queue.get_job(id)
        print(job._data)
        self.assertTrue("The job received a terminatation message while reporting progress" in job.last_error)
        result_id = self.caller.awaitResult(id)
        self.assertEqual(result_id, None)

    def test_job_cache_prio(self):
        params = []
        kwparams = {"test_cache_prio": True}

        job_id = self.caller.test(*params, **kwparams)
        self.caller.awaitResult(job_id)

        job_id_2 = self.caller.test(*params, force_recalculation=True, **kwparams)
        self.caller.awaitResult(job_id_2)

        job_id_3 = self.caller.test(*params, force_recalculation=False, **kwparams)
        self.assertEqual(job_id_2, job_id_3)

        self.worker.terminate()
        self.worker_thread.join()
        # the forced job cannot run without a worker, so a plain request keeps being served
        # by the finished job rather than by the one that is only queued (fkie-cad/mcritweb#47)
        job_id_4 = self.caller.test(*params, force_recalculation=True, **kwparams)
        job_id_5 = self.caller.test(*params, force_recalculation=False, **kwparams)
        self.assertNotEqual(job_id_2, job_id_4)
        self.assertEqual(job_id_2, job_id_5)

        self.worker_thread = Thread(target=self.worker.run)
        self.worker_thread.start()

    def test_function_access(self):
        with self.assertRaises(AttributeError):
            self.caller.function_that_isnt_remote(1, 2, 3)
        with self.assertRaises(AttributeError):
            self.caller.function_that_doesnt_exist(1, 2, 3)

    def tearDown(self):
        self.worker.terminate()
        self.worker_thread.join()
        self.client.drop_database("test_queue_remote_calls")


if __name__ == "__main__":
    unittest.main()
