"""A finished analysis is queued again from what its job stored (POST /jobs/{job_id}/rerun, familiary/mcritweb#55).

A client can only rebuild a request from the arguments a job shows, which leaves out the queries (their
binary or report is only referenced by the job) and the unique blocks (the client cannot ask for those to
be recalculated). The rerun repeats the stored payload instead: the same method, arguments and files, a
descriptor computed with the current results version, and never an answer from the cache.
"""

import json
import logging
import os
import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pymongo.errors
import pytest
from bson import ObjectId
from smda.common.SmdaReport import SmdaReport

from mcrit.client.McritClient import McritClient, McritConflict, McritNotFound
from mcrit.config.McritConfig import McritConfig
from mcrit.config.QueueConfig import QueueConfig
from mcrit.config.StorageConfig import StorageConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import JobNotFoundError, JobNotRerunnableError
from mcrit.server import application_routes
from mcrit.server.JobResource import JobResource
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import RESULTS_VERSION, Worker

from .context import config, getTestMongoServerAndPort

logging.disable(logging.CRITICAL)

FIXTURES = os.path.dirname(os.path.abspath(__file__))
CORPUS = ("example_report.smda", "example_report_2.smda", "example_report_3.smda")
DB_NAME = "test_job_rerun"


def load_report(name):
    return SmdaReport.fromFile(os.path.join(FIXTURES, name))


def load_report_json(name):
    with open(os.path.join(FIXTURES, name)) as handle:
        return json.load(handle)


class JobRerunTest(unittest.TestCase):
    """The rerun on a corpus of three samples, in memory; MongoJobRerunTest repeats it on MongoDB."""

    def _makeIndex(self):
        index = MinHashIndex(config)
        index._storage.clearStorage()
        return index

    def _runQueuedJobs(self):
        """Process what the queue holds; LocalQueue runs every job as it is put."""

    def _setJobFields(self, job_id, **fields):
        self.index.queue._jobs[job_id].update(fields)

    def _fileMeta(self, file_id):
        return self.index.queue._files_meta.get(file_id)

    def _fileExists(self, file_id):
        return file_id in self.index.queue._files

    def _deleteFile(self, file_id):
        self.index.queue._delete_grid(file_id)

    def setUp(self):
        self.index = self._makeIndex()
        self.sample_ids = [self.index.addReport(load_report(name))["sample_info"]["sample_id"] for name in CORPUS]
        self._runQueuedJobs()

    def _job(self, job_id):
        job = self.index.getJob(job_id)
        assert job is not None
        return job

    def _payload(self, job_id, field):
        return json.loads(self._job(job_id).payload[field])

    def _jobCount(self):
        return self.index.getQueueCount()

    def _assertRefused(self, job_id, error=JobNotRerunnableError):
        count = self._jobCount()
        with self.assertRaises(error):
            self.index.rerunJob(job_id, username="bob")
        # checked before anything is queued
        self.assertEqual(count, self._jobCount())

    def test_a_sample_match_is_queued_again_with_its_arguments_and_a_fresh_descriptor(self):
        old = self.index.getMatchesForSample(self.sample_ids[0], username="alice")
        self._runQueuedJobs()
        # a release that changed what such a report holds since (#241): the rerun is keyed on the version declared now
        with patch.object(Worker.getMatchesForSample, "results_version", RESULTS_VERSION + 1):
            new = self.index.rerunJob(old, username="bob")
        self._runQueuedJobs()
        self.assertNotEqual(old, new)
        old_job, new_job = self._job(old), self._job(new)
        self.assertEqual("getMatchesForSample", new_job.method)
        self.assertEqual(self._payload(old, "params"), self._payload(new, "params"))
        old_descriptor, new_descriptor = self._payload(old, "descriptor"), self._payload(new, "descriptor")
        self.assertEqual(old_descriptor[:3], new_descriptor[:3])
        self.assertEqual([{"results_version": RESULTS_VERSION}, {"results_version": RESULTS_VERSION + 1}], [old_descriptor[3], new_descriptor[3]])
        # recorded for who asked for the rerun, the old job stays its requester's
        self.assertEqual(("alice", "bob"), (old_job.username, new_job.username))
        self.assertEqual(self.index.getResultForJob(old)["matches"], self.index.getResultForJob(new)["matches"])

    def test_a_rerun_is_never_answered_from_the_cache(self):
        old = self.index.getMatchesForSample(self.sample_ids[0])
        self._runQueuedJobs()
        # the old job is finished and has the very descriptor the rerun gets
        self.assertEqual(old, self.index.getMatchesForSample(self.sample_ids[0]))
        new = self.index.rerunJob(old)
        self.assertNotEqual(old, new)
        self.assertEqual(self._job(old).payload["descriptor"], self._job(new).payload["descriptor"])
        self._runQueuedJobs()
        # the descriptor a new request computes, so the next one is served by the rerun, the newest finished job
        self.assertEqual(new, self.index.getMatchesForSample(self.sample_ids[0]))

    def test_a_query_rerun_shares_the_file_and_deleting_the_old_job_keeps_it(self):
        old = self.index.getMatchesForSmdaReport(load_report_json("example_report_3.smda"), username="alice")
        self._runQueuedJobs()
        new = self.index.rerunJob(old, username="bob")
        self._runQueuedJobs()
        file_params = self._payload(old, "file_params")
        self.assertEqual(file_params, self._payload(new, "file_params"))
        self.assertEqual(self._payload(old, "descriptor")[2], self._payload(new, "descriptor")[2])
        file_id = file_params["0"]
        self.assertEqual(sorted([old, new]), sorted(self._fileMeta(file_id)["jobs"]))
        self.assertEqual(0, self._fileMeta(file_id)["tmp_lock"])
        # each run stores the query as a sample of its own, so the reports differ in its id, not in what it matched
        old_samples, new_samples = (self.index.getResultForJob(job_id)["matches"]["samples"] for job_id in (old, new))
        self.assertTrue(old_samples)
        self.assertEqual([(entry["sha256"], entry["matched"]) for entry in old_samples], [(entry["sha256"], entry["matched"]) for entry in new_samples])
        # either job keeps the file for the other
        self.assertEqual(1, self.index.deleteJob(old))
        self.assertTrue(self._fileExists(file_id))
        self.assertEqual([new], self._fileMeta(file_id)["jobs"])
        newer = self.index.rerunJob(new)
        self._runQueuedJobs()
        self.assertIsNotNone(self._job(newer).result)
        self.index.deleteJob(new)
        self.assertTrue(self._fileExists(file_id))
        self.index.deleteJob(newer)
        self.assertFalse(self._fileExists(file_id))

    def test_a_query_rerun_keeps_the_file_when_the_old_job_is_deleted_before_it_is_linked(self):
        old = self.index.getMatchesForSmdaReport(load_report_json("example_report_3.smda"))
        self._runQueuedJobs()
        file_id = self._payload(old, "file_params")["0"]
        put = self.index.queue.put

        def put_after_deleting_the_old_job(payload, **kwargs):
            # the file is claimed for the rerun by now, and not linked to it yet
            self.assertEqual(1, self.index.deleteJob(old))
            return put(payload, **kwargs)

        with patch.object(self.index.queue, "put", side_effect=put_after_deleting_the_old_job):
            new = self.index.rerunJob(old)
        self._runQueuedJobs()
        self.assertIsNotNone(self._job(new).result)
        self.assertEqual((0, [new]), (self._fileMeta(file_id)["tmp_lock"], self._fileMeta(file_id)["jobs"]))
        self.index.deleteJob(new)
        self.assertFalse(self._fileExists(file_id))

    def test_a_rerun_that_fails_to_queue_gives_up_its_claim_on_the_file(self):
        old = self.index.getMatchesForSmdaReport(load_report_json("example_report_3.smda"))
        self._runQueuedJobs()
        file_id = self._payload(old, "file_params")["0"]
        count = self._jobCount()
        # the database going away as the job is put, say
        with patch.object(self.index.queue, "put", side_effect=pymongo.errors.AutoReconnect("gone")):
            with self.assertRaises(pymongo.errors.AutoReconnect):
                self.index.rerunJob(old)
        self.assertEqual(count, self._jobCount())
        self.assertEqual((0, [old]), (self._fileMeta(file_id)["tmp_lock"], self._fileMeta(file_id)["jobs"]))
        # a claim left behind would keep the file after its last job
        self.index.deleteJob(old)
        self.assertFalse(self._fileExists(file_id))

    def test_a_cross_compare_reruns_every_job_it_combined(self):
        for sample_group_only in (False, True):
            with self.subTest(sample_group_only=sample_group_only):
                old = self.index.getMatchesCross(self.sample_ids, sample_group_only=sample_group_only, username="alice")
                self._runQueuedJobs()
                new = self.index.rerunJob(old, username="bob")
                old_children, new_children = self._payload(old, "params")["0"], self._payload(new, "params")["0"]
                self.assertEqual(sorted(str(sample_id) for sample_id in self.sample_ids), sorted(new_children))
                self.assertEqual(sorted(old_children), sorted(new_children))
                for sample_id, old_child in old_children.items():
                    new_child = new_children[sample_id]
                    self.assertNotEqual(old_child, new_child)
                    self.assertEqual(self._job(old_child).method, self._job(new_child).method)
                    self.assertEqual(self._payload(old_child, "params"), self._payload(new_child, "params"))
                    self.assertEqual("bob", self._job(new_child).username)
                # the new combining job waits for the new jobs, not the old ones
                self.assertEqual(sorted(new_children.values()), sorted(self._job(new)._data["all_dependencies"]))
                self._runQueuedJobs()
                self.assertEqual(("combineMatchesToCross", "bob"), (self._job(new).method, self._job(new).username))
                self.assertEqual(self.index.getResultForJob(old), self.index.getResultForJob(new))

    def test_a_cross_compare_asked_for_again_is_served_by_the_rerun(self):
        # a new cross compare keys its jobs by int sample id, which the descriptor sorts 2 before 12; keyed by str,
        # "12" would come first, and the rerun would have a descriptor that no new cross compare has
        for _ in range(10):
            self.index._storage._useCounter("samples")
        late_sample_id = self.index.addReport(load_report("library_report.smda"))["sample_info"]["sample_id"]
        self._runQueuedJobs()
        sample_ids = [max(self.sample_ids), late_sample_id]
        self.assertNotEqual(sorted(sample_ids), sorted(sample_ids, key=str))
        old = self.index.getMatchesCross(sample_ids)
        self._runQueuedJobs()
        new = self.index.rerunJob(old)
        self._runQueuedJobs()
        self.assertEqual(new, self.index.getMatchesCross(sample_ids))

    def test_a_cross_compare_that_fails_to_queue_deletes_the_jobs_it_queued(self):
        cross = self.index.getMatchesCross(self.sample_ids)
        self._runQueuedJobs()
        count = self._jobCount()
        put = self.index.queue.put
        # the puts up to the one that fails: on the second job, and on the combining job after all the others
        for puts in (["getMatchesForSample"] * 2, ["getMatchesForSample"] * len(self.sample_ids) + ["combineMatchesToCross"]):
            with self.subTest(failing=puts[-1]):
                methods = []

                def put_failing_at_the_last(payload, **kwargs):
                    methods.append(payload["method"])
                    if len(methods) == len(puts):
                        raise pymongo.errors.AutoReconnect("gone")
                    return put(payload, **kwargs)

                with patch.object(self.index.queue, "put", side_effect=put_failing_at_the_last):
                    with self.assertRaises(pymongo.errors.AutoReconnect):
                        self.index.rerunJob(cross)
                self.assertEqual(puts, methods)
                self.assertEqual(count, self._jobCount())

    def test_a_failed_or_terminated_job_can_be_rerun(self):
        failed = self.index.getMatchesForSample(12345)  # no such sample: every attempt fails
        self._runQueuedJobs()
        self.assertTrue(self._job(failed).is_failed)
        rerun = self.index.rerunJob(failed)
        self._runQueuedJobs()
        self.assertEqual(self._payload(failed, "params"), self._payload(rerun, "params"))
        terminated = self.index.getUniqueBlocks(self.sample_ids[:2], force_recalculation=True)
        self._runQueuedJobs()
        self._setJobFields(terminated, terminated=True)
        rerun = self.index.rerunJob(terminated)
        self._runQueuedJobs()
        self.assertEqual(self._payload(terminated, "params"), self._payload(rerun, "params"))
        self.assertIsNotNone(self._job(rerun).result)

    def test_a_job_still_queued_or_running_is_refused(self):
        job_id = self.index.getMatchesForSample(self.sample_ids[0])
        self._runQueuedJobs()
        self._setJobFields(job_id, finished_at=None, started_at=None, locked_by=None)
        self._assertRefused(job_id)
        self._setJobFields(job_id, started_at=datetime.now(), locked_by="Worker-elsewhere")
        self._assertRefused(job_id)

    def test_what_is_no_analysis_is_refused(self):
        minhash_jobs = self.index.getQueueData(0, 0, method="updateMinHashesForSample")
        self.assertEqual(len(CORPUS), len(minhash_jobs))
        self._assertRefused(str(minhash_jobs[0]["_id"]))
        family_stats = self.index.recomputeFamilyStats(force_recalculation=True)
        self._runQueuedJobs()
        self._assertRefused(family_stats)

    def test_an_unknown_job_is_refused(self):
        self._assertRefused(str(ObjectId()), error=JobNotFoundError)

    def test_a_cross_compare_is_refused_whole_for_one_job_it_combined(self):
        cross = self.index.getMatchesCross(self.sample_ids)
        self._runQueuedJobs()
        children = list(self._payload(cross, "params")["0"].values())
        self._setJobFields(children[-1], finished_at=None, started_at=datetime.now(), locked_by="Worker-elsewhere")
        self._assertRefused(cross)
        self.index.deleteJob(children[-1])
        self._assertRefused(cross)

    def test_a_query_whose_file_is_gone_is_refused(self):
        job_id = self.index.getMatchesForSmdaReport(load_report_json("example_report_3.smda"))
        self._runQueuedJobs()
        self._deleteFile(self._payload(job_id, "file_params")["0"])
        self._assertRefused(job_id)


class RerunnableMethodsTest(unittest.TestCase):
    def test_only_analyses_are_rerunnable(self):
        # an explicit list: a remote method added later is not rerun until someone puts it here
        remote_methods = {name for name in dir(Worker) if getattr(getattr(Worker, name), "remote", False)}
        self.assertTrue(MinHashIndex.RERUNNABLE_METHODS <= remote_methods)
        for method in ("addBinarySample", "deleteSample", "modifySample", "deleteFamily", "modifyFamily", "updateMinHashesForSample", "rebuildIndex", "doDbCleanup"):
            self.assertIn(method, remote_methods)
            self.assertNotIn(method, MinHashIndex.RERUNNABLE_METHODS)


@pytest.mark.mongo
class MongoJobRerunTest(JobRerunTest):
    """The same against MongoDB storage and MongoQueue, with the worker taking the jobs as a worker does."""

    def _makeIndex(self):
        server, port = getTestMongoServerAndPort()
        mongo_config = McritConfig()
        mongo_config.STORAGE_CONFIG = StorageConfig(STORAGE_METHOD=StorageFactory.STORAGE_METHOD_MONGODB, STORAGE_SERVER=server, STORAGE_PORT=port, STORAGE_MONGODB_DBNAME=DB_NAME)
        mongo_config.QUEUE_CONFIG = QueueConfig(QUEUE_METHOD=QueueFactory.QUEUE_METHOD_MONGODB, QUEUE_SERVER=server, QUEUE_PORT=port, QUEUE_MONGODB_DBNAME=DB_NAME + "_queue")
        client = pymongo.MongoClient(server, int(port))
        for name in (DB_NAME, DB_NAME + "_queue"):
            client.drop_database(name)
            self.addCleanup(client.drop_database, name)
        index = MinHashIndex(config=mongo_config)
        self.worker = Worker(config=mongo_config)
        self.addCleanup(self.worker.queue.unregisterWorker)
        self.database = index.queue._getCollection().database
        return index

    def _runQueuedJobs(self):
        while True:
            job = self.worker.queue.next()
            if job is None:
                return
            self.worker._executeJob(job)

    def _setJobFields(self, job_id, **fields):
        self.index.queue._getCollection().update_one({"_id": ObjectId(job_id)}, {"$set": fields})

    def _fileMeta(self, file_id):
        document = self.database["fs.files"].find_one({"_id": ObjectId(file_id)})
        return document and document["metadata"]

    def _fileExists(self, file_id):
        return self.database["fs.files"].find_one({"_id": ObjectId(file_id)}) is not None and self.database["fs.chunks"].count_documents({"files_id": ObjectId(file_id)}) > 0

    def _deleteFile(self, file_id):
        self.index.queue._getFs().delete(ObjectId(file_id))

    def test_the_route_answers_for_jobs_a_worker_has_not_finished(self):
        app = falcon.App()
        app.add_route("/jobs/{job_id}/rerun", JobResource(self.index), suffix="rerun")
        client = falcon.testing.TestClient(app)
        queued = self.index.getMatchesForSample(self.sample_ids[0])
        self.assertEqual(falcon.HTTP_409, client.simulate_post(f"/jobs/{queued}/rerun").status)
        taken = self.worker.queue.next()
        assert taken is not None
        self.assertEqual(queued, str(taken.job_id))
        response = client.simulate_post(f"/jobs/{queued}/rerun")
        self.assertEqual(falcon.HTTP_409, response.status)
        self.assertIn("in progress", response.json["data"]["message"])
        self.worker._executeJob(taken)
        response = client.simulate_post(f"/jobs/{queued}/rerun", headers={"username": "bob"})
        self.assertEqual(falcon.HTTP_200, response.status)
        rerun = response.json["data"]
        self.assertEqual(("bob", True), (self._job(rerun).username, self._job(rerun).finished_at is None))
        self.assertEqual(falcon.HTTP_404, client.simulate_post(f"/jobs/{ObjectId()}/rerun").status)


class JobRerunRouteTest(unittest.TestCase):
    """POST /jobs/{job_id}/rerun answers the new job id like the other job-creating routes, 404 or 409 when it can't."""

    def setUp(self):
        self.index = MinHashIndex(config)
        self.index._storage.clearStorage()
        self.sample_id = self.index.addReport(load_report("example_report_3.smda"))["sample_info"]["sample_id"]
        app = falcon.App()
        app.add_route("/jobs/{job_id}/rerun", JobResource(self.index), suffix="rerun")
        self.client = falcon.testing.TestClient(app)

    def test_a_finished_analysis_answers_the_new_job_id(self):
        old = self.index.getMatchesForSample(self.sample_id, username="alice")
        response = self.client.simulate_post(f"/jobs/{old.upper()}/rerun", headers={"username": "bob"})
        self.assertEqual(falcon.HTTP_200, response.status)
        self.assertEqual("successful", response.json["status"])
        new = response.json["data"]
        self.assertRegex(new, "^[0-9a-f]{24}$")
        self.assertNotEqual(old, new)
        self.assertEqual("bob", self.index.getJob(new).username)

    def test_refusals(self):
        running = self.index.getMatchesForSample(self.sample_id)
        self.index.queue._jobs[running].update(finished_at=None, locked_by="Worker-elsewhere")
        minhashing = self.index.getQueueData(0, 0, method="updateMinHashesForSample")[0]["_id"]
        for label, job_id, status in (
            ("unknown", str(ObjectId()), falcon.HTTP_404),
            ("running", running, falcon.HTTP_409),
            ("no analysis", minhashing, falcon.HTTP_409),
            ("invalid id", "nope", falcon.HTTP_400),
        ):
            with self.subTest(label):
                count = self.index.getQueueCount()
                response = self.client.simulate_post(f"/jobs/{job_id}/rerun")
                self.assertEqual(status, response.status)
                self.assertEqual("failed", response.json["status"])
                self.assertTrue(response.json["data"]["message"])
                self.assertEqual(count, self.index.getQueueCount())

    def test_the_server_routes_the_rerun(self):
        index = MagicMock()
        index.rerunJob.return_value = "fedcba9876543210fedcba98"
        with patch.object(application_routes, "create_index", return_value=index):
            client = falcon.testing.TestClient(application_routes.get_app())
        response = client.simulate_post("/jobs/0123456789abcdef01234567/rerun", headers={"username": "alice"})
        self.assertEqual((falcon.HTTP_200, "fedcba9876543210fedcba98"), (response.status, response.json["data"]))
        index.rerunJob.assert_called_once_with("0123456789abcdef01234567", username="alice")
        self.assertEqual(falcon.HTTP_405, client.simulate_get("/jobs/0123456789abcdef01234567/rerun").status)


class JobRerunClientTest(unittest.TestCase):
    def _post(self, status, body):
        post = patch("mcrit.client.McritClient.requests.Session.post")
        mock = post.start()
        self.addCleanup(post.stop)
        mock.return_value.status_code = status
        mock.return_value.json.return_value = body
        return mock

    def test_the_client_posts_to_the_route_and_answers_the_new_job_id(self):
        post = self._post(200, {"status": "successful", "data": "fedcba9876543210fedcba98"})
        self.assertEqual("fedcba9876543210fedcba98", McritClient("http://mcrit.test", username="alice").requestJobRerun("0123456789abcdef01234567"))
        self.assertEqual("http://mcrit.test/jobs/0123456789abcdef01234567/rerun", post.call_args.args[0])
        self.assertEqual("alice", post.call_args.kwargs["headers"]["username"])

    def test_a_refusal_is_none_or_raised(self):
        for status, error in ((404, McritNotFound), (409, McritConflict)):
            with self.subTest(status=status):
                self._post(status, {"status": "failed", "data": {"message": "no"}})
                self.assertIsNone(McritClient("http://mcrit.test").requestJobRerun("0123456789abcdef01234567"))
                with self.assertRaises(error):
                    McritClient("http://mcrit.test", raise_client_errors=True).requestJobRerun("0123456789abcdef01234567")


if __name__ == "__main__":
    unittest.main()
