import json
import os
import unittest
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, patch

import falcon
import falcon.testing
import pymongo
import pymongo.errors
import pytest
from bson import ObjectId

from mcrit.client.McritClient import McritClient
from mcrit.config.QueueConfig import QueueConfig
from mcrit.index.MinHashIndex import MinHashIndex
from mcrit.libs.mongoqueue import MongoQueue
from mcrit.queue.LocalQueue import LocalQueue
from mcrit.server.StatusResource import StatusResource

from .context import config, getTestMongoServerAndPort


class QueueFileFixtures:
    """Jobs with file parameters and results, laid out as QueueRemoteCalls leaves them."""

    queue: Any

    def _put(self, payload) -> str:
        raise NotImplementedError

    def _setResult(self, job_id, result_id):
        raise NotImplementedError

    def _param(self, sha256=None, tmp_lock=0, jobs=()):
        return self.queue._file_to_grid(b"binary " * 1000, metadata={"sha256": sha256 or os.urandom(8).hex(), "tmp_lock": tmp_lock, "jobs": list(jobs)})

    def _job(self, *file_ids, with_result=True):
        payload = {
            "method": "getMatchesForUnmappedBinary",
            "descriptor": os.urandom(8).hex(),
            "file_params": json.dumps({str(index): file_id for index, file_id in enumerate(file_ids)}),
        }
        job_id = str(self._put(payload))
        if with_result:
            # the metadata QueueRemoteCalls writes: the id as the queue's Job hands it out, an ObjectId for MongoQueue
            job = self.queue.get_job(job_id)
            self._setResult(job_id, self.queue._dicts_to_grid({"report": job_id}, metadata={"result": True, "job": job.job_id}))
        return job_id


@pytest.mark.mongo
class MongoQueueFilesTest(QueueFileFixtures, unittest.TestCase):
    def setUp(self):
        queue_config = QueueConfig()
        queue_config.QUEUE_SERVER, queue_config.QUEUE_PORT = getTestMongoServerAndPort()
        queue_config.QUEUE_MONGODB_DBNAME = "test_queue_files"
        self.queue = MongoQueue(queue_config, "consumer_1")
        self.database = self.queue._getCollection().database
        server, port = getTestMongoServerAndPort()
        self.addCleanup(lambda: pymongo.MongoClient(server, int(port)).drop_database("test_queue_files"))

    def _put(self, payload):
        return self.queue.put(payload)

    def _setResult(self, job_id, result_id):
        self.queue._getCollection().update_one({"_id": ObjectId(job_id)}, {"$set": {"result": result_id, "finished_at": datetime.now()}})

    def _job(self, *file_ids, with_result=True):
        job_id = super()._job(*file_ids, with_result=with_result)
        self.database["fs.files"].update_many({"_id": {"$in": [ObjectId(file_id) for file_id in file_ids]}}, {"$addToSet": {"metadata.jobs": job_id}})
        return job_id

    def _counts(self):
        return self.database["fs.files"].count_documents({}), self.database["fs.chunks"].count_documents({})

    def test_deleting_a_job_deletes_its_files_with_their_chunks(self):
        job_id = self._job(self._param())
        self.assertEqual(1, self.queue.delete_job(job_id))
        self.assertEqual((0, 0), self._counts())

    def test_a_file_shared_with_another_job_stays_until_its_last_job_goes(self):
        shared = self._param()
        first, second = self._job(shared), self._job(shared)
        self.queue.delete_job(first)
        self.assertEqual([second], self.database["fs.files"].find_one({"_id": ObjectId(shared)})["metadata"]["jobs"])
        self.queue.delete_job(second)
        self.assertEqual((0, 0), self._counts())

    def test_a_file_claimed_by_a_new_submission_is_kept(self):
        claimed = self._param()
        job_id = self._job(claimed, with_result=False)
        self.database["fs.files"].update_one({"_id": ObjectId(claimed)}, {"$inc": {"metadata.tmp_lock": 1}})
        self.queue.delete_job(job_id)
        self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(claimed)}))

    def test_a_job_without_a_result_can_be_deleted(self):
        job_id = self._job(with_result=False)
        self.assertEqual(1, self.queue.delete_job(job_id))

    def test_deleting_jobs_by_selection_releases_their_files(self):
        shared, own = self._param(), self._param()
        self._job(shared, own)
        kept = self._put({"method": "other", "descriptor": "x", "file_params": json.dumps({"0": shared})})
        self.database["fs.files"].update_one({"_id": ObjectId(shared)}, {"$addToSet": {"metadata.jobs": str(kept)}})
        self.assertEqual(1, self.queue.delete_jobs(method="getMatchesForUnmappedBinary"))
        self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(shared)}))
        self.assertIsNone(self.database["fs.files"].find_one({"_id": ObjectId(own)}))
        self.assertEqual(0, self.database["fs.chunks"].count_documents({"files_id": ObjectId(own)}))

    def _leak(self):
        """What the old delete paths left behind, next to data that is still in use."""
        live_job = self._job(self._param())
        dead_job = str(ObjectId())
        leaked = {
            "result": self.queue._dicts_to_grid({"report": 1}, metadata={"result": True, "job": dead_job}),
            "file_param": self._param(jobs=[dead_job]),
            "unused_file_param": self._param(),
        }
        kept = {
            "locked_file_param": self._param(jobs=[dead_job], tmp_lock=1),
            "live_result": self.queue._getCollection().find_one({"_id": ObjectId(live_job)})["result"],
        }
        old_chunks, fresh_chunks = ObjectId.from_datetime(datetime.now(UTC) - timedelta(days=2)), ObjectId()
        self.database["fs.chunks"].insert_many(
            [{"files_id": old_chunks, "n": 0, "data": b"x"}, {"files_id": old_chunks, "n": 1, "data": b"x"}, {"files_id": fresh_chunks, "n": 0, "data": b"x"}]
        )
        return live_job, leaked, kept, old_chunks, fresh_chunks

    def test_a_dry_run_counts_the_leftovers_and_deletes_nothing(self):
        self._leak()
        before = self._counts()
        self.assertEqual({"dry_run": True, "results": 1, "file_params": 2, "chunk_files": 1}, self.queue.delete_orphaned_files(dry_run=True))
        self.assertEqual(before, self._counts())

    def test_the_leftovers_are_deleted_and_nothing_in_use(self):
        live_job, leaked, kept, old_chunks, fresh_chunks = self._leak()
        self.assertEqual({"dry_run": False, "results": 1, "file_params": 2, "chunk_files": 1}, self.queue.delete_orphaned_files())
        for name, file_id in leaked.items():
            with self.subTest(leaked=name):
                self.assertIsNone(self.database["fs.files"].find_one({"_id": ObjectId(file_id)}))
                self.assertEqual(0, self.database["fs.chunks"].count_documents({"files_id": ObjectId(file_id)}))
        for name, file_id in kept.items():
            with self.subTest(kept=name):
                self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(file_id)}))
        self.assertEqual(0, self.database["fs.chunks"].count_documents({"files_id": old_chunks}))
        # chunks this young may belong to a file GridFS is still writing
        self.assertEqual(1, self.database["fs.chunks"].count_documents({"files_id": fresh_chunks}))
        self.assertEqual({"report": live_job}, self.queue._grid_to_dicts(kept["live_result"]))
        self.assertEqual({"dry_run": False, "results": 0, "file_params": 0, "chunk_files": 0}, self.queue.delete_orphaned_files())

    def test_jobs_of_another_queue_in_the_database_keep_their_files(self):
        other_config = QueueConfig()
        other_config.QUEUE_SERVER, other_config.QUEUE_PORT = getTestMongoServerAndPort()
        other_config.QUEUE_MONGODB_DBNAME = "test_queue_files"
        other_config.QUEUE_MONGODB_COLLECTION_NAME = "other_queue"
        other_queue = MongoQueue(other_config, "consumer_2")
        other_job = str(other_queue.put({"method": "m", "descriptor": "other", "file_params": "{}"}))
        result = other_queue._dicts_to_grid({"report": 1}, metadata={"result": True, "job": other_queue.get_job(other_job).job_id})
        param = self._param(jobs=[other_job])
        self.assertEqual({"dry_run": False, "results": 0, "file_params": 0, "chunk_files": 0}, self.queue.delete_orphaned_files())
        self.assertEqual({"report": 1}, other_queue._grid_to_dicts(result))
        self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(param)}))

    def test_chunks_whose_file_document_appears_meanwhile_are_kept(self):
        # an old ObjectId, so that only the re-check right before deleting can keep these chunks
        files_id = ObjectId.from_datetime(datetime.now(UTC) - timedelta(days=2))
        self.database["fs.chunks"].insert_one({"files_id": files_id, "n": 0, "data": b"x"})
        batch = self.queue._deleteOrphanedFileBatch

        def document_arrives(*args):
            self.database["fs.files"].insert_one({"_id": files_id, "length": 1, "chunkSize": 261120, "metadata": {"result": True, "job": "none"}})
            return batch(*args)

        with patch.object(self.queue, "_deleteOrphanedFileBatch", side_effect=document_arrives):
            self.assertEqual(0, self.queue.delete_orphaned_files()["chunk_files"])
        self.assertEqual(1, self.database["fs.chunks"].count_documents({"files_id": files_id}))

    def test_bulk_deletion_leaves_a_matching_job_submitted_meanwhile(self):
        self._job(self._param())
        queue_collection = self.queue._getCollection()
        original_find = type(queue_collection).find
        arrived = []

        def find_then_submit(collection, *args, **kwargs):
            cursor = original_find(collection, *args, **kwargs)
            selection = args[0] if args else kwargs.get("filter")
            if collection.name != queue_collection.name or arrived or not isinstance(selection, dict) or "payload.method" not in selection:
                return cursor
            # delete_jobs listing its selection: a job of the same method is submitted right after
            documents = list(cursor)
            arrived.append(self._job(self._param()))
            return documents

        with patch.object(type(queue_collection), "find", autospec=True, side_effect=find_then_submit):
            deleted = self.queue.delete_jobs(method="getMatchesForUnmappedBinary")
        self.assertEqual(1, deleted)
        self.assertIsNotNone(self.queue.get_job(arrived[0]))

    def test_bulk_deletion_works_in_chunks_and_keeps_the_counters(self):
        for _ in range(3):
            self._job(self._param())
        self.queue.refreshCounters()
        counted_before = self._countedJobs()
        original_batched = MongoQueue._batched
        chunks = []

        def small_chunks(items, size):
            for chunk in original_batched(items, 2):
                chunks.append(len(chunk))
                yield chunk

        with patch.object(MongoQueue, "_batched", staticmethod(small_chunks)):
            self.assertEqual(3, self.queue.delete_jobs(method="getMatchesForUnmappedBinary"))
        self.assertEqual([2, 1], chunks)
        # the queue counters follow the deletion, as they do for delete_job
        self.assertEqual((3, 0), (counted_before, self._countedJobs()))
        self.assertEqual((0, 0), self._counts())

    def test_an_interrupted_deletion_leaves_no_job_without_its_result(self):
        job_id = self._job(self._param())
        fs = self.queue._getFs()
        with patch.object(fs, "delete", side_effect=pymongo.errors.AutoReconnect("gone")):
            with self.assertRaises(pymongo.errors.AutoReconnect):
                self.queue.delete_job(job_id)
        # the job is gone; what it left is for the sweep, which takes it
        self.assertIsNone(self.queue.get_job(job_id))
        report = self.queue.delete_orphaned_files()
        self.assertEqual((1, 1), (report["results"], report["file_params"]))

    def test_jobs_deleted_by_someone_else_meanwhile_are_counted_once(self):
        selected = [self._job(self._param()) for _ in range(2)]
        cut = datetime.now()
        # jobs of the same method outside the selection, so that the right count is not zero:
        # the counters stop at zero, which would hide a job subtracted twice
        for _ in range(3):
            self._job(self._param())
        self.queue._getCollection().update_many({"_id": {"$in": [ObjectId(job_id) for job_id in selected]}}, {"$set": {"created_at": cut - timedelta(seconds=10)}})
        self.queue.refreshCounters()
        collection = self.queue._getCollection()
        original_find = type(collection).find
        deleted_meanwhile = []

        def find_then_delete_one(coll, *args, **kwargs):
            cursor = original_find(coll, *args, **kwargs)
            selection = args[0] if args else kwargs.get("filter")
            if coll.name != collection.name or deleted_meanwhile or not isinstance(selection, dict) or "created_at" not in selection:
                return cursor
            documents = list(cursor)
            deleted_meanwhile.append(self.queue.delete_job(selected[0]))
            return documents

        with patch.object(type(collection), "find", autospec=True, side_effect=find_then_delete_one):
            with self.assertRaisesRegex(Exception, "unequal"):
                self.queue.delete_jobs(created_before=cut)
        self.assertEqual([1], deleted_meanwhile)
        self.assertEqual(3, self._countedJobs())

    def _counters(self, method="getMatchesForUnmappedBinary"):
        counters = self.queue._getQueueCounters().find_one({"name": method}) or {}
        return {state: counters.get(state, 0) for state in ("queued", "in_progress", "finished", "failed", "terminated")}

    def test_a_job_is_read_and_deleted_in_one_command(self):
        # read first and deleted after, a job a worker took in between is uncounted from the state it left
        job_id = self._job(with_result=False)
        collection = self.queue._getCollection()
        with patch.object(type(collection), "find_one", autospec=True, side_effect=AssertionError("read apart from the deletion")):
            self.assertEqual(1, self.queue.delete_job(job_id))

    def test_a_job_in_progress_is_uncounted_from_in_progress(self):
        job_id = self._job(with_result=False)
        self._job(self._param())
        self.queue.refreshCounters()
        self.assertEqual(job_id, str(self.queue.next().job_id))
        self.assertEqual(1, self.queue.delete_job(job_id))
        self.assertEqual({"queued": 0, "in_progress": 0, "finished": 1, "failed": 0, "terminated": 0}, self._counters())

    def test_jobs_deleted_by_someone_else_between_reading_and_deleting_a_chunk_are_counted_once(self):
        selected = [self._job(with_result=False) for _ in range(3)]
        cut = datetime.now()
        # queued jobs outside the selection keep the right count off zero, where the clamp would hide a double count
        for _ in range(2):
            self._job(with_result=False)
        self.queue._getCollection().update_many({"_id": {"$in": [ObjectId(job_id) for job_id in selected]}}, {"$set": {"created_at": cut - timedelta(seconds=10)}})
        self.queue.refreshCounters()
        collection = self.queue._getCollection()
        original_delete_many = type(collection).delete_many
        deleted_meanwhile = []

        def delete_one_first(coll, *args, **kwargs):
            if coll.name == collection.name and not deleted_meanwhile:
                deleted_meanwhile.append(self.queue.delete_job(selected[0]))
            return original_delete_many(coll, *args, **kwargs)

        with patch.object(type(collection), "delete_many", autospec=True, side_effect=delete_one_first):
            with self.assertRaisesRegex(Exception, "unequal"):
                self.queue.delete_jobs(created_before=cut)
        self.assertEqual([1], deleted_meanwhile)
        self.assertEqual({"queued": 2, "in_progress": 0, "finished": 0, "failed": 0, "terminated": 0}, self._counters())

    def test_a_job_taken_while_earlier_chunks_are_deleted_is_uncounted_from_where_it_is(self):
        selected = [self._job(with_result=False) for _ in range(3)]
        cut = datetime.now()
        # finished jobs outside the selection, which next() does not hand out, keep the counts off zero
        for _ in range(2):
            self._job(self._param())
        self.queue._getCollection().update_many({"_id": {"$in": [ObjectId(job_id) for job_id in selected]}}, {"$set": {"created_at": cut - timedelta(seconds=10)}})
        self.queue.refreshCounters()
        original_batched = MongoQueue._batched
        taken = []

        def take_before_the_second_chunk(items, size):
            for index, chunk in enumerate(original_batched(items, 2)):
                if index == 1:
                    taken.append(str(self.queue.next().job_id))
                yield chunk

        with patch.object(MongoQueue, "_batched", staticmethod(take_before_the_second_chunk)):
            self.assertEqual(3, self.queue.delete_jobs(created_before=cut))
        self.assertEqual([selected[2]], taken)
        self.assertEqual({"queued": 0, "in_progress": 0, "finished": 2, "failed": 0, "terminated": 0}, self._counters())

    def test_refreshing_counts_a_method_without_jobs_as_zero(self):
        self._job(self._param())
        self.queue.refreshCounters()
        self.assertEqual(1, self._counters()["finished"])
        # gone without going through the counters, as a crashed deletion would leave it
        self.queue._getCollection().delete_many({})
        self.queue.refreshCounters()
        self.assertEqual({"queued": 0, "in_progress": 0, "finished": 0, "failed": 0, "terminated": 0}, self._counters())
        self.assertIsNotNone(self.queue._getQueueCounters().find_one({"name": "workers"}))

    def test_refreshing_leaves_documents_that_are_not_method_counters_alone(self):
        self.queue._getQueueCounters().insert_one({"name": None, "queued": 5})
        self.queue.refreshCounters()
        last_updated = self.queue._getQueueCounters().find_one({"last_updated": {"$ne": None}})
        self.assertNotIn("queued", last_updated)

    def test_refreshing_keeps_the_count_of_a_method_whose_first_job_arrives_during_the_scan(self):
        self._job(self._param())
        collection = self.queue._getCollection()
        original_find = type(collection).find

        def scan_then_submit(coll, *args, **kwargs):
            cursor = original_find(coll, *args, **kwargs)
            if coll.name != collection.name or args or kwargs:
                return cursor
            documents = list(cursor)
            self.queue.put({"method": "getMatchesForSample", "descriptor": "meanwhile", "file_params": "{}"})
            return documents

        with patch.object(type(collection), "find", autospec=True, side_effect=scan_then_submit):
            self.queue.refreshCounters()
        self.assertEqual(1, self._counters("getMatchesForSample")["queued"])

    def _countedJobs(self):
        counters = self.queue._getQueueCounters().find_one({"name": "getMatchesForUnmappedBinary"}) or {}
        return sum(value for key, value in counters.items() if key in ("queued", "in_progress", "finished", "failed", "terminated"))

    def test_a_file_used_by_any_existing_job_is_kept(self):
        live_job, dead_job = self._job(), str(ObjectId())
        for jobs in ([live_job, dead_job], [dead_job, live_job]):
            with self.subTest(jobs=jobs):
                file_id = self._param(jobs=jobs)
                self.assertEqual(0, self.queue.delete_orphaned_files()["file_params"])
                self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(file_id)}))

    def test_a_submission_during_the_sweep_keeps_its_file(self):
        """A job linked to a file by the time the files are read must be seen as existing: the job
        ids are looked up after the scan, never taken from before it."""
        claimed = self._param()
        real_files = self.queue._getFsFiles()

        class ScanAfterSubmission:
            def __getattr__(inner, name):
                return getattr(real_files, name)

            def find(inner, *args, **kwargs):
                # what upload_file_params and add_job_id_to_file do for a new job: create it, then link the file
                job_id = str(self._put({"method": "m", "descriptor": "new", "file_params": json.dumps({"0": claimed})}))
                real_files.update_one({"_id": ObjectId(claimed)}, {"$addToSet": {"metadata.jobs": job_id}})
                return real_files.find(*args, **kwargs)

        with patch.object(self.queue, "_getFsFiles", return_value=ScanAfterSubmission()):
            self.assertEqual(0, self.queue.delete_orphaned_files()["file_params"])
        self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(claimed)}))

    def test_a_file_claimed_after_it_was_found_unused_is_kept(self):
        claimed = self._param()
        retire = self.queue._retireAndDeleteFile

        def claim_first(file_id, still_unused):
            self.database["fs.files"].update_one({"_id": file_id}, {"$inc": {"metadata.tmp_lock": 1}})
            return retire(file_id, still_unused)

        with patch.object(self.queue, "_retireAndDeleteFile", side_effect=claim_first):
            self.assertEqual(0, self.queue.delete_orphaned_files()["file_params"])
        self.assertIsNotNone(self.database["fs.files"].find_one({"_id": ObjectId(claimed)}))

    def test_a_file_is_unclaimable_before_it_is_deleted(self):
        sha256 = "ab" * 32
        self._param(sha256=sha256)
        fs = self.queue._getFs()
        delete = fs.delete
        seen = []

        def delete_checking_claims(file_id):
            seen.append(self.queue.get_file_by_hash_inc_lock(sha256))
            delete(file_id)

        with patch.object(fs, "delete", side_effect=delete_checking_claims):
            self.assertEqual(1, self.queue.delete_orphaned_files()["file_params"])
        self.assertEqual([None], seen)


class LocalQueueFilesTest(QueueFileFixtures, unittest.TestCase):
    def setUp(self):
        self.queue = LocalQueue()
        self.queue.set_worker(MagicMock())

    def _put(self, payload):
        return self.queue.put(payload)

    def _setResult(self, job_id, result_id):
        self.queue._jobs[job_id]["result"] = result_id

    def _job(self, *file_ids, with_result=True):
        job_id = super()._job(*file_ids, with_result=with_result)
        for file_id in file_ids:
            self.queue._files_meta[file_id]["jobs"].append(job_id)
        return job_id

    def test_deleting_a_job_deletes_the_files_only_it_used(self):
        shared, own = self._param(), self._param()
        first, second = self._job(shared, own), self._job(shared)
        self.queue.delete_job(first)
        self.assertNotIn(own, self.queue._files)
        self.assertEqual([second], self.queue._files_meta[shared]["jobs"])

    def test_leftovers_are_counted_then_deleted(self):
        live_job = self._job(self._param())
        dead_job = "0" * 24
        orphans = [self.queue._dicts_to_grid({"report": 1}, metadata={"result": True, "job": dead_job}), self._param(jobs=[dead_job])]
        locked = self._param(jobs=[dead_job], tmp_lock=1)
        self.assertEqual({"dry_run": True, "results": 1, "file_params": 1, "chunk_files": 0}, self.queue.delete_orphaned_files(dry_run=True))
        self.assertTrue(all(orphan in self.queue._files for orphan in orphans))
        self.assertEqual({"dry_run": False, "results": 1, "file_params": 1, "chunk_files": 0}, self.queue.delete_orphaned_files())
        self.assertFalse(any(orphan in self.queue._files for orphan in orphans))
        self.assertIn(locked, self.queue._files)
        self.assertIsNotNone(self.queue._grid_to_dicts(self.queue._jobs[live_job]["result"]))

    def test_a_claimed_file_stays_with_its_last_job_deleted(self):
        claimed = self._param()
        job_id = self._job(claimed, with_result=False)
        self.queue._files_meta[claimed]["tmp_lock"] = 1
        self.queue.delete_job(job_id)
        self.assertIn(claimed, self.queue._files)

    def test_a_job_without_a_result_can_be_deleted(self):
        job_id = self._job(self._param(), with_result=False)
        self.assertEqual(1, self.queue.delete_job(job_id))
        self.assertEqual({}, dict(self.queue._files))


class DeleteOrphanedQueueFilesJobTest(unittest.TestCase):
    def test_the_job_runs_on_the_worker_and_answers_the_report(self):
        index = MinHashIndex(config)
        orphan = index.queue._dicts_to_grid({"report": 1}, metadata={"result": True, "job": "0" * 24})
        dry_job = index.deleteOrphanedQueueFiles(True, force_recalculation=True)
        self.assertEqual({"dry_run": True, "results": 1, "file_params": 0, "chunk_files": 0}, index.getResultForJob(dry_job))
        self.assertIn(orphan, index.queue._files)
        real_job = index.deleteOrphanedQueueFiles(False, force_recalculation=True)
        self.assertEqual({"dry_run": False, "results": 1, "file_params": 0, "chunk_files": 0}, index.getResultForJob(real_job))
        self.assertNotIn(orphan, index.queue._files)


class DeleteOrphanedQueueFilesRouteTest(unittest.TestCase):
    def test_the_route_schedules_the_job_with_dry_run_as_asked(self):
        for query, dry_run in (("dry_run=true", True), ("dry_run=false", False), ("dry_run=TRUE", True)):
            with self.subTest(query=query):
                index = MagicMock()
                index.deleteOrphanedQueueFiles.return_value = "0123456789abcdef01234567"
                app = falcon.App()
                app.add_route("/delete_orphaned_queue_files", StatusResource(index), suffix="delete_orphaned_queue_files")
                response = falcon.testing.TestClient(app).simulate_post("/delete_orphaned_queue_files", query_string=query)
                self.assertEqual(falcon.HTTP_200, response.status)
                self.assertEqual("0123456789abcdef01234567", response.json["data"])
                index.deleteOrphanedQueueFiles.assert_called_once_with(dry_run, force_recalculation=True)

    def test_anything_but_true_or_false_in_the_query_string_is_refused(self):
        # missing (including sent in the body, which is not read), repeated, or not a boolean
        for query, body in (("", None), ("", "dry_run=true"), ("dry_run=true&dry_run=true", None), ("dry_run=1", None), ("dry_run=yes", None)):
            with self.subTest(query=query, body=body):
                index = MagicMock()
                app = falcon.App()
                app.add_route("/delete_orphaned_queue_files", StatusResource(index), suffix="delete_orphaned_queue_files")
                headers = {"Content-Type": "application/x-www-form-urlencoded"} if body else None
                response = falcon.testing.TestClient(app).simulate_post("/delete_orphaned_queue_files", query_string=query, body=body, headers=headers)
                self.assertEqual(falcon.HTTP_400, response.status)
                index.deleteOrphanedQueueFiles.assert_not_called()

    def test_the_client_posts_to_the_route(self):
        with patch("mcrit.client.McritClient.requests.post") as post:
            post.return_value.status_code = 200
            post.return_value.json.return_value = {"status": "successful", "data": "0123456789abcdef01234567"}
            self.assertEqual("0123456789abcdef01234567", McritClient("http://mcrit.test").deleteOrphanedQueueFiles(dry_run=True))
        self.assertEqual("http://mcrit.test/delete_orphaned_queue_files", post.call_args.args[0])
        self.assertEqual({"dry_run": "true"}, post.call_args.kwargs["params"])


if __name__ == "__main__":
    unittest.main()
