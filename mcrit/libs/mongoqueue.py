#   Copyright 2012 Kapil Thangavelu
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import json
import logging
import re
import threading
import time
import traceback
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import gridfs
import pymongo
from bson.errors import InvalidId
from bson.objectid import ObjectId
from bson.regex import Regex
from pymongo import MongoClient, ReturnDocument, UpdateOne

LOGGER = logging.getLogger(__name__)

DEFAULT_INSERT: Dict[str, Any] = {
    "locked_by": None,
    "locked_at": None,
    "last_error": None,
    "finished_at": None,
    "created_at": None,
    "started_at": None,
    "terminated": False,
    "required_by": [],
    "number": None,
    "result": None,
    "notify_done": False,
}


# added to track incremental job numbers via MCRIT storage (apart from OIDs)
def _useCounter(database, name: str) -> int:
    result = database.counters.find_one_and_update(filter={"name": name}, update={"$inc": {"value": 1}}, upsert=True)
    if result is None:
        return 0
    return result["value"]


class MongoQueue:
    """A queue class"""

    def __init__(self, queue_config, consumer_id, timeout=300, max_attempts=3):
        """ """
        self.queue_config = queue_config
        self.collection = None
        self.queue_counters = None
        self.queue_counters_initialized = False
        self.consumer_id = consumer_id
        self.timeout = timeout
        self.max_attempts = max_attempts
        self._default_insert = dict(DEFAULT_INSERT)
        self._default_insert["attempts_left"] = max_attempts
        self.fs = None
        self.fs_files = None
        self.cache_time: float = 10**9
        # overridden by QueueFactory from QUEUE_CLEAN_INTERVAL
        self.clean_interval: float = queue_config.QUEUE_CLEAN_INTERVAL
        # worker liveness (#150): a worker refreshes its heartbeat while polling, and a
        # registered worker whose heartbeat is older than the timeout counts as dead, so
        # the jobs it holds can be reclaimed. Both are throttled off the poll loop.
        self.heartbeat_interval: float = max(1.0, self.timeout / 3)
        self._last_heartbeat: float = 0.0
        # the heartbeat has to keep going while a job runs, and jobs run synchronously in the
        # poll loop (a job longer than the timeout would otherwise look dead and be reclaimed):
        # a daemon thread refreshes it from registration to unregistration
        self._heartbeat_stop: Optional[threading.Event] = None
        self._heartbeat_thread: Optional[threading.Thread] = None
        self._last_reclaim: float = 0.0

    def _getCollection(self):
        # because of gunicorn and forking workers, we want to delay creation of MongoClient until actual usage and avoid it within __init__()
        if self.collection is None:
            userpw_url = (
                f"{self.queue_config.QUEUE_MONGODB_USERNAME}:{self.queue_config.QUEUE_MONGODB_PASSWORD}@"
                if self.queue_config.QUEUE_MONGODB_USERNAME
                and len(self.queue_config.QUEUE_MONGODB_USERNAME) > 0
                and self.queue_config.QUEUE_MONGODB_PASSWORD
                and len(self.queue_config.QUEUE_MONGODB_PASSWORD) > 0
                else ""
            )
            port_url = f":{self.queue_config.QUEUE_PORT}" if self.queue_config.QUEUE_PORT else ""
            flags_url = f"?{self.queue_config.QUEUE_MONGODB_FLAGS}" if self.queue_config.QUEUE_MONGODB_FLAGS and len(self.queue_config.QUEUE_MONGODB_FLAGS) > 0 else ""

            mongo_uri = f"mongodb://{userpw_url}{self.queue_config.QUEUE_SERVER}{port_url}/{self.queue_config.QUEUE_MONGODB_DBNAME}{flags_url}"

            db = MongoClient(mongo_uri, connect=False)
            self.collection = db[self.queue_config.QUEUE_MONGODB_DBNAME][self.queue_config.QUEUE_MONGODB_COLLECTION_NAME]
            self.fs = gridfs.GridFS(self.collection.database)
            self.fs_files = self.collection.database["fs.files"]
            self.queue_counters = db[self.queue_config.QUEUE_MONGODB_DBNAME][self.queue_config.QUEUE_MONGODB_COLLECTION_NAME + "_counters"]
            self.queue_counters_initialized = self.queue_counters.find_one({"last_updated": {"$ne": None}})
            if not self.queue_counters_initialized:
                self.refreshCounters()
            self.registerWorker()
            self._ensure_indices()
        return self.collection

    def _getFs(self):
        if self.fs is None:
            collection = self._getCollection()
            self.fs = gridfs.GridFS(collection.database)
            self.fs_files = collection.database["fs.files"]
        return self.fs

    def _getFsFiles(self):
        if self.fs_files is None:
            collection = self._getCollection()
            self.fs = gridfs.GridFS(collection.database)
            self.fs_files = collection.database["fs.files"]
        return self.fs_files

    def _getQueueCounters(self):
        self._getCollection()
        assert self.queue_counters is not None
        return self.queue_counters

    def _ensure_indices(self):
        # should only be called, after self.collection has been initiated
        collection = self.collection
        assert collection is not None
        collection.create_index("payload.method")
        collection.create_index("payload.descriptor")
        # serves the polling query in next() and _jobs_to_do(): equality on locked_by/finished_at, then the sort keys
        collection.create_index(
            [
                ("locked_by", pymongo.ASCENDING),
                ("finished_at", pymongo.ASCENDING),
                ("priority", pymongo.DESCENDING),
                ("created_at", pymongo.ASCENDING),
            ]
        )
        self._getFsFiles().create_index("metadata.sha256")

    def _identifyJobState(self, doc):
        # started_at and not finished_at or terminated -> in_progress
        if doc["started_at"] and doc["locked_by"] and not (doc["finished_at"] or doc["terminated"]):
            return "in_progress"
        # attempts_left == 0 and not finished_at and not terminated -> failed
        elif doc["attempts_left"] == 0 and not doc["finished_at"] and not doc["terminated"]:
            return "failed"
        # not finished_at, not locked_by -> queued
        elif not doc["finished_at"] and not doc["locked_by"] and not doc["terminated"]:
            return "queued"
        # finished_at and not terminated -> finished
        elif doc["finished_at"] and not doc["terminated"]:
            return "finished"
        # terminated -> terminated
        elif doc["terminated"]:
            return "terminated"
        return "unknown"

    def getQueueStatistics(self, refresh=False):
        self._getCollection()
        if refresh:
            self.refreshCounters()
        statistics = {}
        for doc in self._getQueueCounters().find({}, {"_id": 0}):
            if "name" in doc and doc["name"] != "workers":
                name = doc.pop("name")
                statistics[name] = doc
        return statistics

    def refreshCounters(self):
        aggregated = {}
        for doc in self._getCollection().find():
            method = doc["payload"]["method"]
            if method not in aggregated:
                aggregated[method] = {"queued": 0, "failed": 0, "in_progress": 0, "finished": 0, "terminated": 0}
            state = self._identifyJobState(doc)
            aggregated[method][state] += 1

        operations = [UpdateOne({"name": key}, {"$set": counters}, upsert=True) for key, counters in aggregated.items()]
        operations.append(UpdateOne({"last_updated": {"$ne": None}}, {"$set": {"last_updated": datetime.now()}}, upsert=True))
        if operations:
            self._getQueueCounters().bulk_write(operations)

    def updateQueueCounter(self, method, state, value):
        self.updateQueueCounters([(method, state, value)])
        return

    def updateQueueCounters(self, updates):
        self._getCollection()
        operations = []
        # Aggregate increments by method to minimize operations per document
        aggregated_incs = {}
        for method, state, value in updates:
            if method not in aggregated_incs:
                aggregated_incs[method] = {}
            aggregated_incs[method][state] = aggregated_incs[method].get(state, 0) + value

        for method, states in aggregated_incs.items():
            operations.append(UpdateOne({"name": method}, {"$inc": states}, upsert=True))
            for state, total_value in states.items():
                if total_value < 0:
                    operations.append(UpdateOne({"name": method, state: {"$lt": 0}}, {"$set": {state: 0}}))

        if not operations:
            return

        operations.append(UpdateOne({"last_updated": {"$ne": None}}, {"$set": {"last_updated": datetime.now()}}, upsert=True))
        self._getQueueCounters().bulk_write(operations)

    def registerWorker(self):
        if self.consumer_id != "index":
            self._getQueueCounters().find_one_and_update(
                {"name": "workers"},
                {"$addToSet": {"workers": self.consumer_id}, "$set": {f"heartbeats.{self.consumer_id}": datetime.now()}},
                upsert=True,
            )
            self._last_heartbeat = time.monotonic()
            self._startHeartbeatThread()
        self.release_orphaned_jobs()

    def _startHeartbeatThread(self):
        if self._heartbeat_thread is not None and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = threading.Thread(target=self._heartbeatLoop, name=f"heartbeat-{self.consumer_id}", daemon=True)
        self._heartbeat_thread.start()

    def _heartbeatLoop(self):
        stop = self._heartbeat_stop
        assert stop is not None
        while not stop.wait(self.heartbeat_interval):
            try:
                self.heartbeat(force=True)
            except Exception:
                LOGGER.warning("Heartbeat of %s could not be written.", self.consumer_id, exc_info=True)

    def _stopHeartbeatThread(self):
        if self._heartbeat_stop is not None:
            self._heartbeat_stop.set()
        if self._heartbeat_thread is not None:
            self._heartbeat_thread.join(timeout=self.heartbeat_interval + 1)
        self._heartbeat_thread = None

    def unregisterWorker(self):
        self._stopHeartbeatThread()
        if self.queue_counters is not None:
            self._getQueueCounters().find_one_and_update(
                {"name": "workers"},
                {"$pull": {"workers": self.consumer_id}, "$unset": {f"heartbeats.{self.consumer_id}": ""}},
                upsert=True,
            )

    def heartbeat(self, force=False):
        """Record that this worker is alive. Throttled to heartbeat_interval unless forced."""
        if self.consumer_id == "index":
            return
        if not force and time.monotonic() - self._last_heartbeat < self.heartbeat_interval:
            return
        # re-adds the registration as well: a worker judged dead by mistake (paused, a stalled
        # database connection) lost its current job to a reclaim, but must serve the next ones
        self._getQueueCounters().find_one_and_update(
            {"name": "workers"}, {"$addToSet": {"workers": self.consumer_id}, "$set": {f"heartbeats.{self.consumer_id}": datetime.now()}}, upsert=True
        )
        self._last_heartbeat = time.monotonic()

    def _live_worker_ids(self, now=None):
        """The registered workers whose heartbeat is younger than the timeout.

        A registered worker without any heartbeat is either a process from before heartbeats
        existed (a rolling upgrade: it may well be busy with a job) or one that died before
        writing its first one. It is given the benefit of the doubt once: a heartbeat is
        stamped for it now, so it counts as alive for one timeout and is judged like every
        other worker after that. A worker killed with SIGKILL never unregisters, so
        registration alone is not liveness (#150).
        """
        now = now or datetime.now()
        registration = self._getQueueCounters().find_one({"name": "workers"}, {"workers": 1, "heartbeats": 1, "_id": 0})
        if not registration:
            return set()
        heartbeats = registration.get("heartbeats") or {}
        live = set()
        for worker_id in registration.get("workers") or []:
            last_seen = heartbeats.get(worker_id)
            if last_seen is None:
                # only if still absent: two pollers must not keep re-stamping each other's view
                self._getQueueCounters().update_one({"name": "workers", f"heartbeats.{worker_id}": {"$exists": False}}, {"$set": {f"heartbeats.{worker_id}": now}})
                live.add(worker_id)
            elif now - last_seen < timedelta(seconds=self.timeout):
                live.add(worker_id)
        return live

    def _housekeeping(self):
        """Called off the poll loop: refresh our heartbeat, reclaim what dead workers hold."""
        self.heartbeat()
        if time.monotonic() - self._last_reclaim >= self.timeout:
            self.release_orphaned_jobs()
            self._last_reclaim = time.monotonic()

    def close(self):
        """Close the in memory queue connection."""
        self._getCollection().connection.close()

    def clear(self):
        """Clear the queue."""
        self._getCollection().database["fs"].drop()
        self._getCollection().drop()
        self._ensure_indices()

    def size(self):
        """Total size of the queue"""
        return self._getCollection().count_documents({"finished_at": None})

    def size_inc_finished(self):
        """Total size of the queue"""
        return self._getCollection().count_documents(filter={})

    def repair(self):
        """Clear out stale locks.

        Increments per job attempt counter.

        NOTE: this currently has no callers. Before wiring it up, be aware that the
        staleness threshold only means what it says for jobs that refresh their lock:
        `locked_at` is only bumped by Job.progressor(), and a matcher running as a single
        batch never steps its progress reporter, so a long job can look "stale" while it
        is healthily running. Reclaiming it would decrement attempts_left underneath a
        live worker. release_orphaned_jobs() tests the worker's liveness instead (#150).
        """
        self._getCollection().find_one_and_update(
            # timedelta()'s first positional argument is DAYS: the timeout, which is
            # seconds everywhere else (QUEUE_TIMEOUT, default 300), has to be named
            filter={"locked_by": {"$ne": None}, "locked_at": {"$lt": datetime.now() - timedelta(seconds=self.timeout)}},
            update={"$set": {"locked_by": None, "locked_at": None}, "$inc": {"attempts_left": -1}},
        )

    def drop_max_attempts(self):
        """ """
        self._getCollection().find_one_and_update({"attempts_left": {"$lte": 0}}, remove=True)

    def put(self, payload, priority=0, await_jobs: List[str] = [], username=None):
        """Place a job into the queue; username records who asked for it (fkie-cad/mcritweb#37)"""
        job = dict(self._default_insert)
        job["number"] = _useCounter(self._getCollection().database, "job")
        job["created_at"] = datetime.now()
        job["priority"] = priority
        job["payload"] = payload
        job["username"] = username
        await_jobs_set = set(await_jobs)
        job["unfinished_dependencies"] = list(await_jobs_set)
        job["all_dependencies"] = list(await_jobs_set)
        insert_result = self._getCollection().insert_one(job)
        if insert_result.acknowledged:
            job_id = insert_result.inserted_id
            for child_job_id in await_jobs_set:
                self._notify_on_done(child_job_id, job_id)
            self.updateQueueCounter(payload["method"], "queued", 1)
            return job_id
        return None

    def _notify_on_done(self, notifying_job_id: str, notified_job_id: str):
        self._getCollection().find_one_and_update(filter={"_id": ObjectId(notifying_job_id)}, update={"$push": {"required_by": notified_job_id}})
        notifying_job = self._getCollection().find_one({"_id": ObjectId(notifying_job_id)})
        if notifying_job["notify_done"]:
            self._getCollection().find_one_and_update(
                filter={"_id": ObjectId(notified_job_id)},
                update={"$pull": {"unfinished_dependencies": notifying_job_id}},
            )
        #     # atomically
        #     # should handle already deleted deps gracefully
        #     remove job.id from job_to_notify.unfinished_deps

    def _notify_dependent_jobs(self, job_id: str):
        job = self._getCollection().find_one_and_update(
            filter={"_id": ObjectId(job_id)},
            update={"$set": {"notify_done": True}},
        )
        for job_id_to_notify in job["required_by"]:
            self._getCollection().find_one_and_update(
                filter={"_id": ObjectId(job_id_to_notify)},
                update={"$pull": {"unfinished_dependencies": job_id}},
            )

    def next(self):
        self._getCollection()
        self._housekeeping()
        current_time = datetime.now()
        job = self._getCollection().find_one_and_update(
            filter={
                # locked_by is the authoritative lock; locked_at is only its heartbeat
                # timestamp. Requiring locked_at: None as well would starve any job left in
                # a half-released state by the progressor/release race (#106 analysis).
                "locked_by": None,
                "attempts_left": {"$gt": 0},
                "finished_at": None,
                "unfinished_dependencies": [],
            },
            update={"$set": {"locked_by": self.consumer_id, "locked_at": current_time, "started_at": current_time}},
            sort=[("priority", pymongo.DESCENDING), ("created_at", pymongo.ASCENDING)],
            new=1,
            # limit=1
        )
        if job:
            self.updateQueueCounters([(job["payload"]["method"], "in_progress", 1), (job["payload"]["method"], "queued", -1)])
        return self._wrap_one(job)

    def _jobs_to_do(self):
        return self._getCollection().find(
            filter={"locked_by": None, "attempts_left": {"$gt": 0}, "finished_at": None},
            sort=[("priority", pymongo.DESCENDING)],
        )

    def _jobs_in_progress(self):
        return self._getCollection().find(
            filter={
                "locked_by": {"$ne": None},
                "locked_at": {"$ne": None},
                "attempts_left": {"$gt": 0},
                "finished_at": None,
            },
            sort=[("priority", pymongo.DESCENDING)],
        )

    def _jobs_given_up(self):
        return self._getCollection().find(
            filter={"attempts_left": {"$le": 0}, "finished_at": None},
            sort=[("priority", pymongo.DESCENDING)],
        )

    def _jobs_finished(self):
        return self._getCollection().findOne(
            filter={"finished_at": {"$ne": None}},
            sort=[("priority", pymongo.DESCENDING)],
        )

    def _wrap_one(self, data):
        return data and Job(self, data) or None

    def stats(self):
        """Get statistics on the queue.

        Use sparingly requires a collection lock.
        """
        queries = []
        queries.append({"locked_by": None, "attempts_left": {"$gt": 0}})
        queries.append({"locked_by": {"$ne": None}})
        queries.append({"attempts_left": {"$lte": 0}})
        queries.append({})
        counts = [self._getCollection().count_documents(q) for q in queries]

        return dict(zip(["available", "locked", "errors", "total"], counts))

    # The states of _identifyJobState as queries, in the same order of precedence: each query
    # excludes what an earlier branch would have claimed, so a document lands in exactly one.
    _STATE_QUERIES = {
        "in_progress": {"started_at": {"$ne": None}, "locked_by": {"$ne": None}, "finished_at": None, "terminated": False},
        "failed": {"attempts_left": 0, "finished_at": None, "terminated": False, "$or": [{"started_at": None}, {"locked_by": None}]},
        "queued": {"attempts_left": {"$ne": 0}, "finished_at": None, "locked_by": None, "terminated": False},
        "finished": {"finished_at": {"$ne": None}, "terminated": False},
        "terminated": {"terminated": True},
    }

    @classmethod
    def _job_query(cls, method=None, state=None, filter=None, username=None, sample_ids: Optional[List[int]] = None, job_ids: Optional[List[str]] = None) -> dict:
        """The query behind get_jobs and get_job_count, so that paging, filtering and counting
        all see the same set of documents (fkie-cad/mcritweb#57): a text filter used to be applied to a page
        after it had been cut, which answered "the matches among jobs 0-24" instead of "the
        first 25 matches", and a state used to be decided in Python per document."""
        conditions: List[dict] = []
        if sample_ids is not None:
            conditions.append(cls._sample_ids_condition(method, sample_ids))
        elif method is not None:
            conditions.append({"payload.method": method})
        if job_ids is not None:
            valid_ids = []
            for job_id in job_ids:
                try:
                    valid_ids.append(ObjectId(job_id))
                except (InvalidId, TypeError):
                    continue
            # an empty $in matches nothing, which is exactly "present but no valid id"
            conditions.append({"_id": {"$in": valid_ids}})
        if state is not None:
            # an unknown state names no job, as it never did
            conditions.append(dict(cls._STATE_QUERIES.get(state, {"_id": {"$exists": False}})))
        if filter:
            # what the job's parameters rendering is made of: the method name and the
            # serialized parameters
            pattern = re.compile(re.escape(filter), re.IGNORECASE)
            conditions.append({"$or": [{"payload.method": pattern}, {"payload.params": pattern}]})
        if username is not None:
            conditions.append({"username": username})
        if not conditions:
            return {}
        return {"$and": conditions}

    @staticmethod
    def _sample_ids_condition(method, sample_ids: List[int]) -> dict:
        """Selects the jobs of <method> whose first positional argument is one of <sample_ids>.

        It matches payload.descriptor: rearrange_params stores positional arguments under "0",
        "1", ... and keyword names sort after digits, so get_descriptor's json.dumps(sort_keys=True)
        always puts "0" right after the method name. Each id becomes two anchored regexes that are
        literal to their end, one for an argument followed by another and one for the last, so each
        bounds its own range of the payload.descriptor index; a single regex with an alternation
        gets no bounds and is tested against every key. The literal prefix pins the method, so
        payload.method is not queried as well.
        """
        selected_ids: Dict[int, None] = {}
        for sample_id in sample_ids:
            try:
                selected_ids[int(sample_id)] = None
            except (TypeError, ValueError):
                continue
        if method is None or not selected_ids:
            # no method to anchor the regexes on, or no id that parses: select nothing
            return {"_id": {"$exists": False}}
        prefix = '^\\["%s", \\{"0": ' % re.escape(method)
        return {"payload.descriptor": {"$in": [Regex("%s%d%s" % (prefix, sample_id, end)) for sample_id in selected_ids for end in (",", "\\}")]}}

    def get_jobs(
        self,
        start_index: int,
        limit: int,
        method=None,
        state=None,
        ascending=False,
        filter=None,
        username=None,
        sample_ids: Optional[List[int]] = None,
        job_ids: Optional[List[str]] = None,
    ) -> List["Job"]:
        query = self._job_query(method=method, state=state, filter=filter, username=username, sample_ids=sample_ids, job_ids=job_ids)
        cursor = self._getCollection().find(query, sort=[("_id", 1 if ascending else -1)]).skip(start_index).limit(limit)
        return [self._wrap_one(job_document) for job_document in cursor]

    def get_job_count(self, method=None, state=None, filter=None, username=None, sample_ids: Optional[List[int]] = None, job_ids: Optional[List[str]] = None) -> int:
        return self._getCollection().count_documents(self._job_query(method=method, state=state, filter=filter, username=username, sample_ids=sample_ids, job_ids=job_ids))

    def get_job(self, job_id):
        job_id = ObjectId(job_id)
        return self._wrap_one(self._getCollection().find_one({"_id": job_id}))

    def delete_job(self, job_id, with_result=True):
        job_id = ObjectId(job_id)
        deletable_job = self._getCollection().find_one({"_id": job_id})
        if deletable_job:
            self.updateQueueCounter(deletable_job["payload"]["method"], self._identifyJobState(deletable_job), -1)
            # if job has file parameters, we need to remove them from GridFS as well
            print(deletable_job)
            if "file_params" in deletable_job["payload"]:
                file_params_dict = json.loads(deletable_job["payload"]["file_params"])
                for _, file_object_id in file_params_dict.items():
                    file_object_id = ObjectId(file_object_id)
                    # update gridFs entry of file to not link
                    # to this job anymore
                    self._getFsFiles().update_one({"_id": file_object_id}, {"$pull": {"metadata.jobs": str(job_id)}})
                    # check if file is safe to delete
                    if self._getFsFiles().count_documents({"_id": file_object_id, "metadata.jobs": [], "metadata.tmp_lock": 0}) > 0:
                        self._getFsFiles().delete_one({"_id": file_object_id})
            if with_result:
                # delete result from GridFS
                self._getFsFiles().delete_one({"_id": ObjectId(deletable_job["result"])})
        job_deletion_result = self._getCollection().delete_one({"_id": job_id})
        return job_deletion_result.deleted_count

    def delete_jobs(self, method=None, created_before=None, finished_before=None, with_results=True):
        filter_count = len([1 for item in [method, created_before, finished_before] if item is not None])
        combined_filter = {"$and": []} if filter_count > 1 else {}
        method_filter = {}
        created_filter = {}
        finished_filter = {}
        if method is not None:
            method_filter = {"payload.method": method}
            if filter_count > 1:
                combined_filter["$and"].append(method_filter)
            else:
                combined_filter = method_filter
        if created_before is not None:
            created_filter = {"created_at": {"$lt": created_before}}
            if filter_count > 1:
                combined_filter["$and"].append(created_filter)
            else:
                combined_filter = created_filter
        elif finished_before is not None:
            finished_filter = {"finished_at": {"$lt": finished_before}}
            if filter_count > 1:
                combined_filter["$and"].append(finished_filter)
            else:
                combined_filter = finished_filter
        # run find() first to determine how many jobs of which method will be deleted and what their results are
        jobs_to_be_deleted = [j for j in self._getCollection().find(combined_filter)]
        # delete results
        counter_updates = []
        for deletable_job in jobs_to_be_deleted:
            counter_updates.append((deletable_job["payload"]["method"], self._identifyJobState(deletable_job), -1))
            if with_results and deletable_job["result"]:
                # delete result from GridFS
                self._getFs().delete(ObjectId(deletable_job["result"]))
        job_deletion_result = self._getCollection().delete_many(combined_filter)
        if len(jobs_to_be_deleted) != job_deletion_result.deleted_count:
            raise Exception("Number of deleted jobs was unequal to number of jobs to delete!")
        return job_deletion_result.deleted_count

    def _file_to_grid(self, binary, metadata=None):
        object_id = self._getFs().put(binary, metadata=metadata)
        return str(object_id)

    def _grid_to_file(self, grid, results_only=True):
        oid = ObjectId(grid)
        if not self._getFs().exists(oid):
            return None
        entry = self._getFs().get(oid)
        if results_only:
            metadata = entry.metadata
            if "result" not in metadata or not metadata["result"]:
                return b'"Access Not Allowed"'
        result = entry.read()
        return result

    def _dicts_to_grid(self, dicts, **kwargs):
        return self._file_to_grid(json.dumps(dicts).encode("ascii"), **kwargs)

    def _grid_to_dicts(self, grid, **kwargs):
        result = None
        grid_file = self._grid_to_file(grid, **kwargs)
        if grid_file is not None:
            result = json.loads(grid_file)
        return result

    def _delete_grid(self, grid):
        oid = ObjectId(grid)
        self._getFs().delete(oid)

    def _grid_to_meta(self, grid):
        oid = ObjectId(grid)
        entry = self._getFs().get(oid)
        return entry.metadata

    def get_cached_job_id(self, payload):
        """The job whose result a repeated request with the same descriptor should reuse.

        Two things decide that. What is eligible: a job is only worth handing out again when
        it is finished, waiting, or actually in flight on a live worker - not when a dead
        worker still holds its lock (#150), and never when it failed (no attempts left) or
        was terminated. Which of the eligible ones wins: a finished job is preferred over one
        still queued or running, and among several of the same kind the newest. Sorting by
        finished_at descending puts the finished jobs first, because a descending sort places
        null (unfinished) after every date (fkie-cad/mcritweb#47).
        """
        job = self._wrap_one(
            self._getCollection().find_one(
                {
                    "attempts_left": {"$gt": 0},
                    "payload.descriptor": payload["descriptor"],
                    "terminated": False,
                    # a result that depended on state outside its arguments (UncacheableResult)
                    "cacheable": {"$ne": False},
                    "$or": [
                        {"finished_at": {"$ne": None}},
                        {"locked_by": None},
                        {"locked_by": {"$in": sorted(self._live_worker_ids())}},
                    ],
                },
                sort=[("finished_at", pymongo.DESCENDING), ("created_at", pymongo.DESCENDING)],
            )
        )
        return job and job.job_id or None

    def getFileByHash(self, sha256, max_bytes=-1):
        file = self._getFs().find_one({"metadata.sha256": sha256})
        if file:
            return file.read(max_bytes)

    def get_file_by_hash_inc_lock(self, sha256):
        file = self._getFsFiles().find_one_and_update({"metadata.sha256": sha256}, {"$inc": {"metadata.tmp_lock": 1}})
        return file and str(file["_id"]) or None

    def add_job_id_to_file(self, job_id, file_id):
        file_id = ObjectId(file_id)
        self._getFsFiles().find_one_and_update({"_id": file_id}, {"$inc": {"metadata.tmp_lock": -1}, "$addToSet": {"metadata.jobs": job_id}})

    def clean(self):
        # TODO consider a good way to implement this
        # we probably do not want to drop matches and their results automatically, which would be the case right now
        time_threshold = datetime.now() - timedelta(seconds=self.cache_time)
        job_query = {"finished_at": {"$lt": time_threshold}}
        to_delete = self._getCollection().find(job_query)
        to_delete = list(to_delete)
        results = [data["result"] for data in to_delete]
        file_params = {data["_id"]: list(json.loads(data["payload"]["file_params"]).values()) for data in to_delete}

        # delete results
        for r in results:
            self._delete_grid(r)

        # remove job from params
        for job, params in file_params.items():
            self._getFsFiles().update_many({"_id": {"$in": [ObjectId(p) for p in params]}}, {"$pull": {"metadata.jobs": str(job)}})

        # delete params
        all_params = [ObjectId(j) for i in file_params.values() for j in i]
        # for all of the params that can be deleted (with no jobs, and no lock), remove the sha256
        # In this way the file cannot be utilized as a cached file again
        self._getFsFiles().update_many(
            {"_id": {"$in": all_params}, "metadata.jobs": [], "metadata.tmp_lock": 0},
            {"$set": {"metadata.sha256": None}},
        )
        # now get the list of files to be deleted
        params_to_delete = self._getFsFiles().find({"_id": {"$in": all_params}, "metadata.jobs": [], "metadata.tmp_lock": 0, "metadata.sha256": None})
        # delete them
        for p in params_to_delete:
            self._getFs().delete(ObjectId(p["_id"]))

        # delete jobs
        self._getCollection().delete_many(job_query)

    def release_all_jobs(self, consumer_id=None):
        # release all jobs associated with our consumer id if they are started, locked, but not finished.
        self._getCollection().update_many(
            filter={"locked_by": consumer_id if consumer_id else self.consumer_id, "started_at": {"$ne": None}, "finished_at": {"$eq": None}},
            update={"$set": {"locked_by": None, "locked_at": None}, "$inc": {"attempts_left": -1}},
        )

    def release_orphaned_jobs(self):
        """Return the jobs held by workers that are not alive to the queue.

        A worker is alive while it is registered with a fresh heartbeat; one that was
        never registered, unregistered, or stopped heartbeating (killed with SIGKILL,
        OOM) is not, and its unfinished jobs are released with one attempt fewer, like
        a worker's own error path does. Dead registrations are dropped along the way, so
        the worker list stops accumulating ids of processes that are long gone (#150).
        """
        now = datetime.now()
        live_worker_ids = self._live_worker_ids(now)
        registration = self._getQueueCounters().find_one({"name": "workers"}, {"workers": 1, "_id": 0}) or {}
        dead_registered = [worker_id for worker_id in registration.get("workers") or [] if worker_id not in live_worker_ids]
        if dead_registered:
            self._getQueueCounters().update_one(
                {"name": "workers"},
                {"$pull": {"workers": {"$in": dead_registered}}, "$unset": {f"heartbeats.{worker_id}": "" for worker_id in dead_registered}},
            )
        holders = set(wid for wid in self._getCollection().distinct("locked_by") if wid)
        orphan_ids = sorted(holders.difference(live_worker_ids))
        if not orphan_ids:
            return
        stranded = {"locked_by": {"$in": orphan_ids}, "started_at": {"$ne": None}, "finished_at": None}
        counter_updates = []
        released = 0
        for job_id in [job["_id"] for job in self._getCollection().find(stranded, {"_id": 1})]:
            # per job, like Job.error(): the attempt that died counts, and a job out of
            # attempts fails and releases whatever waits on it instead of being requeued
            job = self._getCollection().find_one_and_update(
                {"_id": job_id, **stranded},
                {"$set": {"locked_by": None, "locked_at": None}, "$inc": {"attempts_left": -1}},
                return_document=ReturnDocument.AFTER,
            )
            if job is None:
                continue
            released += 1
            method = job["payload"]["method"]
            counter_updates.append((method, "in_progress", -1))
            if job["attempts_left"] <= 0:
                self._notify_dependent_jobs(str(job["_id"]))
                counter_updates.append((method, "failed", 1))
            else:
                counter_updates.append((method, "queued", 1))
        if released:
            LOGGER.warning("Released %d job(s) held by dead worker(s) %s back to the queue.", released, ", ".join(orphan_ids))
            self.updateQueueCounters(counter_updates)

    def terminate_all_jobs(self):
        pass


class Job:
    def __init__(self, queue, data):
        """ """
        self._queue = queue
        self._data = data

    def __str__(self) -> str:
        return f"ID: {self.job_id}, created: {self.created_at}, finished: {self.finished_at}, result: {self.result}"

    @property
    def method(self):
        return self._data["payload"]["method"]

    @property
    def payload(self):
        return self._data["payload"]

    @property
    def job_id(self):
        return self._data["_id"]

    @property
    def number(self):
        return self._data["number"] if "number" in self._data else -1

    @property
    def priority(self):
        return self._data["priority"]

    @property
    def username(self):
        # absent on jobs created before the field existed
        return self._data.get("username")

    @property
    def attempts_left(self):
        return self._data["attempts_left"]

    @property
    def is_failed(self):
        return self._data["attempts_left"] == 0

    @property
    def locked_by(self):
        return self._data["locked_by"]

    @property
    def locked_at(self):
        return self._data["locked_at"]

    @property
    def created_at(self):
        return self._data["created_at"]

    @property
    def started_at(self):
        return self._data["started_at"]

    @property
    def last_error(self):
        return self._data["last_error"]

    @property
    def finished_at(self):
        return self._data["finished_at"]

    @property
    def is_finished(self):
        return self._data["finished_at"] is not None

    @property
    def parameters(self):
        method_str = ""
        if "payload" in self._data and "params" in self._data["payload"] and "method" in self._data["payload"]:
            payload_params = json.loads(self._data["payload"]["params"])
            method_str = self._data["payload"]["method"]
            indexed_key_values = []
            named_key_values = []
            for k, v in payload_params.items():
                try:
                    int(k)
                    indexed_key_values.append(v)
                except (TypeError, ValueError):
                    named_key_values.append(v)
            combined_values = indexed_key_values + named_key_values
            method_str += "(" + ", ".join([str(v) for v in combined_values]) + ")"
        return method_str

    @property
    def progress(self):
        return self._data["progress"]

    # This is a GridFS id, not the actual result
    @property
    def result(self):
        return self._data["result"]

    @result.setter
    def result(self, res):
        self._data["result"] = res

    ## job control

    def complete(self, result=None):
        """job has been completed."""
        if result:
            self._data["result"] = result
        job = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id}, update={"$set": {"finished_at": datetime.now(), "progress": 1}}, return_document=ReturnDocument.AFTER
        )
        # job result was not set by another completion before, or if it is forced by argument
        if (job and job["result"] is None) or result:
            job = self._queue.collection.find_one_and_update(filter={"_id": self.job_id}, update={"$set": {"result": self._data["result"]}}, return_document=ReturnDocument.AFTER)
        if not job:
            return
        self._queue.updateQueueCounters([(self.method, "in_progress", -1), (self.method, "finished", 1)])
        self._queue._notify_dependent_jobs(str(self.job_id))
        return job

    def mark_uncacheable(self):
        """keep this job from being served to later requests with the same descriptor."""
        self._data["cacheable"] = False
        self._queue.collection.update_one({"_id": self.job_id}, {"$set": {"cacheable": False}})

    def error(self, message=None):
        """note an error processing a job, and return it to the queue."""
        job = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id},
            update={"$set": {"locked_by": None, "locked_at": None, "last_error": message}, "$inc": {"attempts_left": -1}},
            return_document=ReturnDocument.AFTER,
        )
        if not job:
            return
        updates = [(self.method, "in_progress", -1), (self.method, "queued", 1)]
        if job["attempts_left"] <= 0:
            self._queue._notify_dependent_jobs(str(job["_id"]))
            updates.append((self.method, "queued", -1))
            updates.append((self.method, "failed", 1))
        self._queue.updateQueueCounters(updates)

    def progressor(self, count=0):
        """note progress on a long running task."""
        # refresh the lock heartbeat only while the job is actually held: an unconditional
        # write can race a concurrent release and resurrect locked_at on a job whose
        # locked_by was just cleared, leaving a half-locked document (observed while
        # reproducing #106 - the job then starves, invisible to polling forever)
        updated_job = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id, "locked_by": {"$ne": None}},
            update={"$set": {"progress": count, "locked_at": datetime.now()}},
            return_document=ReturnDocument.AFTER,
        )
        if updated_job is None:
            # the job was released while we were working on it; still record the progress
            updated_job = self._queue.collection.find_one_and_update(filter={"_id": self.job_id}, update={"$set": {"progress": count}}, return_document=ReturnDocument.AFTER)
        return updated_job

    def release(self):
        """put the job back into_queue."""
        job = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id}, update={"$set": {"locked_by": None, "locked_at": None}, "$inc": {"attempts_left": -1}}, return_document=ReturnDocument.AFTER
        )
        if not job:
            return
        updates = [(self.method, "in_progress", -1), (self.method, "queued", 1)]
        if job["attempts_left"] <= 0:
            self._queue._notify_dependent_jobs(str(job["_id"]))
            updates.append((self.method, "queued", -1))
            updates.append((self.method, "failed", 1))
        self._queue.updateQueueCounters(updates)
        return job

    ## context manager support

    def __enter__(self):
        return self._data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)

    # only works for jobs that report progress
    def _is_terminated(self, use_cached=False):
        if use_cached:
            data = self._data
        else:
            data = self._queue.collection.find_one({"_id": self.job_id})

        if data is None:
            return True

        return ("terminated" in data) and data["terminated"] or False

    # only works for jobs that report progress
    def terminate(self):
        job = self._queue.collection.find_one_and_update(
            filter={"_id": self.job_id}, update={"$set": {"terminated": True, "locked_at": datetime.now()}}, return_document=ReturnDocument.AFTER
        )
        if not job:
            return
        self._queue.updateQueueCounters([(job["payload"]["method"], "in_progress", -1), (job["payload"]["method"], "terminated", 1)])
        return job
