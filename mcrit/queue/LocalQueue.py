import json
import logging
import traceback
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

# Only do basicConfig if no handlers have been configured
if not logging.root.handlers:
    logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
LOGGER = logging.getLogger(__name__)


# copied from mongoqueue
class Job:
    """
    This class is also used to represent jobs to the outside
    """

    def __init__(self, data, queue):
        self._data = data
        self._queue = queue
        # All currently known remote jobs as implemented in the Worker, redundantly organized by various aspects
        self.method_types = {
            "matching": [
                "getMatchesForSample",
                "getMatchesForSampleVs",
                "combineMatchesToCross",
            ],
            "query": [
                "getMatchesForUnmappedBinary",
                "getMatchesForMappedBinary",
                "getMatchesForSmdaReport",
            ],
            "blocks": [
                "getUniqueBlocks",
            ],
            "minhashing": [
                "updateMinHashesForSample",
                "updateMinHashes",
                "rebuildIndex",
            ],
            "collection": [
                "addBinarySample",
                "deleteSample",
                "modifySample",
                "deleteFamily",
                "modifyFamily",
            ],
            "sample_id": [
                "getMatchesForSampleVs",
                "getMatchesForSample",
                "updateMinHashesForSample",
                "deleteSample",
                "modifySample",
            ],
            "family_id": [
                "deleteFamily",
                "modifyFamily",
            ],
            "all": [
                "getMatchesForSample",
                "getMatchesForSampleVs",
                "combineMatchesToCross",
                "getMatchesForUnmappedBinary",
                "getMatchesForMappedBinary",
                "getMatchesForSmdaReport",
                "getUniqueBlocks",
                "updateMinHashesForSample",
                "updateMinHashes",
                "addBinarySample",
                "modifySample",
                "deleteSample",
                "modifyFamily",
                "deleteFamily",
                "rebuildIndex",
            ],
        }

    def __str__(self) -> str:
        return f"ID: {self.job_id} - {self.parameters} | created: {self.created_at}, finished: {self.finished_at}, result: {self.result}, progress: {self.progress}"

    @property
    def family(self) -> Optional[str]:
        if self.method in ["addBinarySample"]:
            descriptor = json.loads(self._data["payload"]["descriptor"])
            return descriptor[1]["2"]

    @property
    def sha256(self) -> Optional[str]:
        if self.method in ["getMatchesForUnmappedBinary", "getMatchesForMappedBinary", "getMatchesForSmdaReport", "addBinarySample"]:
            descriptor = json.loads(self._data["payload"]["descriptor"])
            return descriptor[2]["0"]

    @property
    def filename(self) -> Optional[str]:
        if self.method in ["addBinarySample"]:
            descriptor = json.loads(self._data["payload"]["descriptor"])
            return descriptor[1]["1"]

    @property
    def family_id(self):
        if self.method in self.method_types["family_id"]:
            return int(self.arguments[0])
        if self.method == "getUniqueBlocks":
            if len(self.arguments) > 1:
                return int(self.arguments[1])

    @property
    def has_family_id(self):
        if self.method in self.method_types["family_id"] or self.method == "getUniqueBlocks":
            return True
        return False

    @property
    def sample_ids(self):
        sample_ids = []
        if self.method == "combineMatchesToCross":
            sample_ids = [int(key) for key, value in self.arguments[0].items()]
        elif self.method == "getUniqueBlocks":
            sample_ids = [int(sid) for sid in self.arguments[0]]
        elif self.method in self.method_types["sample_id"]:
            sample_ids.append(int(self.arguments[0]))
            if self.method == "getMatchesForSampleVs":
                sample_ids.append(int(self.arguments[1]))
        elif self.sample_id is not None:
            sample_ids.append(self.sample_id)
        return set(list(sample_ids))

    @property
    def sample_id(self):
        if self.method in self.method_types["sample_id"]:
            return int(self.arguments[0])
        elif self.method == "updateMinHashesForSample":
            return int(self.arguments[0])
        elif self.method == "getUniqueBlocks":
            if len(self.arguments[0]) > 0:
                return int(self.arguments[0][0])
        elif self.method == "deleteSample":
            return int(self.arguments[0])
        elif self.method == "modifySample":
            return int(self.arguments[0])

    @property
    def other_sample_id(self):
        if self.method == "getMatchesForSampleVs":
            return int(self.arguments[1])

    def has_sample_id(self, sample_id: int):
        if self.method in self.method_types["sample_id"]:
            if self.method == "getMatchesForSampleVs":
                return int(self.arguments[0]) == sample_id or int(self.arguments[1]) == sample_id
            else:
                return int(self.arguments[0]) == sample_id

    @property
    def is_matching_job(self):
        if self.method in self.method_types["matching"]:
            return True
        return False

    @property
    def is_minhashing_job(self):
        if self.method in self.method_types["minhashing"]:
            return True
        return False

    @property
    def is_query_job(self):
        if self.method in self.method_types["query"]:
            return True
        return False

    @property
    def is_block_job(self):
        if self.method in self.method_types["blocks"]:
            return True
        return False

    @property
    def is_collection_job(self):
        if self.method in self.method_types["collection"]:
            return True
        return False

    @property
    def method(self):
        if "payload" in self._data and "method" in self._data["payload"]:
            return self._data["payload"]["method"]

    @property
    def payload(self):
        return self._data["payload"]

    @property
    def arguments(self):
        combined_values = []
        if "payload" in self._data and "params" in self._data["payload"] and "method" in self._data["payload"]:
            payload_params = json.loads(self._data["payload"]["params"])
            indexed_key_values = []
            named_key_values = []
            for k, v in payload_params.items():
                try:
                    int(k)
                    indexed_key_values.append(v)
                except (TypeError, ValueError):
                    named_key_values.append(v)
            combined_values = indexed_key_values + named_key_values
        return combined_values

    @property
    def parameters(self):
        method_str = ""
        if "payload" in self._data and "params" in self._data["payload"] and "method" in self._data["payload"]:
            method_str = self._data["payload"]["method"]
            method_str += "(" + ", ".join([str(v) for v in self.arguments]) + ")"
        return method_str

    @property
    def short_parameters(self):
        method_str = ""
        if "payload" in self._data and "params" in self._data["payload"] and "method" in self._data["payload"]:
            method_str = self._data["payload"]["method"]
            method_str += "(" + ", ".join([str(v) for v in self.arguments]) + ")"
            if len(method_str) > 50:
                method_str = method_str[:107] + "..." + ")"
        return method_str

    @property
    def all_dependencies(self):
        return self._data["all_dependencies"]

    @property
    def job_id(self):
        if isinstance(self._data["_id"], dict):
            return str(self._data["_id"]["$oid"])
        return self._data["_id"]

    @property
    def number(self):
        return self._data["number"] if "number" in self._data else -1

    @property
    def priority(self):
        return self._data["priority"]

    @property
    def username(self):
        # who asked for the job (fkie-cad/mcritweb#37); None on jobs from a backend that did not record it
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
        if isinstance(self._data["locked_at"], dict):
            return str(self._data["locked_at"]["$date"])
        return self._data["locked_at"]

    @property
    def last_error(self):
        return self._data["last_error"]

    @property
    def finished_at(self):
        if isinstance(self._data["finished_at"], dict):
            return str(self._data["finished_at"]["$date"])
        return self._data["finished_at"]

    @property
    def is_finished(self):
        return self._data["finished_at"] is not None

    @property
    def duration(self):
        if self.is_finished:
            FMT = "%Y-%m-%d-%H:%M:%S"
            finished_at = self.finished_at[:10] + "-" + self.finished_at[11:19]
            started_at = self.started_at[:10] + "-" + self.started_at[11:19]
            duration = datetime.strptime(finished_at, FMT) - datetime.strptime(started_at, FMT)
            return duration
        return None

    @property
    def created_at(self):
        if isinstance(self._data["created_at"], dict):
            return str(self._data["created_at"]["$date"])
        return self._data["created_at"]

    @property
    def started_at(self):
        if isinstance(self._data["started_at"], dict):
            return str(self._data["started_at"]["$date"])
        return self._data["started_at"]

    @property
    def is_terminated(self):
        return self._is_terminated()

    @property
    def progress(self):
        return self._data["progress"] if "progress" in self._data else 0

    # NOTE: This is a GridFS id, not the actual result
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
        self._data["finished_at"] = datetime.now()
        self._data["progress"] = 1
        return self

    def error(self, message=None):
        """note an error processing a job, and return it to the queue."""
        self._data["locked_by"] = None
        self._data["locked_at"] = None
        self._data["last_error"] = message
        self._data["attempts_left"] -= 1

    def progressor(self, count=0):
        self._data["progress"] = count

    def __enter__(self):
        return self._data

    def __exit__(self, type, value, tb):
        if (type, value, tb) == (None, None, None):
            self.complete()
        else:
            error = traceback.format_exc()
            self.error(error)

    # only works for jobs that report progress
    # used_cached flag is useless for this kind of queue
    def _is_terminated(self, use_cached=False):
        terminated = self._data["terminated"]
        if terminated is None:
            return False
        return terminated

    # only works for jobs that report progress
    def terminate(self):
        self._data["terminated"] = True


class LocalQueue:
    def __init__(self):
        self._setup_empty_queue()
        self._worker: Any = None
        self._job_counter = 0
        self.clean_interval: float = 10**9
        self.cache_time: float = 10**9
        self.max_attempts = 1

    def _setup_empty_queue(self):
        self._jobs: Dict[str, Any] = defaultdict(lambda: None)
        self._files: Dict[str, Any] = defaultdict(lambda: None)
        self._files_meta: Dict[str, Any] = defaultdict(lambda: None)
        self._hash_to_file: Dict[str, Any] = defaultdict(lambda: None)

    def registerWorker(self):
        # left empty, as we only have one worker in local mode
        pass

    def unregisterWorker(self):
        # left empty, as we only have one worker in local mode
        pass

    def getQueueStatistics(self):
        # we don't track stats for the volatile local mode
        return {}

    def set_worker(self, worker):
        self._worker = worker

    def get_job(self, job_id):
        # .get(): _jobs is a defaultdict, and indexing it with an unknown id used to leave a
        # None entry behind that every later scan of the jobs tripped over
        data = self._jobs.get(job_id)
        return data and Job(data, self)

    @staticmethod
    def _identifyJobState(doc) -> str:
        # the same rule as MongoQueue._identifyJobState, on a job dict whose absent fields read
        # as None
        if doc["started_at"] and doc["locked_by"] and not (doc["finished_at"] or doc["terminated"]):
            return "in_progress"
        if doc["attempts_left"] == 0 and not doc["finished_at"] and not doc["terminated"]:
            return "failed"
        if not doc["finished_at"] and not doc["locked_by"] and not doc["terminated"]:
            return "queued"
        if doc["finished_at"] and not doc["terminated"]:
            return "finished"
        if doc["terminated"]:
            return "terminated"
        return "unknown"

    @staticmethod
    def _has_matching_first_argument(job_document, sample_ids) -> bool:
        # the same selection MongoQueue makes on payload.descriptor (first positional
        # argument via rearrange_params' "0" key), applied to payload.params instead
        first_argument = json.loads(job_document["payload"]["params"]).get("0")
        try:
            return int(first_argument) in sample_ids
        except (TypeError, ValueError):
            return False

    def _matching_jobs(self, method=None, state=None, filter=None, username=None, ascending=False, sample_ids=None, job_ids=None):
        # the same selection MongoQueue._job_query makes (fkie-cad/mcritweb#57), in submission order
        if sample_ids is not None and method is None:
            return []
        selected_sample_ids = set(sample_ids) if sample_ids is not None else None
        selected_job_ids = set(job_ids) if job_ids is not None else None
        jobs = []
        for job_id, job_document in self._jobs.items():
            if method is not None and job_document["payload"]["method"] != method:
                continue
            if selected_sample_ids is not None and not self._has_matching_first_argument(job_document, selected_sample_ids):
                continue
            if selected_job_ids is not None and job_id not in selected_job_ids:
                continue
            if state is not None and self._identifyJobState(job_document) != state:
                continue
            if username is not None and job_document["username"] != username:
                continue
            if filter:
                haystack = (job_document["payload"].get("method") or "") + " " + (job_document["payload"].get("params") or "")
                if filter.lower() not in haystack.lower():
                    continue
            jobs.append(job_document)
        jobs.sort(key=lambda job_document: job_document["number"], reverse=not ascending)
        return jobs

    def get_jobs(self, start_index: int, limit: int, method=None, state=None, ascending=False, filter=None, username=None, sample_ids=None, job_ids=None):
        matching = self._matching_jobs(method=method, state=state, filter=filter, username=username, ascending=ascending, sample_ids=sample_ids, job_ids=job_ids)
        jobs = [Job(job_document, self) for job_document in matching]
        if limit:
            return jobs[start_index : start_index + limit]
        return jobs[start_index:]

    def get_job_count(self, method=None, state=None, filter=None, username=None, sample_ids=None, job_ids=None) -> int:
        return len(self._matching_jobs(method=method, state=state, filter=filter, username=username, sample_ids=sample_ids, job_ids=job_ids))

    def get_cached_job_id(self, payload):
        # same rule as MongoQueue.get_cached_job_id (fkie-cad/mcritweb#47): finished before unfinished, newest
        # first within each group, failed and terminated jobs never
        candidates = [
            job_data
            for job_data in self._jobs.values()
            if job_data["payload"]["descriptor"] == payload["descriptor"] and job_data["attempts_left"] > 0 and not job_data["terminated"]
        ]
        if not candidates:
            return None
        best = max(candidates, key=lambda job_data: (job_data["finished_at"] is not None, job_data["number"]))
        return best["_id"]

    def _file_to_grid(self, file, metadata=None):
        id = str(uuid.uuid4())
        try:
            file.seek(0)
            data = file.read()
        except AttributeError:
            if not isinstance(file, (str, bytes)):
                raise TypeError("Only strings, bytes or file-like objecs are supported")
            if isinstance(file, str):
                data = file.encode("utf-8")
            else:
                data = file
        self._files[id] = data
        self._files_meta[id] = metadata
        if metadata is not None:
            try:
                self._hash_to_file[metadata["sha256"]] = id
            except KeyError:
                pass
        return id

    def _grid_to_file(self, grid, results_only=True):
        file = self._files[grid]
        if file is None:
            return None
        if results_only:
            metadata = self._files_meta[grid]
            if "result" not in metadata or not metadata["result"]:
                return b'"Access Not Allowed"'
        return file

    def get_file_by_hash_inc_lock(self, sha256):
        id = self._hash_to_file[sha256]
        self._inc_lock(id)
        return id

    def _get_file_by_hash(self, sha256):
        return self._hash_to_file[sha256]

    def _inc_lock(self, id):
        if id is None:
            return
        try:
            self._files_meta[id]["tmp_lock"] += 1
        except KeyError:
            pass

    def _dec_lock(self, id):
        if id is None:
            return
        try:
            self._files_meta[id]["tmp_lock"] -= 1
        except KeyError:
            pass

    def add_job_id_to_file(self, job_id, file_id):
        self._files_meta[file_id]["jobs"].append(job_id)
        self._dec_lock(file_id)

    def _dicts_to_grid(self, dicts, **kwargs):
        return self._file_to_grid(json.dumps(dicts).encode("ascii"), **kwargs)

    def _grid_to_dicts(self, grid, **kwargs):
        return json.loads(self._grid_to_file(grid, **kwargs).decode("ascii"))

    def _grid_to_meta(self, grid):
        return self._files_meta[grid]

    def _delete_grid(self, grid):
        meta = self._files_meta[grid]
        try:
            if self._hash_to_file[meta["sha256"]] == grid:
                del self._hash_to_file[meta["sha256"]]
        except KeyError:
            pass
        del self._files[grid]
        del self._files_meta[grid]

    def put(self, payload, await_jobs=[], username=None):
        id = str(uuid.uuid4())
        job_data: Dict[str, Any] = defaultdict(lambda: None)
        job_data["_id"] = id
        job_data["number"] = self._job_counter
        self._job_counter += 1
        job_data["payload"] = payload
        job_data["username"] = username
        job_data["unfinished_dependencies"] = await_jobs
        job_data["all_dependencies"] = await_jobs
        job_data["attempts_left"] = self.max_attempts
        job_data["created_at"] = datetime.now()
        self._jobs[id] = job_data
        job_data["started_at"] = datetime.now()
        # NOTE: we can just ignore await jobs, because all jobs are
        #       executed in submission order
        self._worker._executeJob(Job(job_data, self))
        return id

    def clear(self):
        self.terminate_all_jobs()
        self._setup_empty_queue()

    def delete_job(self, id):
        if id not in self._jobs:
            return 0
        job = self._jobs[id]
        result = job["result"]
        file_params = json.loads(job["payload"]["file_params"])
        del self._jobs[id]
        if result is not None:
            self._delete_grid(result)
        for f in file_params.values():
            meta = self._grid_to_meta(f)
            LOGGER.debug("Job meta: %s", meta)
            meta["jobs"].remove(id)
            # as MongoQueue does: a file no job uses and nobody is about to claim goes with its last job
            if not meta["jobs"] and meta.get("tmp_lock", 0) == 0:
                self._delete_grid(f)
        return 1

    def delete_orphaned_files(self, dry_run=False):
        """As MongoQueue.delete_orphaned_files; there are no chunks to leave behind in memory."""
        results = [grid for grid, meta in self._files_meta.items() if meta and meta.get("result") and str(meta.get("job")) not in self._jobs]
        file_params = [
            grid
            for grid, meta in self._files_meta.items()
            if meta
            and not meta.get("result")
            and meta.get("tmp_lock") == 0
            and isinstance(meta.get("jobs"), list)
            and not any(str(job_id) in self._jobs for job_id in meta["jobs"])
        ]
        if not dry_run:
            for grid in results + file_params:
                self._delete_grid(grid)
        return {"dry_run": dry_run, "results": len(results), "file_params": len(file_params), "chunk_files": 0}

    delete_history = []

    def delete_jobs(self, method=None, created_before=None, finished_before=None):
        delete_list = []
        deleted_count = 0
        for d in self._jobs.values():
            if d["payload"]["method"] is not None and d["payload"]["method"] != method:
                continue
            if d["created_at"] is not None and d["created_at"] >= finished_before:
                continue
            if d["finished_at"] is not None and d["finished_at"] >= finished_before:
                continue
            delete_list.append(d["_id"])
        for id in delete_list:
            deleted_count += self.delete_job(id)
        return deleted_count

    def clean(self):
        time_threshold = datetime.now() - timedelta(seconds=self.cache_time)
        delete_list = []
        for d in self._jobs.values():
            if (d["finished_at"] is not None) and d["finished_at"] < time_threshold:
                delete_list.append(d["_id"])

        self.delete_history.append(delete_list)
        LOGGER.debug("Perfomed clean, delete_history now has %d entries.", len(self.delete_history))

        for id in delete_list:
            self.delete_job(id)

        delete_list = []
        for id, meta in self._files_meta.items():
            if "result" in meta and meta["result"]:
                continue
            if len(meta["jobs"]) == 0 and meta["tmp_lock"] == 0:
                delete_list.append(id)
        for id in delete_list:
            self._delete_grid(id)

    def terminate_all_jobs(self):
        for job in self._jobs.values():
            if job["result"] is None:
                job["terminated"] = True

    def next(self):
        # this is a dummy, as we don't have an actual queue
        return None
