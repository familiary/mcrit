#!/usr/bin/env python3

import logging
import os
import uuid
from typing import TYPE_CHECKING, Optional

from pymongo import ReturnDocument

from mcrit.config.McritConfig import McritConfig
from mcrit.minhash.MinHasher import MinHasher
from mcrit.queue.QueueFactory import QueueFactory
from mcrit.queue.QueueRemoteCalls import JobProgressReporter
from mcrit.storage.StorageFactory import StorageFactory
from mcrit.Worker import Worker

if TYPE_CHECKING:
    from mcrit.storage.StorageInterface import StorageInterface

logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)


class SingleJobWorker(Worker):
    def __init__(self, job_id, queue=None, config=None, storage: Optional["StorageInterface"] = None, profiling=False):
        self.job_id = job_id
        self._worker_id = f"Worker-{uuid.uuid4()}"
        LOGGER.info(f"Starting as worker: {self._worker_id}")
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
        self.queue.unregisterWorker()
        self.queue.release_all_jobs()

    #### Overwrite inherited methods to achive execution of a single job ####

    def _executeJobPayload(self, job_payload, job):
        LOGGER.debug("DECODE JOB: %s", job_payload)
        method, params, kwparams = self._decodeJobPayload(job_payload)
        # Add progress reporter if necessary:
        if method.progressor:
            LOGGER.debug("kwparams: %s", kwparams)
            kwparams["progress_reporter"] = JobProgressReporter(job, 0.1)
        LOGGER.debug("EXECUTE JOB: %s", job_payload)
        result = method(*params, **kwparams)
        LOGGER.debug("FINISHED JOB: %s", job_payload)
        return result

    def _executeJob(self, job):
        try:
            with job as j:
                LOGGER.info("Processing Remote Job: %s", job)
                result = self._executeJobPayload(j["payload"], job)
                # LOGGER.debug("Remote Job Result: %s", result)
                # ensure we always have a job_id for finished job payloads
                result_id = self._storeJobResult(job, result)
                # update result directly from single job to ensure we don't loose it
                LOGGER.info("Updating job %s with result %s", job.job_id, result_id)
                updated_job = self.queue.collection.find_one_and_update(
                    filter={"_id": job.job_id}, update={"$set": {"result": result_id, "progress": 1}}, return_document=ReturnDocument.AFTER
                )
                # LOGGER.info(updated_job)
                if updated_job is None:
                    raise RuntimeError(f"Failed to update job {job.job_id} with result {result_id} in database.")
                job.result = result_id
                LOGGER.info("Finished Remote Job producing result_id: %s", result_id)
                print(result_id)
        except Exception as exc:
            LOGGER.error("Job %s failed with exception: %s", job.job_id, exc, exc_info=True)

    def run(self):
        self._alive = True
        job = self.queue.get_job(self.job_id)
        LOGGER.debug("Found job")
        self._executeJob(job)

    def terminate(self):
        self._alive = False
