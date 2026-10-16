from dataclasses import dataclass
from typing import Optional

from mcrit.config.ConfigInterface import ConfigInterface
from mcrit.queue.QueueFactory import QueueFactory


@dataclass
class QueueConfig(ConfigInterface):
    # QUEUE_METHOD:... = QueueFactory.QUEUE_METHOD_FAKE
    QUEUE_METHOD: str = QueueFactory.QUEUE_METHOD_MONGODB
    QUEUE_SERVER: str = "127.0.0.1"
    QUEUE_PORT: str = "27017"
    # By default, MongoDbStorage's DB's name and MongoQueue's DB's name are both "mcrit"
    # Changing one DB name here or at runtime DOES NOT change the other name!
    QUEUE_MONGODB_DBNAME: str = "mcrit"
    QUEUE_MONGODB_USERNAME: Optional[str] = None
    QUEUE_MONGODB_PASSWORD: Optional[str] = None
    QUEUE_MONGODB_FLAGS: str = ""
    QUEUE_MONGODB_COLLECTION_NAME: str = "queue"
    QUEUE_TIMEOUT: int = 300
    QUEUE_MAX_ATTEMPTS: int = 3
    # QUEUE_CLEAN_INTERVAL is the time EACH WORKER waits between cleaning
    QUEUE_CLEAN_INTERVAL: int = 20 * 60  # Clean every 20 minutes
    # timeout in seconds for child processes spawned by SpawningWorker
    QUEUE_SPAWNINGWORKER_CHILDREN_TIMEOUT: int = 60 * 60
    # timeout in seconds for a job run by a plain `mcrit worker`, 0 for none: its heartbeat keeps a job
    # claimed however long it runs, so nothing else frees the worker from a job that does not end. A
    # job over it fails and is retried like any failing job. Applies to the jobs that read the index and
    # whose cost a request controls (matching, unique blocks), not to jobs that write it (binary
    # submission, minhash updates: an interruption could split minhashes from their band postings) or
    # to corpus maintenance (rebuild*, recalculate*, repair*, deletions, cleanup). Needs SIGALRM (POSIX).
    QUEUE_WORKER_JOB_TIMEOUT: int = 60 * 60
    # Memory, in bytes, a job run by a SpawningWorker may hold before it is stopped; 0 is no limit.
    # Measured as the resident size of the job's process and everything it started, such as its
    # hashing pool, once a second. A job over it is killed and fails like any failing job instead
    # of taking the host's memory from the other workers (#69). Needs /proc (Linux); ignored with
    # a warning elsewhere.
    QUEUE_SPAWNINGWORKER_CHILD_MAX_MEMORY: int = 0
