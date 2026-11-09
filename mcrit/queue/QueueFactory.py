from mcrit.queue.retention import TRANSIENT_JOB_METHODS


class QueueFactory:
    QUEUE_METHOD_FAKE = "fake"
    QUEUE_METHOD_MONGODB = "mongodb"

    @staticmethod
    def getQueue(config, storage=None, consumer_id=None):
        queue_config = config.QUEUE_CONFIG
        if queue_config.QUEUE_METHOD == QueueFactory.QUEUE_METHOD_FAKE:
            from mcrit.queue.LocalQueue import LocalQueue
            from mcrit.Worker import Worker

            if storage is None:
                raise ValueError("QueueFactory needs storage to set up LocalQueue")
            queue = LocalQueue()
            queue.set_worker(Worker(queue=queue, storage=storage, config=config))
        else:
            from mcrit.libs.mongoqueue import MongoQueue

            if consumer_id is None:
                raise ValueError("QueueFactory needs consumer_id to set up MongoQueue")
            queue = MongoQueue(
                queue_config,
                consumer_id,
                timeout=queue_config.QUEUE_TIMEOUT,
                max_attempts=queue_config.QUEUE_MAX_ATTEMPTS,
            )
        queue.clean_interval = queue_config.QUEUE_CLEAN_INTERVAL
        queue.retention_seconds = getattr(queue_config, "QUEUE_TRANSIENT_JOB_RETENTION_SECONDS", 0)
        queue.transient_methods = TRANSIENT_JOB_METHODS
        return queue
