import os

broker_transport_options = {
    "region": os.environ["CELERY_BROKER_REGION"],
    "is_secure": os.environ.get("CELERY_BROKER_IS_SECURE", "false").lower() == "true",
    # Must be >= task_time_limit so SQS doesn't re-deliver a message mid-execution.
    "visibility_timeout": int(
        os.environ.get("CELERY_BROKER_VISIBILITY_TIMEOUT", "1800")
    ),
}

broker_connection_retry_on_startup = True

# Raise SoftTimeLimitExceeded at 29 min to allow graceful cleanup, hard kill at 30 min.
task_soft_time_limit = 1740
task_time_limit = 1800

# Late ack means the task messages will be acknowledged __after__ the task has been executed,
# not right before, which is the default behavior.
task_acks_late = True

# Even if task_acks_late is enabled, the worker will acknowledge tasks
# when the worker process executing them abruptly exits or is signaled (e.g., KILL/INT, etc).
task_reject_on_worker_lost = True

# How many messages to prefetch at a time multiplied by the number of concurrent processes.
# The default is 4 (four messages for each process).
# We only want 1 message prefetched per worker to give maximum possibility for other workers (including other instances)
# to have visibility of messages on the SQS queue
worker_prefetch_multiplier = 1
