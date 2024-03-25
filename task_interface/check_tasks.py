import os
from typing import Dict, Optional

from celery import Task, Celery

celery = Celery("async-request-processor", broker=os.environ["CELERY_BROKER_URL"])


class CheckDataFileTask(Task):
    name = "task_interface.check_datafile_task"

    def run(self, request: Dict, directories: Optional[str] = None):
        raise NotImplementedError("Base class must implement")
