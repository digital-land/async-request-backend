import os
from typing import Dict

from celery import Task, Celery

celery = Celery('async-request-processor', broker=os.environ['CELERY_BROKER_URL'])


class CheckDataFileTask(Task):
    name = 'request_tasks.check_datafile_task'

    def run(self, request: Dict):
        raise NotImplemented('Base class must implement')
