"""
module to create the celery app and fake tasks to allow tasks to be accessed separetely from different projects
and therefore worker app/api does not need to use the same requirements as worker. In our case stops the api from 
needed to install digital-land and all it's dependencies

when using in the api for a celery set up the task classes can be instantiated and used to
que on the message broker

when using in the worker you should overwrite the task name with the functionality
"""

import os
from typing import Dict, Optional

from celery import Task, Celery

# celery object with same broker instantiation from the command line to feed both app
# and worker
celery = Celery("async-request-processor", broker=os.environ["CELERY_BROKER_URL"])


# classes to be used in both app and worker, in app used to que task,
# in the worker you should override the task name with the correct task
class CheckDataFileTask(Task):
    name = "task_interface.check_datafile_task"

    def run(self, request: Dict, directories: Optional[str] = None):
        raise NotImplementedError("Base class must implement")
    
class CheckDataUrlTask(Task):
    name = "task_interface.check_url_task"

    def run(self, request: Dict, directories: Optional[str] = None):
        raise NotImplementedError("Base class must implement")
