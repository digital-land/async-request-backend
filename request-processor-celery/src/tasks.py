import os
import time
from typing import Dict

from celery.utils.log import get_task_logger
from celery.signals import task_prerun, task_success, task_failure

import s3_transfer_manager
import crud
import database
from request_model import schemas
from task_interface.check_tasks import celery, CheckDataFileTask

logger = get_task_logger(__name__)

# Threshold for s3_transfer_manager to automatically use multipart download
max_file_size_mb = 30


@celery.task(base=CheckDataFileTask, name=CheckDataFileTask.name)
def check_datafile(request: Dict):
    logger.info('check datafile')
    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params

    s3_transfer_manager.download_with_default_configuration(
        os.environ["REQUEST_FILES_BUCKET_NAME"],
        request_data.uploaded_filename,
        f"/tmp/{request_data.uploaded_filename}",
        max_file_size_mb
    )

    # Future: call pipeline here and write error summary to database
    # Simulate processing delay
    time.sleep(5)

    # Remove downloaded file
    os.remove(f"/tmp/{request_data.uploaded_filename}")

    return _get_request(request_schema.id)


@task_prerun.connect
def before_task(task_id, task, args, **kwargs):
    request_id = args[0]['id']
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, 'PROCESSING')


@task_success.connect
def after_task_success(sender, result, **kwargs):
    request_id = sender.request.args[0]['id']
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, 'COMPLETE')


@task_failure.connect
def after_task_failure(task_id, exception, traceback, einfo, args, **kwargs):
    request_id = args[0]['id']
    logger.debug(f"Set status to FAILED for request {request_id}")
    _update_request_status(request_id, 'FAILED')


def _update_request_status(request_id, status):
    db_session = database.session_maker()
    with db_session() as session:
        model = crud.get_request(session, request_id)
        model.status = status
        session.commit()
        session.flush()


def _get_request(request_id):
    db_session = database.session_maker()
    with db_session() as session:
        yield crud.get_request(session, request_id)
