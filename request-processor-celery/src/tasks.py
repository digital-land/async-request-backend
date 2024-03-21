import os
from typing import Dict

from celery.utils.log import get_task_logger
from celery.signals import task_prerun, task_success, task_failure

import request_model.schemas as schemas
import s3_transfer_manager
import crud
import database
from task_interface.check_tasks import celery, CheckDataFileTask

from application.core import workflow
from application.configurations.config import Directories
from pathlib import Path

logger = get_task_logger(__name__)
# Threshold for s3_transfer_manager to automatically use multipart download
max_file_size_mb = 30

tmp_dir = os.path.join(Directories.COLLECTION_DIR + "resource")


@celery.task(base=CheckDataFileTask, name=CheckDataFileTask.name)
def check_datafile(request: Dict):
    logger.info("check datafile")
    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params

    # Ensure tmp_dir exists, create it if it doesn't
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    s3_transfer_manager.download_with_default_configuration(
        os.environ["REQUEST_FILES_BUCKET_NAME"],
        request_data.uploaded_filename,
        f"{tmp_dir}/{request_data.uploaded_filename}",
        max_file_size_mb,
    )
    response_data = workflow.run_workflow(
        request_data.collection, request_data.dataset, "", request_data.geom_type
    )

    return {"request": _get_request(request_schema.id), "response_data": response_data}


@task_prerun.connect
def before_task(task_id, task, args, **kwargs):
    request_id = args[0]["id"]
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, "PROCESSING")


@task_success.connect
def after_task_success(sender, result, **kwargs):
    request_id = sender.request.args[0]["id"]
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, "COMPLETE")


@task_failure.connect
def after_task_failure(task_id, exception, traceback, einfo, args, **kwargs):
    request_id = args[0]["id"]
    logger.debug(f"Set status to FAILED for request {request_id}")
    _update_request_status(request_id, "FAILED")


def _update_request_status(request_id, status):
    with database.SessionLocal() as session:
        model = crud.get_request(session, request_id)
        model.status = status
        session.commit()
        session.flush()


def _get_request(request_id):
    with database.SessionLocal() as session:
        yield crud.get_request(session, request_id)
