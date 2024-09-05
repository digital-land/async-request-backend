import os
from typing import Dict

import sentry_sdk
from celery.utils.log import get_task_logger
from celery.signals import task_prerun, task_success, task_failure, celeryd_init
import request_model.schemas as schemas
import request_model.models as models
import s3_transfer_manager
import crud
import database
from task_interface.check_tasks import celery, CheckDataFileTask
import json
from application.core import workflow
from application.configurations.config import Directories
import application.core.utils as utils
from application.exceptions.customExceptions import CustomException
from pathlib import Path

logger = get_task_logger(__name__)
# Threshold for s3_transfer_manager to automatically use multipart download
max_file_size_mb = 30


@celery.task(base=CheckDataFileTask, name=CheckDataFileTask.name)
def check_datafile(request: Dict, directories=None):
    logger.info("check datafile")
    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params
    if not request_schema.status == "COMPLETE":
        if not directories:
            directories = Directories
        elif directories:
            data_dict = json.loads(directories)
            # Create an instance of the Directories class
            directories = Directories()
            # Update attribute values based on the dictionary
            for key, value in data_dict.items():
                setattr(directories, key, value)

        fileName = ""
        tmp_dir = os.path.join(
            directories.COLLECTION_DIR + "/resource" + f"/{request_schema.id}"
        )
        # Ensure tmp_dir exists, create it if it doesn't
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        if request_data.type == "check_file":
            fileName = request_data.uploaded_filename
            try:
                s3_transfer_manager.download_with_default_configuration(
                    os.environ["REQUEST_FILES_BUCKET_NAME"],
                    request_data.uploaded_filename,
                    f"{tmp_dir}/{request_data.uploaded_filename}",
                    max_file_size_mb,
                )
            except Exception as e:
                logger.error(str(e))
                log = {}
                log["message"] = "The uploaded file not found in S3 bucket"
                log["status"] = ""
                log["exception_type"] = type(e).__name__
                save_response_to_db(request_schema.id, log)
                raise CustomException(log)

        elif request_data.type == "check_url":
            log, content = utils.get_request(request_data.url)
            if content:
                fileName = utils.save_content(content, tmp_dir)
            else:
                save_response_to_db(request_schema.id, log)
                raise CustomException(log)
        response = workflow.run_workflow(
            fileName,
            request_schema.id,
            request_data.collection,
            request_data.dataset,
            "",
            request_data.geom_type if hasattr(request_data, "geom_type") else "",
            (
                request_data.column_mapping
                if hasattr(request_data, "column_mapping")
                else {}
            ),
            directories,
        )
        save_response_to_db(request_schema.id, response)
    return _get_request(request_schema.id)


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


# TODO: Look into retry mechanism with Celery


@task_failure.connect
def after_task_failure(task_id, exception, traceback, einfo, args, **kwargs):
    request_id = args[0]["id"]
    logger.debug(f"Set status to FAILED for request {request_id}")
    _update_request_status(request_id, "FAILED")


@celeryd_init.connect
def init_sentry(**_kwargs):
    if os.environ.get("SENTRY_ENABLED", "false").lower() == "true":
        sentry_sdk.init(
            enable_tracing=os.environ.get("SENTRY_TRACING_ENABLED", "false").lower()
            == "true",
            traces_sample_rate=float(
                os.environ.get("SENTRY_TRACING_SAMPLE_RATE", "0.01")
            ),
            release=os.environ.get("GIT_COMMIT"),
            debug=os.environ.get("SENTRY_DEBUG", "false").lower() == "true",
        )


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


def _get_response(request_id):
    db_session = database.session_maker()
    with db_session() as session:
        return crud.get_response(session, request_id)


def save_response_to_db(request_id, response_data):
    db_session = database.session_maker()
    with db_session() as session:
        try:
            existing = _get_response(request_id)
            if not existing:
                if (
                    "column-field-log" in response_data
                    and "error-summary" in response_data
                    and "converted-csv" in response_data
                    and "issue-log" in response_data
                    and "transformed-csv" in response_data
                ):
                    data = {
                        "column-field-log": response_data.get("column-field-log", {}),
                        "error-summary": response_data.get("error-summary", {}),
                    }
                    # Create a new Response instance
                    new_response = models.Response(request_id=request_id, data=data)

                    # Add the response to the session
                    session.add(new_response)
                    session.flush()  # Flush to get the response ID

                    # Initialize line number
                    entry_number = 1
                    converted_row_data = response_data.get("converted-csv")
                    issue_log_data = response_data.get("issue-log")
                    transformed_data = response_data.get("transformed-csv")
                    # Save converted_row_data and issue_log_data in ResponseDetails
                    for converted_row in converted_row_data:
                        # Collect issue logs corresponding to the current line number
                        current_issue_logs = [
                            issue_log
                            for issue_log in issue_log_data
                            if issue_log.get("entry-number") == str(entry_number)
                        ]
                        transformed_csv = [
                            transformed
                            for transformed in transformed_data
                            if transformed.get("entry-number") == str(entry_number)
                        ]

                        new_response_detail = models.ResponseDetails(
                            response_id=new_response.id,
                            detail={
                                "converted_row": converted_row,
                                "issue_logs": current_issue_logs,
                                "entry_number": entry_number,
                                "transformed_row": transformed_csv,
                            },
                        )
                        session.add(new_response_detail)

                        # Increment line number for the next iteration
                        entry_number += 1

                        session.add(new_response_detail)

                    # Commit the changes to the database
                    session.commit()

                elif "message" in response_data:
                    error = CustomException(response_data)
                    # error_detail_json = json.dumps(error.detail)
                    # error_details = {
                    #     "detail": error.as_dict()
                    # }
                    new_response = models.Response(
                        request_id=request_id, error=error.detail
                    )
                    session.add(new_response)
                    session.commit()
            else:
                logger.exception(
                    "response already exists in DB for request: ", request_id
                )
        except Exception as e:
            session.rollback()
            raise e
