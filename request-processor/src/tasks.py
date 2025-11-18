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
from task_interface.base_tasks import (
    celery,
    CheckDataFileTask,
    CheckDataUrlTask,
    AddDataTask,
)
import json
from application.core import workflow
from application.configurations.config import Directories
import application.core.utils as utils
from application.exceptions.customExceptions import CustomException
from pathlib import Path
from digital_land.collect import Collector, FetchStatus

logger = get_task_logger(__name__)
# Threshold for s3_transfer_manager to automatically use multipart download
max_file_size_mb = 30


@celery.task(base=CheckDataFileTask, name=CheckDataFileTask.name)
def check_datafile(request: Dict, directories=None):
    logger.info(
        f"Started check_datafile task for request_id={request.get('id', 'unknown')}"
    )
    logger.debug(f"Request payload: {json.dumps(request, default=str)}")
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

        tmp_dir = os.path.join(
            directories.COLLECTION_DIR, "resource", request_schema.id
        )
        # Ensure tmp_dir exists, create it if it doesn't
        Path(tmp_dir).mkdir(parents=True, exist_ok=True)
        fileName = handle_check_file(request_schema, request_data, tmp_dir)

        log = {
            "message": "No file processed",
            "status": "",
            "exception_type": "File processing failed",
        }

        if fileName:
            logger.info(f"Running workflow for file: {fileName}")
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
            logger.info(
                f"Workflow completed and response saved for request_id={request_schema.id}"
            )
        else:
            logger.error(
                f"No fileName found for request_id={request_schema.id}, saving error log and raising exception."
            )
            save_response_to_db(request_schema.id, log)
            raise CustomException(log)
    return _get_request(request_schema.id)


def handle_check_file(request_schema, request_data, tmp_dir):
    fileName = request_data.uploaded_filename
    try:
        logger.info(f"Attempting to download file {fileName} from S3 to {tmp_dir}")
        s3_transfer_manager.download_with_default_configuration(
            os.environ["REQUEST_FILES_BUCKET_NAME"],
            request_data.uploaded_filename,
            f"{tmp_dir}/{request_data.uploaded_filename}",
            max_file_size_mb,
        )
        logger.info(f"File {fileName} downloaded successfully.")
    except Exception as e:
        logger.error(str(e))
        log = {}
        log["message"] = "The uploaded file not found in S3 bucket"
        log["status"] = ""
        log["exception_type"] = type(e).__name__
        save_response_to_db(request_schema.id, log)
        raise CustomException(log)
    return fileName


@celery.task(base=CheckDataUrlTask, name=CheckDataUrlTask.name)
def check_dataurl(request: Dict, directories=None):
    logger.info(
        f"Started check_dataurl task for request_id={request.get('id', 'unknown')}"
    )
    logger.debug(f"Request payload: {json.dumps(request, default=str)}")
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

        file_name = ""
        resource_dir = os.path.join(directories.COLLECTION_DIR, "resource", request_schema.id)
        log_dir = os.path.join(directories.COLLECTION_DIR, "log", request_schema.id)
        file_name, log = _fetch_resource(
            directories.COLLECTION_DIR,
            resource_dir,
            log_dir,
            request_data.url,
            getattr(request_data, "plugin", None)
        )
        if file_name:
            response = workflow.run_workflow(
                file_name,
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
        else:
            save_response_to_db(request_schema.id, log)
            raise CustomException(log)
    return _get_request(request_schema.id)


@celery.task(base=AddDataTask, name=AddDataTask.name)
def add_data_task(request: Dict, directories=None):
    logger.info(f"Started add_data task for request_id={request.get('id', 'unknown')}")
    logger.info(f"Request payload: {json.dumps(request, default=str)}")
    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params
    logger.info(f"request_payload_params: {json.dumps(request_data, default=str)}")
    if not request_schema.status == "COMPLETE":
        if not directories:
            directories = Directories
        else:
            data_dict = json.loads(directories)
            directories = Directories()
            for key, value in data_dict.items():
                setattr(directories, key, value)

        resource_dir = os.path.join(directories.COLLECTION_DIR, "resource", request_schema.id)
        log_dir = os.path.join(directories.COLLECTION_DIR, "log", request_schema.id)
        file_name, log = _fetch_resource(
            directories.COLLECTION_DIR,
            resource_dir,
            log_dir,
            request_data.url,
            getattr(request_data, "plugin", None)
        )
        logger.info(f"file name from fetch resource is : {file_name} and the log from fetch resource is {log}")
        if file_name:
            response = workflow.add_data_workflow(
                file_name,
                request_schema.id,
                request_data.collection,
                request_data.dataset,
                request_data.organisation,
                directories,
            )
            logger.info(f"response is : {response}")
            save_response_to_db(request_schema.id, response)
        else:
            save_response_to_db(request_schema.id, log)
            raise CustomException(log)
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
        result = crud.get_request(session, request_id)
    return result


def _get_response(request_id):
    db_session = database.session_maker()
    with db_session() as session:
        result = crud.get_response(session, request_id)
    return result


def save_response_to_db(request_id, response_data):
    logger.info(f"save_response_to_db started for request_id: {request_id}")
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

                elif "entity-summary" in response_data:
                    new_response = models.Response(
                        request_id=request_id,
                        data=response_data
                    )
                    session.add(new_response)
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


def _fetch_resource(collection_dir, resource_dir, log_dir, url, plugin=None):
    """
    Fetches resource files using Collector,
    Raises CustomException and logs error if fetch fails.
    """

    # Ensure tmp_dir exists, create it if it doesn't
    Path(resource_dir).mkdir(parents=True, exist_ok=True)
    # With Collector from digital-land/collect, edit to use correct directory path without changing Collector class
    collector = Collector(collection_dir=Path(collection_dir))
    # Override the resource_dir to match our tmp_dir structure
    collector.resource_dir = Path(resource_dir)  # Use the same directory as tmp_dir
    collector.log_dir = Path(log_dir)
    # TBD: Can test infering plugin from URL, then if fails retry normal method without plugin?
    # if 'FeatureServer' in request_data.url or 'MapServer' in request_data.url:
    #     request_data.plugin = "arcgis"

    status = collector.fetch(url, plugin=plugin)
    logger.info(f"Collector Fetch status: {status}")

    # The resource is saved in collector.resource_dir with hash as filename
    resource_files = list(collector.resource_dir.iterdir())

    log = {}

    if status == FetchStatus.OK:
        if resource_files and len(resource_files) == 1:
            logger.info(f"Resource Files Path from collector: {resource_files}")
            file_name = resource_files[-1].name  # Get the hash filename
            logger.info(f"File Hash From Collector: {file_name}")

        else:
            log["message"] = "No endpoint files found after successful fetch."
            log["status"] = str(status)
            log["exception_type"] = "URL check failed"
            raise CustomException(log)
    else:
        log["status"] = str(status)
        log["message"] = "Fetch operation failed"
        log["exception_type"] = "URL check failed"
        logger.warning(f"URL check failed with fetch status: {status}")
        raise CustomException(log)
    return file_name, log

