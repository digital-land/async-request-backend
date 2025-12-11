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


# Remove resource directories created by Collector, necessary if exception occurs, workflow will not clean up
def clean_up_request_files(request_id):
    resource_dir = os.path.join(Directories.COLLECTION_DIR, "resource", request_id)
    try:
        if os.path.exists(resource_dir):
            for root, dirs, files in os.walk(resource_dir, topdown=False):
                for name in files:
                    os.remove(os.path.join(root, name))
                for name in dirs:
                    os.rmdir(os.path.join(root, name))
            os.rmdir(resource_dir)
            logger.info(f"Cleaned up resource directory: {resource_dir}")

            # Clean up parent directories if empty
            resource_parent_dir = os.path.dirname(resource_dir)
            if os.path.exists(resource_parent_dir) and not os.listdir(
                resource_parent_dir
            ):
                os.rmdir(resource_parent_dir)
                logger.info(
                    f"Cleaned up resource parent directory: {resource_parent_dir}"
                )

                collection_dir = os.path.dirname(resource_parent_dir)
                if os.path.exists(collection_dir) and not os.listdir(collection_dir):
                    os.rmdir(collection_dir)
                    logger.info(f"Cleaned up collection directory: {collection_dir}")

    except Exception as e:
        logger.error(f"Failed to clean up resource directory {resource_dir}: {e}")


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
        f"Started check_dataurl task for request_id = {request.get('id', 'unknown')}"
    )
    logger.info(f"Request payload: {json.dumps(request, default=str)}")
    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params

    if request_schema.status == "COMPLETE":
        logger.info(f"Request {request_schema.id} already COMPLETE")
        return _get_request(request_schema.id)

    if not directories:
        directories = Directories
    elif directories:
        data_dict = json.loads(directories)
        directories = Directories()
        for key, value in data_dict.items():
            setattr(directories, key, value)

    resource_dir = os.path.join(
        directories.COLLECTION_DIR, "resource", request_schema.id
    )

    file_name = None

    # IMPORTANT: 'message' set in error_log to be user friendly = Map known exception types to user-friendly messages
    try:
        file_name, fetch_log = _fetch_resource(resource_dir, request_data.url)
        logger.info(f"Fetched resource: file_name={file_name}")

    except CustomException as e:
        logger.error(f"URL FETCH Error during _fetch_resource: {e}")
        # Track in Sentry for monitoring (not as error)
        _capture_sentry_event(
            e.detail,
            request_schema.id,
            url=request_data.url,
            task_name="CheckURL",
            is_custom_exception=True,
        )
        error_log = utils.create_user_friendly_error_log(e.detail)
        save_response_to_db(request_schema.id, error_log)

        return _get_request(request_schema.id)

    except Exception as e:
        logger.error(f"Error during _fetch_resource: {e}")
        logger.exception("Full traceback:")
        _capture_sentry_event(
            e, request_schema.id, url=request_data.url, task_name="CheckURL"
        )
        save_response_to_db(
            request_schema.id,
            {"message": "There is a problem with our service, please try again later."},
        )
        return _get_request(request_schema.id)

    if file_name:
        try:
            response = workflow.run_workflow(
                file_name,
                request_schema.id,
                request_data.collection,
                request_data.dataset,
                "",
                getattr(request_data, "geom_type", ""),
                getattr(request_data, "column_mapping", {}),
                directories,
            )
            if "plugin" in fetch_log:
                response["plugin"] = fetch_log["plugin"]
            save_response_to_db(request_schema.id, response)
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            logger.exception("Full traceback:")
            _capture_sentry_event(
                e,
                request_schema.id,
                url=request_data.url,
                task_name="CheckURL",
                extra_context={"workflow_stage": "run_workflow"},
            )
            save_response_to_db(
                request_schema.id,
                {
                    "message": "There is a problem with our service, please try again later."
                },
            )
    else:
        logger.error("File could not be fetched from collector")
        _capture_sentry_event(
            Exception("File could not be fetched - unknown error"),
            request_schema.id,
            url=request_data.url,
            task_name="CheckURL",
            extra_context={"error_type": "unknown_collection_error"},
        )
        save_response_to_db(
            request_schema.id,
            {"message": "There is a problem with our service, please try again later."},
        )

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

        resource_dir = os.path.join(
            directories.COLLECTION_DIR, "resource", request_schema.id
        )
        file_name, log = _fetch_resource(resource_dir, request_data.url)
        logger.info(
            f"file name from fetch resource is : {file_name} and the log from fetch resource is {log}"
        )
        if file_name:
            response = workflow.add_data_workflow(
                file_name,
                request_schema.id,
                request_data.collection,
                request_data.dataset,
                request_data.organisation,
                request_data.url,
                request_data.documentation_url,
                directories,
            )
            if "plugin" in log:
                response["plugin"] = log["plugin"]
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
    clean_up_request_files(request_id)


# TODO: Look into retry mechanism with Celery


@task_failure.connect
def after_task_failure(task_id, exception, traceback, einfo, args, **kwargs):
    request_id = args[0]["id"]
    logger.debug(f"Set status to FAILED for request {request_id}")
    _update_request_status(request_id, "FAILED")
    clean_up_request_files(request_id)


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


# Built with a desire to have context about Check URL errors, even ones that are handled.
def _capture_sentry_event(
    exception_or_detail,
    request_id,
    url=None,
    task_name=None,
    extra_context=None,
    is_custom_exception=False,
):
    """
    Capture exceptions or custom exception details in Sentry.

    Args:
        exception_or_detail: Either an Exception object or a dict (for CustomException detail)
        request_id: The request ID for context
        url: Optional URL being processed
        task_name: Optional task name
        extra_context: Optional dict of additional context
        is_custom_exception: If True, captures as warning message; if False, captures as exception
    """
    with sentry_sdk.push_scope() as scope:
        context = {"id": request_id}
        if url:
            context["url"] = url
        scope.set_context("request", context)

        # Set tags
        if is_custom_exception:
            scope.set_tag("exception_type", "CustomException")
        if task_name:
            scope.set_tag("task", task_name)
        if extra_context:
            for key, value in extra_context.items():
                scope.set_tag(key, value)

        # Capture based on type
        if is_custom_exception:
            # CustomException: add details as extra data and capture as message
            if isinstance(exception_or_detail, dict):
                for key, value in exception_or_detail.items():
                    scope.set_extra(key, str(value))
                message = exception_or_detail.get("message", "CustomException occurred")
            else:
                message = str(exception_or_detail)
            sentry_sdk.capture_message(message, level="warning")
        else:
            # Regular exception: capture as error
            sentry_sdk.capture_exception(exception_or_detail)


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
                        "plugin": response_data.get("plugin", None),
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
                        request_id=request_id, data=response_data
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
                logger.info(f"Response already exists in DB for request: {request_id}")
        except Exception as e:
            session.rollback()
            raise e


def _fetch_resource(resource_dir, url):
    """
    Fetches resource files using Collector, trying different plugins.
    Raises CustomException and logs error if fetch fails.
    """
    Path(resource_dir).mkdir(parents=True, exist_ok=True)
    collector = Collector(resource_dir=Path(resource_dir))
    plugins = [None, "arcgis", "wfs"]
    content_type = None

    for plugin in plugins:
        fetch_status, log = collector.fetch(url, plugin=plugin, refill_todays_logs=True)
        log["fetch-status"] = fetch_status.name
        if plugin is None:
            content_type = log.get("response-headers", {}).get("content-type")
        if fetch_status == FetchStatus.OK:
            log["plugin"] = plugin
            try:
                file_name = next(reversed(list(collector.resource_dir.iterdir()))).name
                return file_name, log
            except StopIteration:
                raise CustomException(
                    {
                        "message": "No endpoint files found after successful fetch.",
                        **log,
                    }
                )
        elif log.get("exception") or log.get("status", "").startswith("4"):
            break

    # All fetch attempts failed - include content-type if available
    error_detail = {"message": "All fetch attempts failed", **log}
    if content_type:
        error_detail["content-type"] = content_type
    raise CustomException(error_detail)
