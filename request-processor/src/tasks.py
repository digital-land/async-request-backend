import os
from typing import Dict
import urllib.parse

import sentry_sdk
from sentry_sdk.integrations.logging import ignore_logger
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
from application.configurations.config import DATASTORE_URL, Directories
import application.core.utils as utils
from application.exceptions.customExceptions import (
    CustomException,
    create_generic_error_log,
)
from pathlib import Path
from digital_land.collection import Collection
from digital_land.collect import Collector, FetchStatus

logger = get_task_logger(__name__)
_DETAIL_BATCH_SIZE = 1000
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

            # Clean up parent directories if empty
            resource_parent_dir = os.path.dirname(resource_dir)
            if os.path.exists(resource_parent_dir) and not os.listdir(
                resource_parent_dir
            ):
                os.rmdir(resource_parent_dir)
                collection_dir = os.path.dirname(resource_parent_dir)
                if os.path.exists(collection_dir) and not os.listdir(collection_dir):
                    os.rmdir(collection_dir)

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


def _download_resource(resource_dir, collection, resource):
    Path(resource_dir).mkdir(parents=True, exist_ok=True)
    resource_url = (
        f"{DATASTORE_URL}{collection}-collection/"
        f"collection/resource/{urllib.parse.quote(resource)}"
    )
    destination = Path(resource_dir) / resource

    try:
        workflow.download_file(resource_url, destination)
    except Exception as e:
        logger.error(str(e))
        log = {
            "message": "The resource could not be downloaded",
            "status": "",
            "exception_type": type(e).__name__,
            "resource": resource,
            "resource-url": resource_url,
        }
        raise CustomException(log) from e

    return resource, {
        "resource": resource,
        "resource-url": resource_url,
        "fetch-status": FetchStatus.OK.name,
    }


def _get_resource_metadata(collection_dir, collection, dataset, resource):
    Path(collection_dir).mkdir(parents=True, exist_ok=True)
    try:
        Collection.download(Path(collection_dir), collection)
        collection_model = Collection(name=collection, directory=collection_dir)
        collection_model.load()
        endpoints = [
            endpoint
            for endpoint in collection_model.resource_endpoints(resource)
            if endpoint
        ]
        organisations = [
            organisation
            for organisation in collection_model.resource_organisations(resource)
            if organisation
        ]
        start_date = collection_model.resource_start_date(resource)
        endpoint_urls = [
            collection_model.endpoint.records[endpoint][-1].get("endpoint-url")
            for endpoint in endpoints
            if endpoint in collection_model.endpoint.records
        ]
        source_rows = [
            source
            for endpoint in endpoints
            for source in collection_model.filtered_sources(
                {"endpoint": endpoint, "pipelines": dataset}
            )
        ]
        documentation_urls = [
            source.get("documentation-url")
            for source in source_rows
            if source.get("documentation-url")
        ]
        licences = [
            source.get("licence") for source in source_rows if source.get("licence")
        ]
    except Exception as e:
        log = {
            "message": "The resource metadata could not be found",
            "status": "",
            "exception_type": type(e).__name__,
            "resource": resource,
            "collection": collection,
        }
        raise CustomException(log) from e

    if not endpoints or not organisations:
        log = {
            "message": "The resource metadata is missing endpoints or organisations",
            "status": "",
            "exception_type": "MissingResourceMetadata",
            "resource": resource,
            "collection": collection,
            "endpoints": endpoints,
            "organisations": organisations,
        }
        raise CustomException(log)

    return {
        "endpoints": endpoints,
        "organisation": organisations[0],
        "start_date": start_date,
        "url": endpoint_urls[0] if endpoint_urls else None,
        "documentation_url": documentation_urls[0] if documentation_urls else None,
        "licence": licences[0] if licences else None,
    }


@celery.task(base=CheckDataUrlTask, name=CheckDataUrlTask.name)
def check_dataurl(request: Dict, directories=None):  # noqa
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
        file_name, fetch_log = _fetch_resource(
            resource_dir,
            request_data.url,
            endpoint_parameters=request_data.endpoint_parameters,
        )
        logger.info(f"Fetched resource: file_name={file_name}")

    except CustomException as e:
        error_log = utils.create_user_friendly_error_log(e.detail)
        # Track in Sentry for monitoring (not as error)
        _capture_sentry_event(
            error_log,
            request_schema.id,
            handled=True,
        )
        save_response_to_db(request_schema.id, error_log)

        return _get_request(request_schema.id)

    except Exception as e:
        logger.error(f"Error during _fetch_resource: {e}")
        error_log = create_generic_error_log(
            url=request_data.url, exception=e, status=getattr(e, "status", None)
        )
        _capture_sentry_event(
            error_log,
            request_schema.id,
        )
        save_response_to_db(request_schema.id, error_log)
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
            sentry_sdk.metrics.count("async.url_submission.success", 1)
        except Exception as e:
            logger.error(f"Workflow failed: {e}")
            plugin = fetch_log.get("plugin") if "plugin" in fetch_log else None
            error_log = create_generic_error_log(
                url=request_data.url,
                exception=e,
                status=getattr(e, "status", None),
                plugin=plugin,
                extra_context={"workflow_stage": "run_workflow"},
            )
            _capture_sentry_event(
                error_log,
                request_schema.id,
            )
            save_response_to_db(request_schema.id, error_log)
    else:
        logger.info("File could not be fetched from collector")
        error_log = create_generic_error_log(
            url=request_data.url,
            exception="UnknownCollectionError",
            status=None,
            extra_context={"error_type": "unknown_collection_error"},
        )
        _capture_sentry_event(
            error_log,
            request_schema.id,
        )
        save_response_to_db(request_schema.id, error_log)

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
        if request_data.url:
            file_name, log = _fetch_resource(
                resource_dir,
                request_data.url,
                endpoint_parameters=request_data.endpoint_parameters,
            )
            organisations = request_data.organisation
            endpoints = None
            resource_start_date = request_data.start_date
            resource_url = request_data.url
            documentation_url = request_data.documentation_url
            licence = request_data.licence
        elif request_data.resource:
            file_name, log = _download_resource(
                resource_dir, request_data.collection, request_data.resource
            )
            collection_dir = os.path.join(directories.COLLECTION_DIR, request_schema.id)
            metadata = _get_resource_metadata(
                collection_dir,
                request_data.collection,
                request_data.dataset,
                request_data.resource,
            )
            organisations = metadata["organisation"]
            endpoints = metadata["endpoints"]
            resource_start_date = metadata["start_date"]
            resource_url = request_data.url or metadata["url"]
            documentation_url = (
                request_data.documentation_url or metadata["documentation_url"]
            )
            licence = request_data.licence or metadata["licence"]
        else:
            log = {
                "message": "Either url or resource must be supplied for add_data",
                "status": "",
                "exception_type": "MissingAddDataInput",
            }
            save_response_to_db(request_schema.id, log)
            raise CustomException(log)

        # Auto detect plugin needs to update request_data.plugin for downstream processing
        if "plugin" in log:
            request_data.plugin = log["plugin"]
        logger.info(
            f"file name from fetch resource is : {file_name} and the log from fetch resource is {log}"
        )
        if file_name:
            response = workflow.add_data_workflow(
                file_name,
                request_schema.id,
                request_data.collection,
                request_data.dataset,
                organisations,
                resource_url,
                documentation_url,
                directories,
                licence,
                resource_start_date,
                request_data.plugin,
                geom_type=getattr(request_data, "geom_type", ""),
                column_mapping=getattr(request_data, "column_mapping", {}),
                github_branch=request_data.github_branch,
                endpoint_parameters=request_data.endpoint_parameters,
                endpoints=endpoints,
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
        # esridump logs ERROR-level on handled/expected failures (e.g. probing
        # non-ArcGIS URLs with the arcgis fallback plugin), which would
        # otherwise be captured as spurious Sentry issues.
        ignore_logger("esridump.dumper")
        sentry_sdk.init(
            enable_tracing=os.environ.get("SENTRY_TRACING_ENABLED", "false").lower()
            == "true",
            traces_sample_rate=float(
                os.environ.get("SENTRY_TRACING_SAMPLE_RATE", "0.01")
            ),
            release=os.environ.get("GIT_COMMIT"),
            debug=os.environ.get("SENTRY_DEBUG", "false").lower() == "true",
            enable_logs=True,
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
    error_log,
    request_id,
    handled=False,
    task_name="CheckURL",
):
    """
    Report CheckURL errors to Sentry.

    Args:
        error_log: A dict containing error details
        request_id: The request ID for context
        handled: If True, this is an expected user error (e.g. an
            unreachable/unsupported URL submitted for checking) - recorded as
            a metric plus a structured log entry, not a Sentry Issue, since
            it isn't an application bug. If False, treated as an unexpected
            error and captured as a Sentry Issue.
        task_name: Task name (defaults to "CheckURL")
    """
    is_dict = isinstance(error_log, dict)
    message = error_log.get("message", "Error occurred") if is_dict else str(error_log)

    if handled:
        sentry_sdk.metrics.count(
            "async.check_url.user_error",
            1,
            attributes={
                "task": task_name,
                "exception": error_log.get("exception") if is_dict else None,
                "status": error_log.get("status") if is_dict else None,
                "plugin": error_log.get("plugin") if is_dict else None,
            },
        )
        attributes = {"request_id": request_id, "task": task_name}
        if is_dict:
            attributes.update(
                {k: str(v) for k, v in error_log.items() if v is not None}
            )
        sentry_sdk.logger.warning(message, attributes=attributes)
        return

    with sentry_sdk.new_scope() as scope:
        context = {"id": request_id}
        if is_dict:
            if "endpoint-url" in error_log:
                context["url"] = error_log["endpoint-url"]
            # Add all error_log fields as extra data
            for key, value in error_log.items():
                scope.set_extra(key, str(value))

        scope.set_context("request", context)
        scope.set_tag("task", task_name)
        scope.set_tag("handled", "False")

        sentry_sdk.capture_message(
            "Check URL Unexpected Error: " + message, level="error"
        )


def _get_response(request_id):
    db_session = database.session_maker()
    with db_session() as session:
        result = crud.get_response(session, request_id)
    return result


def _save_response_details(
    session, response_id, converted_row_data, issue_log_data, transformed_data
):
    issue_log_by_entry = {}
    for issue in issue_log_data:
        key = issue.get("entry-number")
        if key is not None:
            key = str(key)
        issue_log_by_entry.setdefault(key, []).append(issue)

    transformed_by_entry = {}
    for row in transformed_data:
        key = row.get("entry-number")
        if key is not None:
            key = str(key)
        transformed_by_entry.setdefault(key, []).append(row)

    for entry_number, converted_row in enumerate(converted_row_data, start=1):
        entry_key = str(entry_number)
        session.add(
            models.ResponseDetails(
                response_id=response_id,
                detail={
                    "converted_row": converted_row,
                    "issue_logs": issue_log_by_entry.get(entry_key, []),
                    "entry_number": entry_number,
                    "transformed_row": transformed_by_entry.get(entry_key, []),
                },
            )
        )
        if entry_number % _DETAIL_BATCH_SIZE == 0:
            session.flush()
            session.expunge_all()

    session.commit()


def save_response_to_db(request_id, response_data):
    logger.info(f"save_response_to_db started for request_id: {request_id}")
    db_session = database.session_maker()
    with db_session() as session:
        try:
            existing = _get_response(request_id)
            if not existing:
                if (
                    "converted-csv" in response_data
                    and "issue-log" in response_data
                    and "transformed-csv" in response_data
                ):
                    data = {
                        "task-log": response_data.get("task-log", []),
                        "column-mapping": response_data.get("column-mapping", []),
                        "plugin": response_data.get("plugin", None),
                    }
                    new_response = models.Response(request_id=request_id, data=data)
                    session.add(new_response)
                    session.flush()
                    _save_response_details(
                        session,
                        new_response.id,
                        response_data.get("converted-csv"),
                        response_data.get("issue-log"),
                        response_data.get("transformed-csv"),
                    )

                elif "pipeline-summary" in response_data:
                    pipeline_issues = response_data.get("pipeline-issues", [])
                    data = {
                        "pipeline-summary": response_data.get("pipeline-summary"),
                        "endpoint-summary": response_data.get("endpoint-summary"),
                        "source-summary": response_data.get("source-summary"),
                    }
                    new_response = models.Response(request_id=request_id, data=data)
                    session.add(new_response)
                    session.flush()
                    _save_response_details(
                        session,
                        new_response.id,
                        response_data.get("converted-csv", []),
                        pipeline_issues,
                        response_data.get("transformed-csv", []),
                    )

                elif "message" in response_data:
                    error = CustomException(response_data)
                    new_response = models.Response(
                        request_id=request_id, error=error.detail
                    )
                    session.add(new_response)
                    session.commit()

                else:
                    logger.warning(
                        f"save_response_to_db: unrecognised response shape for request_id: {request_id}"
                    )
            else:
                logger.info(f"Response already exists in DB for request: {request_id}")
        except Exception as e:
            session.rollback()
            raise e


def _fetch_resource(resource_dir, url, endpoint_parameters=None):
    """
    Fetches resource files using Collector, trying different plugins.
    Raises CustomException and logs error if fetch fails.
    """
    Path(resource_dir).mkdir(parents=True, exist_ok=True)
    collector = Collector(resource_dir=Path(resource_dir))
    plugins = [None, "arcgis", "wfs"]
    content_type = None

    for plugin in plugins:
        fetch_status, log = collector.fetch(
            url, plugin=plugin, parameters=endpoint_parameters, refill_todays_logs=True
        )
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
            log["plugin"] = plugin  # Save plugin used for arcgis error context
            break

    # All fetch attempts failed - include content-type if available
    error_detail = {"message": "All fetch attempts failed", **log}
    if content_type:
        error_detail["content-type"] = content_type
    raise CustomException(error_detail)
