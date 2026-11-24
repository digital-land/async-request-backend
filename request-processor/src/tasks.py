import os
from datetime import datetime, timezone
from typing import Dict
import urllib.parse
from sqlalchemy import or_

from celery.utils.log import get_task_logger
from celery.signals import task_prerun, task_success, task_failure, celeryd_init
import request_model.schemas as schemas
import request_model.models as models
from request_model.url_safety import UnsafeUrlError, check_fetch_url
import s3_transfer_manager
from request_results import publish_request_result
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
from application.security.resource_safety import (
    UnsafeResourceError,
    check_resource_is_safe,
)
from application.security.request_cleanup import clean_up_request_files
from application.logging import monitoring
from application.logging.monitoring import count_security_rejection
from application.security.safe_fetch import (
    MAX_RESPONSE_BYTES,
    MAX_RESPONSE_SECONDS,
    ResponseTooLargeError,
    ResponseTooSlowError,
    check_host_resolves_publicly,
    untrusted_fetches,
)
from application.security.task_guard import (
    BrokerMessageGuard,
    UnsafeRequestParams,
    UntrustedTaskMessage,
    is_valid_request_id,
)
from application.core.plan_datasets import split_dataset_values
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


class _DatabaseBrokerMessageGuard(BrokerMessageGuard):
    """Checks delivered messages against the request rows in the database."""

    def get_stored_request(self, request_id):
        return _get_request(request_id)


class GuardedCheckDataFileTask(_DatabaseBrokerMessageGuard, CheckDataFileTask):
    expected_type = schemas.RequestTypeEnum.check_file.value


class GuardedCheckDataUrlTask(_DatabaseBrokerMessageGuard, CheckDataUrlTask):
    expected_type = schemas.RequestTypeEnum.check_url.value


class GuardedAddDataTask(_DatabaseBrokerMessageGuard, AddDataTask):
    expected_type = schemas.RequestTypeEnum.add_data.value


@celery.task(base=GuardedCheckDataFileTask, name=CheckDataFileTask.name)
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

    # An uploaded file can be unsafe to convert just as a fetched one can;
    # see resource_safety.
    try:
        check_resource_is_safe(os.path.join(tmp_dir, fileName))
    except UnsafeResourceError as e:
        count_security_rejection("content", "upload")
        logger.warning(f"Rejected uploaded file {fileName}: {e}")
        log = {
            "message": "The uploaded file is not supported",
            "status": "",
            "exception_type": "UnsafeResourceError",
            "exception": "UnsafeResourceError",
            "user_message_detail": str(e),
        }
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


@celery.task(base=GuardedCheckDataUrlTask, name=CheckDataUrlTask.name)
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
        monitoring.record_check_url_user_error(error_log, request_schema.id)
        save_response_to_db(request_schema.id, error_log)

        return _get_request(request_schema.id)

    except Exception as e:
        monitoring.log_check_url_failure("fetch", request_schema.id)
        error_log = create_generic_error_log(
            url=request_data.url, exception=e, status=getattr(e, "status", None)
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
            monitoring.count_check_url_success()
        except Exception as e:
            monitoring.log_check_url_failure("run_workflow", request_schema.id)
            plugin = fetch_log.get("plugin") if "plugin" in fetch_log else None
            error_log = create_generic_error_log(
                url=request_data.url,
                exception=e,
                status=getattr(e, "status", None),
                plugin=plugin,
                extra_context={"workflow_stage": "run_workflow"},
            )
            save_response_to_db(request_schema.id, error_log)
    else:
        # Not an exception, but not expected either: _fetch_with_plugins
        # raises when every fetch fails.
        logger.error(
            "check_url fetched no file",
            extra={"request_id": request_schema.id, "stage": "fetch"},
        )
        error_log = create_generic_error_log(
            url=request_data.url,
            exception="UnknownCollectionError",
            status=None,
            extra_context={"error_type": "unknown_collection_error"},
        )
        save_response_to_db(request_schema.id, error_log)

    return _get_request(request_schema.id)


@celery.task(base=GuardedAddDataTask, name=AddDataTask.name)
def add_data_task(request: Dict, directories=None):
    logger.info(f"Started add_data task for request_id={request.get('id', 'unknown')}")
    logger.info(f"Request payload: {json.dumps(request, default=str)}")
    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params
    logger.info(f"request_payload_params: {json.dumps(request_data, default=str)}")
    # A saved response may exist after an S3 failure or a worker redelivery.
    # Publish that exact result rather than allocating entity numbers again.
    saved_response = _get_response(request_schema.id)
    if saved_response is None and request_schema.status != "COMPLETE":
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
            file_name, log = _fetch_add_data_resource(request_schema, resource_dir)
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
                excluded_references=getattr(request_data, "excluded_references", None),
                selected_redirects=getattr(request_data, "selected_redirects", None),
                authoritative_organisation=(
                    request_data.authoritative_organisation
                    if request_data.authoritative is False
                    else None
                ),
            )
            if "plugin" in log:
                response["plugin"] = log["plugin"]
            save_response_to_db(request_schema.id, response)
        else:
            save_response_to_db(request_schema.id, log)
            raise CustomException(log)
    result = _get_request(request_schema.id)
    _publish_request_result(result)
    return result


def _fetch_add_data_resource(request_schema, resource_dir):
    """
    Fetch an add_data request's URL. If the fetch fails, save why, as
    check_dataurl does, so the request is not just marked FAILED with no error.
    """
    request_data = request_schema.params
    try:
        return _fetch_resource(
            resource_dir,
            request_data.url,
            endpoint_parameters=request_data.endpoint_parameters,
        )
    except CustomException as e:
        save_response_to_db(
            request_schema.id, utils.create_user_friendly_error_log(e.detail)
        )
        raise


def _request_id_from_args(args):
    """Return the request id from task args, or None if it is not a valid id."""
    try:
        request_id = args[0]["id"]
    except (IndexError, KeyError, TypeError):
        return None
    if not is_valid_request_id(request_id):
        logger.error(f"Ignoring task signal for invalid request id {request_id!r}")
        return None
    return request_id


@task_prerun.connect
def before_task(task_id, task, args, **kwargs):
    if isinstance(task, BrokerMessageGuard):
        try:
            stored_status = task.validate_message(args, kwargs.get("kwargs") or {})
        except UntrustedTaskMessage as e:
            # Leave the request untouched; __call__ raises the same error.
            # Counted here, once per delivery: __call__ reuses this result.
            count_security_rejection("message", "broker")
            logger.error(f"Rejected untrusted task message {task_id}: {e}")
            return
        except UnsafeRequestParams as e:
            # A real request: __call__ raises the same error and
            # after_task_failure fails the request with an error.
            count_security_rejection("params", "broker")
            logger.warning(f"Refused params for task {task_id}: {e}")
            return
        if stored_status == "PROCESSING":
            logger.warning(f"Task {task_id} redelivered for a PROCESSING request")
            monitoring.count_task_redelivered(task.name)
    request_id = _request_id_from_args(args)
    if request_id is None:
        return
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, "PROCESSING")


@task_success.connect
def after_task_success(sender, result, **kwargs):
    request_id = _request_id_from_args(sender.request.args)
    if request_id is None:
        return
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, "COMPLETE")
    clean_up_request_files(request_id)


# TODO: Look into retry mechanism with Celery


@task_failure.connect
def after_task_failure(task_id, exception, traceback, einfo, args, **kwargs):
    if isinstance(exception, UntrustedTaskMessage):
        # The message was forged: don't touch the request row or the filesystem.
        logger.error(f"Rejected untrusted task message {task_id}: {exception}")
        return
    request_id = _request_id_from_args(args)
    if request_id is None:
        return
    if isinstance(exception, UnsafeRequestParams):
        save_response_to_db(
            request_id,
            {
                "message": exception.user_message,
                "status": "",
                "exception": "UnsafeRequestParams",
            },
        )
    logger.debug(f"Set status to FAILED for request {request_id}")
    monitoring.count_task_failure(
        exception, getattr(kwargs.get("sender"), "name", None)
    )
    _update_request_status(request_id, "FAILED")
    if args[0].get("type") == schemas.RequestTypeEnum.add_data:
        try:
            _publish_request_result(_get_request(request_id), status="FAILED")
        except Exception:
            # S3 may itself be the cause of failure. Keep the original error
            # and database status, and still clean up the request files.
            logger.exception("Failed to publish failed add-data request %s", request_id)
    clean_up_request_files(request_id)


@celeryd_init.connect
def init_sentry(**_kwargs):
    monitoring.init_sentry()


def _publish_request_result(request, status="COMPLETE"):
    if not publish_request_result(request, status=status):
        return
    published_at = datetime.now(timezone.utc)
    try:
        with database.session_maker()() as session:
            model = crud.get_request(session, request.id)
            model.published_at = published_at
            session.commit()
    except Exception:
        # S3 already accepted the result; bookkeeping must not fail the task.
        logger.exception(
            "Failed to record publication timestamp for request %s", request.id
        )


def _update_request_status(request_id, status):
    db_session = database.session_maker()
    with db_session() as session:
        query = session.query(models.Request).filter(models.Request.id == request_id)
        if status != "COMPLETE":
            # Keep completed add-data requests terminal across overlapping deliveries.
            # The predicate must be part of the UPDATE, not a separate read/check.
            query = query.filter(
                or_(
                    models.Request.type != schemas.RequestTypeEnum.add_data,
                    models.Request.status != "COMPLETE",
                )
            )
        updated = query.update(
            {models.Request.status: status}, synchronize_session=False
        )
        if not updated and crud.get_request(session, request_id) is None:
            logger.error(f"Cannot set status {status}: request {request_id} not found")
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


def _get_datasets_in_resource(transformed_rows):
    """Return distinct dataset values across the complete, mapped check output."""
    return list(
        dict.fromkeys(
            value
            for row in transformed_rows
            if row.get("field") in ("dataset", "datasets")
            for value in split_dataset_values(row.get("value"))
        )
    )


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
                        "datasets-in-resource": _get_datasets_in_resource(
                            response_data["transformed-csv"]
                        ),
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
                    if "errMsg" in response_data:
                        error_detail = dict(response_data)
                        error_detail["errMsg"] = error_detail.pop("message")
                    else:
                        error_detail = CustomException(response_data).detail
                    new_response = models.Response(
                        request_id=request_id, error=error_detail
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


def _uses_paged_wfs(endpoint_parameters):
    return isinstance(endpoint_parameters, dict) and bool(
        endpoint_parameters.get("page_size")
    )


def _url_not_allowed(error):
    return CustomException(
        {
            "message": "The URL is not allowed",
            "status": "",
            "exception": "UnsafeUrlError",
            "user_message_detail": str(error),
        }
    )


def _fetch_resource(resource_dir, url, endpoint_parameters=None):
    """
    Fetches resource files using Collector, trying different plugins.
    Raises CustomException and logs error if fetch fails.

    The URL is user-supplied, so only public http(s) addresses may be fetched.
    """
    try:
        check_fetch_url(url)
    except UnsafeUrlError as e:
        count_security_rejection("url", "fetch")
        raise _url_not_allowed(e)
    # ogr2ogr fetches outside urllib3, so check its host up front. A host that
    # resolves to an internal address is counted by safe_fetch.
    if _uses_paged_wfs(endpoint_parameters):
        try:
            check_host_resolves_publicly(url)
        except UnsafeUrlError as e:
            raise _url_not_allowed(e)

    Path(resource_dir).mkdir(parents=True, exist_ok=True)
    collector = Collector(resource_dir=Path(resource_dir))
    # Older digital-land versions mount a file: adapter on the Collector's
    # session. Remove it so neither the URL nor a redirect can read local files.
    session = getattr(collector, "session", None)
    if session is not None:
        session.adapters.pop("file:", None)

    with untrusted_fetches():
        file_name, log = _fetch_with_plugins(collector, url, endpoint_parameters)

    # A public URL can still serve a file that is unsafe to convert, so check
    # it before anything converts it.
    if file_name:
        try:
            check_resource_is_safe(os.path.join(resource_dir, file_name))
        except UnsafeResourceError as e:
            count_security_rejection("content", "fetch")
            raise CustomException(
                {
                    "message": "The file at this URL is not supported",
                    "status": "",
                    "exception": "UnsafeResourceError",
                    "user_message_detail": str(e),
                }
            )
    return file_name, log


def _refused_fetch_error(log):
    refusals = {
        ResponseTooLargeError.__name__: (
            "size",
            "The file at this URL is too large",
            f"The file at this URL is larger than {MAX_RESPONSE_BYTES // 1_000_000}MB",
        ),
        ResponseTooSlowError.__name__: (
            "time",
            "The file at this URL took too long to download",
            "The server did not finish sending the file within "
            f"{MAX_RESPONSE_SECONDS // 60:.0f} minutes",
        ),
    }
    exception = log.get("exception")
    if exception not in refusals:
        return None
    layer, message, detail = refusals[exception]
    count_security_rejection(layer, "fetch")
    return CustomException(
        {
            "message": message,
            "status": log.get("status", ""),
            "exception": exception,
            "user_message_detail": detail,
        }
    )


def _fetch_with_plugins(collector, url, endpoint_parameters):
    plugins = [None, "arcgis", "wfs"]
    content_type = None
    response_headers = {}

    for plugin in plugins:
        fetch_status, log = collector.fetch(
            url, plugin=plugin, parameters=endpoint_parameters, refill_todays_logs=True
        )
        log["fetch-status"] = fetch_status.name
        if plugin is None:
            response_headers = log.get("response-headers", {})
            content_type = next(
                (
                    value
                    for key, value in response_headers.items()
                    if key.lower() == "content-type"
                ),
                None,
            )
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
            refused = _refused_fetch_error(log)
            if refused:
                raise refused
            log["plugin"] = plugin  # Save plugin used for arcgis error context
            break

    # All fetch attempts failed - include content-type if available
    error_detail = {"message": "All fetch attempts failed", **log}
    if content_type:
        error_detail["content-type"] = content_type
    if response_headers:
        error_detail["response-headers"] = response_headers
    raise CustomException(error_detail)
