import os
import json
from pathlib import Path
from typing import Dict, List, Optional
from celery import chain

import sentry_sdk
from celery.utils.log import get_task_logger
from celery.signals import task_prerun, task_success, task_failure, celeryd_init

import request_model.schemas as schemas
import request_model.models as models
import s3_transfer_manager
import crud
import database

from task_interface.check_tasks import celery, CheckDataFileTask
from application.core import workflow
from application.configurations.config import Directories
import application.core.utils as utils
from application.exceptions.customExceptions import CustomException

logger = get_task_logger(__name__)

max_file_size_mb = 30


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


def _resolve_directories(directories) -> Directories:
    """Normalize directories argument into a Directories instance."""
    if directories is None:
        return Directories()
    if isinstance(directories, Directories):
        return directories
    if isinstance(directories, str):
        try:
            data = json.loads(directories)
            d = Directories()
            for k, v in data.items():
                setattr(d, k, v)
            return d
        except Exception:
            logger.warning("Failed to parse directories JSON; using defaults")
            return Directories()
    logger.warning("Unsupported directories value; using defaults")
    return Directories()


def _ensure_tmp_dir(directories: Directories, request_id: str) -> str:
    tmp_dir = os.path.join(directories.COLLECTION_DIR, "resource", request_id)
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)
    return tmp_dir


def _get_content_from_url(url: str, request_id: str, tmp_dir: str) -> Optional[str]:
    """Fetch content from a URL, save it, and return the filename."""
    log, content = utils.get_request(url)
    if content:
        if utils.check_content(content):
            return utils.save_content(content, tmp_dir)
        else:
            log = {
                "message": "EndpointURL includes multiple dataset layers.",
                "status": "",
                "exception_type": "URL check failed",
            }
    save_response_to_db(request_id, log)
    logger.warning(f"URL fetch for request {request_id} failed: {log}")
    return None


def _log_incoming_config(request_data):
    """Helper to log the incoming request parameters."""
    try:
        logger.info(
            json.dumps(
                {
                    "collection": getattr(request_data, "collection", None),
                    "dataset": getattr(request_data, "dataset", None),
                    "url": getattr(request_data, "url", None),
                    "column_mapping": getattr(request_data, "column_mapping", None),
                    "geom_type": getattr(request_data, "geom_type", None),
                    "documentation_url": (
                        str(request_data.documentation_url)
                        if getattr(request_data, "documentation_url", None)
                        else None
                    ),
                    "licence": getattr(request_data, "licence", None),
                    "start_date": (
                        str(request_data.start_date)
                        if getattr(request_data, "start_date", None)
                        else None
                    ),
                    "organisation": getattr(request_data, "organisation", None),
                },
                indent=2,
                default=str,
            )
        )
    except Exception as e:
        logger.warning(f"Failed to log incoming config: {e}")


@celery.task(base=CheckDataFileTask, name="add_data.prepare")
def add_data_prepare(request: Dict, directories=None):
    request_schema = schemas.Request.model_validate(request)
    params = request_schema.params
    dirs = _resolve_directories(directories)
    tmp_dir = _ensure_tmp_dir(dirs, request_schema.id)
    logger.info(
        json.dumps(
            {
                "phase": "add_data.prepare",
                "request_id": request_schema.id,
                "dataset": params.dataset,
                "collection": params.collection,
            }
        )
    )
    _log_incoming_config(params)
    return {
        "request_id": request_schema.id,
        "request": request,
        "directories": {},
        "tmp_dir": tmp_dir,
        "file_name": None,
    }


@celery.task(base=CheckDataFileTask, name="add_data.fetch")
def add_data_fetch(ctx: Dict):
    request_schema = schemas.Request.model_validate(ctx["request"])
    params = request_schema.params
    logger.info(
        json.dumps({"phase": "add_data.fetch", "request_id": request_schema.id})
    )
    tmp_dir = ctx["tmp_dir"]
    file_name = None
    if getattr(params, "url", None):
        file_name = _get_content_from_url(params.url, request_schema.id, tmp_dir)
    elif getattr(params, "content", None):
        file_name = utils.save_content(params.content.encode("utf-8"), tmp_dir)
    ctx["file_name"] = file_name  # Still tiny (just a string)
    return ctx


@celery.task(base=CheckDataFileTask, name="add_data.pipeline")
def add_data_pipeline(ctx: Dict):
    """
    Runs workflow OR preview and immediately persists to DB.
    Does NOT return large response content in task message.
    """
    request_schema = schemas.Request.model_validate(ctx["request"])
    params = request_schema.params
    logger.info(
        json.dumps({"phase": "add_data.pipeline", "request_id": request_schema.id})
    )

    dirs = Directories()  # reconstruct fresh
    file_name = ctx.get("file_name")

    # Preview path (source_request_id with no new file)
    if getattr(params, "source_request_id", None) and not file_name:
        original = _get_response(params.source_request_id)
        if original and original.data:
            preview_response = workflow.run_preview_workflow(
                request_schema.id, params, dirs, original.data
            )
            save_response_to_db(request_schema.id, preview_response)
            return {"request_id": request_schema.id}

    # Missing file case
    if not file_name:
        save_response_to_db(
            request_schema.id,
            {
                "message": "No file content for add_data",
                "status": "",
                "exception_type": "FileMissing",
            },
        )
        return {"request_id": request_schema.id}

    org = (getattr(params, "organisation", "") or "").strip()
    response_data = workflow.run_workflow(
        file_name,
        request_schema.id,
        params.collection,
        params.dataset,
        org,
        dirs,
        request_data=params,
    )
    save_response_to_db(request_schema.id, response_data)
    return {"request_id": request_schema.id}


@celery.task(base=CheckDataFileTask, name="add_data.finalize")
def add_data_finalize(ctx: Dict):
    request_id = ctx["request_id"]
    logger.info(json.dumps({"phase": "add_data.finalize", "request_id": request_id}))
    # Status COMPLETE will also be set by task_success signal; explicit retrieval fine
    return _get_request(request_id)


def _schedule_add_data_chain(request: Dict, directories=None):
    chain(
        add_data_prepare.s(request, directories),
        add_data_fetch.s(),
        add_data_pipeline.s(),
        add_data_finalize.s(),
    ).apply_async()


def _handle_check_file_task(request_schema, params, tmp_dir, directories_obj):
    file_name = handle_check_file(request_schema, params, tmp_dir)
    org = (getattr(params, "organisation", "") or "").strip()
    response = workflow.run_workflow(
        file_name,
        request_schema.id,
        params.collection,
        params.dataset,
        org,
        directories_obj,
        request_data=params,
    )
    save_response_to_db(request_schema.id, response)
    return _get_request(request_schema.id)


def _handle_check_url_task(request_schema, params, tmp_dir, directories_obj):
    logger.info("== Incoming config for check_url ==")
    _log_incoming_config(params)
    file_name = _get_content_from_url(params.url, request_schema.id, tmp_dir)
    if not file_name:
        logger.info("No file to process, returning.")
        return _get_request(request_schema.id)
    org = (getattr(params, "organisation", "") or "").strip()
    response = workflow.run_workflow(
        file_name,
        request_schema.id,
        params.collection,
        params.dataset,
        org,
        directories_obj,
        request_data=params,
    )
    save_response_to_db(request_schema.id, response)
    return _get_request(request_schema.id)


@celery.task(base=CheckDataFileTask, name=CheckDataFileTask.name)
def check_datafile(request: Dict, directories=None):
    logger.info("check datafile")
    logger.info("Received payload:")
    logger.info(json.dumps(request, indent=2, default=str))

    request_schema = schemas.Request.model_validate(request)
    params = request_schema.params

    if request_schema.status == "COMPLETE":
        return _get_request(request_schema.id)

    if params.type == "add_data":
        logger.info(
            json.dumps(
                {
                    "phase": "add_data.dispatch",
                    "request_id": request_schema.id,
                    "dataset": params.dataset,
                }
            )
        )
        _schedule_add_data_chain(request, directories)
        return _get_request(request_schema.id)

    directories_obj = _resolve_directories(directories)
    tmp_dir = _ensure_tmp_dir(directories_obj, request_schema.id)

    if params.type == "check_file":
        return _handle_check_file_task(request_schema, params, tmp_dir, directories_obj)
    elif params.type == "check_url":
        return _handle_check_url_task(request_schema, params, tmp_dir, directories_obj)
    else:
        logger.warning(f"Unknown request type: {params.type}")
        return _get_request(request_schema.id)


def handle_check_file(request_schema, request_data, tmp_dir):
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
        log = {
            "message": "The uploaded file not found in S3 bucket",
            "status": "",
            "exception_type": type(e).__name__,
        }
        save_response_to_db(request_schema.id, log)
        raise CustomException(log)
    return fileName


@task_prerun.connect
def before_task(task_id, task, args, **kwargs):
    request_id = args[0]["id"]
    logger.debug(f"Set status to PROCESSING for request {request_id}")
    _update_request_status(request_id, "PROCESSING")


@task_success.connect
def after_task_success(sender, result, **kwargs):
    request_id = sender.request.args[0]["id"]
    logger.debug(f"Set status to COMPLETE for request {request_id}")
    _update_request_status(request_id, "COMPLETE")


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


# -----------------------------
# Response persistence
# -----------------------------
def save_response_to_db(request_id, response_data):
    """
    Persist the response.

    If it's a normal check result, also:
      - compute 'new-entities' + 'new-entity-count'
      - compute 'entity-summary' (CLI-style) with breakdown
      - add per-row 'is_new_entity' in ResponseDetails

    If it's an error dict ('message', 'exception_type'), store it as response.error.
    """
    db_session = database.session_maker()
    with db_session() as session:
        try:
            existing = _get_response(request_id)
            if existing:
                logger.exception(
                    "response already exists in DB for request: %s", request_id
                )
                return

            if _handle_error_response(session, request_id, response_data):
                return

            extracted_data = _validate_and_extract_data(
                session, request_id, response_data
            )
            if not extracted_data:
                return

            # Get original request params for context
            req = _get_request(request_id)
            params = getattr(req, "params", None)
            try:
                params_dict = (
                    params.dict() if hasattr(params, "dict") else dict(params or {})
                )
            except Exception:
                params_dict = {}

            entity_summary_data = _build_entity_summary(
                extracted_data["converted"],
                extracted_data["facts"],
                extracted_data["existing_entities"],
                extracted_data["new_entities"],
                params_dict,
            )

            _persist_response_and_details(
                session, request_id, response_data, entity_summary_data, extracted_data
            )

        except Exception as e:
            session.rollback()
            raise e


def _handle_error_response(session, request_id, response_data):
    """Handle and persist an error response, returning True if handled."""
    if "message" in response_data and "exception_type" in response_data:
        error = CustomException(response_data)
        session.add(models.Response(request_id=request_id, error=error.detail))
        session.commit()
        return True
    return False


def _validate_and_extract_data(session, request_id, response_data):
    """Validate response data and extract core components."""
    required = (
        "column-field-log",
        "error-summary",
        "converted-csv",
        "issue-log",
        "transformed-csv",
    )
    if not all(k in response_data for k in required):
        session.add(models.Response(request_id=request_id, data=response_data))
        session.commit()
        return None  # Indicates that persistence is done and further processing should stop

    return {
        "converted": response_data.get("converted-csv") or [],
        "issues": response_data.get("issue-log") or [],
        "facts": response_data.get("transformed-csv") or [],
        "existing_entities": response_data.get("existing-entities", []) or [],
        "new_entities": response_data.get("new-entities", []) or [],
    }


def _ref_from_conv(row: dict) -> str:
    """Extract reference from a converted CSV row."""
    if not row:
        return ""
    for key in (
        "Reference",
        "reference",
        "Entity reference",
        "entity_reference",
        "entity-reference",
        "ListEntry",
        "ref",
    ):
        v = row.get(key)
        if v not in (None, ""):
            return str(v).strip()
    for k, v in row.items():
        if "ref" in (k or "").lower() and v not in (None, ""):
            return str(v).strip()
    return ""


def _build_entity_summary(
    converted: List[dict],
    facts: List[dict],
    existing_entities: List[dict],
    new_entities: List[dict],
    params_dict: dict,
):
    """Build the summary of new and existing entities."""
    # Index existing and new entities by reference for efficient lookups
    existing_by_ref = {
        str(e.get("reference", "")).strip(): e
        for e in existing_entities
        if str(e.get("reference", "")).strip()
    }
    new_by_ref = {
        str(row.get("reference", "")).strip(): row
        for row in new_entities
        if str(row.get("reference", "")).strip()
    }

    # --- Determine references present in this resource ---
    resource_refs = _collect_resource_refs(converted, facts)

    # --- Categorize references into new or existing breakdowns ---
    existing_entity_breakdown, new_entity_breakdown = _categorize_entities(
        resource_refs, existing_by_ref, new_by_ref, params_dict
    )

    logger.info(
        "Entity counts — existing(list): %d, existing(in resource by ref): %d, new(in resource): %d",
        len(existing_entities),
        len(existing_entity_breakdown),
        len(new_entity_breakdown),
    )

    return {
        "summary": {
            "existing-in-resource": len(existing_entity_breakdown),
            "existing-entity-breakdown": existing_entity_breakdown,
            "new-in-resource": len(new_entity_breakdown),
            "new-entity-breakdown": new_entity_breakdown,
        },
        "new_entity_breakdown": new_entity_breakdown,
    }


def _collect_resource_refs(converted: List[dict], facts: List[dict]) -> set[str]:
    """Collect reference values from converted rows or fallback to facts."""
    refs = set()
    if converted:
        for row in converted:
            if r := _ref_from_conv(row):
                refs.add(r)
    elif facts:
        for fact in facts:
            if (fact.get("field") or "").lower() == "reference":
                ref = str(fact.get("value", "")).strip()
                if ref:
                    refs.add(ref)
    return refs


def _categorize_entities(
    resource_refs: set[str],
    existing_by_ref: dict,
    new_by_ref: dict,
    params_dict: dict,
) -> tuple[list, list]:
    """Split resource refs into existing vs new entity breakdowns."""
    existing_entity_breakdown = []
    new_entity_breakdown = []

    if _is_special_case(params_dict):
        for ref in resource_refs:
            if ref in existing_by_ref:
                new_entity_breakdown.append(existing_by_ref[ref])
    else:
        for ref in resource_refs:
            if ref in new_by_ref:
                new_entity_breakdown.append(new_by_ref[ref])
            elif ref in existing_by_ref:
                existing_entity_breakdown.append(existing_by_ref[ref])

    return existing_entity_breakdown, new_entity_breakdown


def _is_special_case(params_dict: dict) -> bool:
    """Return True if the special-case override should apply."""
    return (
        params_dict.get("dataset") == "conservation-area"
        and params_dict.get("organisation") == "local-authority:MIK"
    )


def _persist_response_and_details(
    session, request_id, response_data, entity_summary_data, extracted_data
):
    """Persist the main response and its row-by-row details."""
    # Assemble the main data payload for the Response model
    data = {
        "column-field-log": response_data.get("column-field-log", {}),
        "error-summary": response_data.get("error-summary", {}),
        "entity-summary": entity_summary_data["summary"],
    }
    if response_data.get("endpoint_url_validation"):
        data["endpoint_url_validation"] = response_data.get("endpoint_url_validation")
    if response_data.get("source_validation"):
        data["source_validation"] = response_data.get("source_validation")
    if "column-mapping" in response_data:
        data["column-mapping"] = response_data.get("column-mapping")

    # Persist top-level Response
    resp_row = models.Response(request_id=request_id, data=data)
    session.add(resp_row)
    session.flush()  # need resp_row.id for details

    # Persist row details with is_new_entity flag
    new_entity_breakdown = entity_summary_data["new_entity_breakdown"]
    converted = extracted_data["converted"]
    issues = extracted_data["issues"]
    facts = extracted_data["facts"]

    for i, conv_row in enumerate(converted, start=1):
        entry_number_str = str(i)
        row_issues = [
            it for it in issues if str(it.get("entry-number")) == entry_number_str
        ]
        row_facts = [
            ft for ft in facts if str(ft.get("entry-number")) == entry_number_str
        ]

        ref_val = _ref_from_conv(conv_row)
        is_new = any(
            ent["reference"] == ref_val and ent["entity"] == conv_row.get("entity")
            for ent in new_entity_breakdown
        )

        session.add(
            models.ResponseDetails(
                response_id=resp_row.id,
                detail={
                    "converted_row": conv_row,
                    "issue_logs": row_issues,
                    "entry_number": i,
                    "transformed_row": row_facts,
                    "is_new_entity": is_new,
                },
            )
        )

    session.commit()
