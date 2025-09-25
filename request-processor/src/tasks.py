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
        if directories is None:
            directories = Directories()
        elif isinstance(directories, str):
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
            fileName = handle_check_file(request_schema, request_data, tmp_dir)

        elif request_data.type == "check_url":
            log, content = utils.get_request(request_data.url)
            if content:
                check = utils.check_content(content)
                if check:
                    fileName = utils.save_content(content, tmp_dir)
                else:
                    log = {}
                    log[
                        "message"
                    ] = "Endpoint URL includes multiple dataset layers. Endpoint URL must include a single dataset layer only."  # noqa
                    log["status"] = ""
                    log["exception_type"] = "URL check failed"
                    save_response_to_db(request_schema.id, log)
                    return
            else:
                save_response_to_db(request_schema.id, log)
                logger.warning(f"URL check failed: {log}")
                return

        if fileName:
            org = (getattr(request_data, "organisation", "") or "").strip()
            response = workflow.run_workflow(
                fileName,
                request_schema.id,
                request_data.collection,
                request_data.dataset,
                org,
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
        log = {}
        log["message"] = "The uploaded file not found in S3 bucket"
        log["status"] = ""
        log["exception_type"] = type(e).__name__
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
    """
    Persist the response.

    - On error payload (contains 'message' & 'exception_type'): store as Response.error.
    - On full payload: compute and store:
        * new-entities + new-entity-count
        * entity-summary (with new-entity-breakdown)
        * per-row is_new_entity in ResponseDetails
    - On partial payload: store whatever we have (debug aid).
    """
    db_session = database.session_maker()
    with db_session() as session:
        try:
            existing = _get_response(request_id)
            if existing:
                logger.exception("response already exists in DB for request: %s", request_id)
                return

            # Error payload?
            if "message" in response_data and "exception_type" in response_data:
                error = CustomException(response_data)
                session.add(models.Response(request_id=request_id, error=error.detail))
                session.commit()
                return

            # Validate required keys
            required = ("column-field-log", "error-summary", "converted-csv", "issue-log", "transformed-csv")
            if not all(k in response_data for k in required):
                session.add(models.Response(request_id=request_id, data=response_data))
                session.commit()
                return

            converted = response_data.get("converted-csv") or []
            issues = response_data.get("issue-log") or []
            facts = response_data.get("transformed-csv") or []

            # Index converted rows by entry number (1-based)
            by_entry_conv = {str(i): (row or {}) for i, row in enumerate(converted, start=1)}

            # Index transformed facts by entry number (string keys)
            by_entry_facts = {}
            for f in facts:
                en = str((f.get("entry-number") or "")).strip()
                if not en:
                    continue
                by_entry_facts.setdefault(en, []).append(f)

            # Pull organisation from original request
            req = _get_request(request_id)
            params = getattr(req, "params", None)
            try:
                params_dict = params.dict() if hasattr(params, "dict") else dict(params or {})
            except Exception:
                params_dict = {}
            org_for_entities = (params_dict.get("organisation") or "").strip()

            # Helpers
            def _fact_value(entry_no: str, field_name: str) -> str:
                aliases = {
                    "reference": {"reference", "entity reference", "entity-reference", "ref"},
                    "reference-entity": {"reference-entity", "entity-reference"},
                    "prefix": {"prefix"},
                }
                wanted = aliases.get(field_name.lower(), {field_name.lower()})
                vals = [
                    r.get("value")
                    for r in by_entry_facts.get(entry_no, [])
                    if (r.get("field") or "").lower().replace("_", "-") in wanted
                ]
                return str(vals[0]).strip() if vals else ""

            def _ref_from_conv(row: dict) -> str:
                if not row:
                    return ""
                for key in (
                    "Reference", "reference", "Entity reference", "entity_reference",
                    "entity-reference", "ListEntry", "ref"
                ):
                    v = row.get(key)
                    if v not in (None, ""):
                        return str(v).strip()
                for k, v in row.items():
                    if "ref" in (k or "").lower() and v not in (None, ""):
                        return str(v).strip()
                return ""

            # 1) Determine "new" from explicit issues (unknown entity)
            new_refs = set()
            new_entities = []
            for iss in issues:
                en = str((iss.get("entry-number") or "")).strip()
                itype_raw = str(iss.get("issue-type") or iss.get("type") or "").lower()
                itype = itype_raw.replace("-", " ")  # allow "unknown-entity"
                if not en or "unknown entity" not in itype:
                    continue

                ref = (
                    _fact_value(en, "reference")
                    or _ref_from_conv(by_entry_conv.get(en, {}))
                    or str(iss.get("reference", "")).strip()
                )
                if not ref or ref in new_refs:
                    continue
                prefix = _fact_value(en, "prefix")
                new_entities.append({
                    "reference": ref,
                    "prefix": prefix,
                    "organisation": org_for_entities,
                })
                new_refs.add(ref)

            # 2) Fallback: reference present but reference-entity empty
            if by_entry_facts:
                max_en = max([int(k) for k in by_entry_facts.keys()] or [0])
            else:
                max_en = 0
            for i in range(1, max_en + 1):
                en = str(i)
                ref = _fact_value(en, "reference")
                ref_entity = _fact_value(en, "reference-entity")
                if ref and not ref_entity and ref not in new_refs:
                    prefix = _fact_value(en, "prefix")
                    new_entities.append({
                        "reference": ref,
                        "prefix": prefix,
                        "organisation": org_for_entities,
                    })
                    new_refs.add(ref)

            # --- Entity summary (for FE) ---
            breakdown = []
            seen_breakdown = set()
            for iss in issues:
                itype_raw = str(iss.get("issue-type") or iss.get("type") or "").lower()
                itype = itype_raw.replace("-", " ")
                if "unknown entity" not in itype:
                    continue
                en = str((iss.get("entry-number") or "")).strip()
                if not en:
                    continue
                ref = (
                    _fact_value(en, "reference")
                    or _ref_from_conv(by_entry_conv.get(en, {}))
                    or str(iss.get("reference", "")).strip()
                )
                if not ref or ref in seen_breakdown:
                    continue
                breakdown.append({"reference": ref, "line-number": en})
                seen_breakdown.add(ref)

            for ref in sorted(new_refs):
                if ref not in seen_breakdown:
                    breakdown.append({"reference": ref})

            entity_summary = {
                "existing-in-resource": 0,  # not computed here
                "new-in-resource": len(new_refs),
                "new-entity-breakdown": breakdown,
            }

            # Build top-level Response.data
            data = {
                "column-field-log": response_data.get("column-field-log", {}),
                "error-summary": response_data.get("error-summary", {}),
                "new-entities": new_entities,
                "new-entity-count": len(new_entities),
                "entity-summary": entity_summary,
            }

            resp_row = models.Response(request_id=request_id, data=data)
            session.add(resp_row)
            session.flush()  # need resp_row.id for details

            # Persist row details with is_new_entity
            entry_number = 1
            for conv in converted:
                en = str(entry_number)
                row_issues = [it for it in issues if str(it.get("entry-number")) == en]
                row_facts = [ft for ft in facts if str(ft.get("entry-number")) == en]

                ref_val = _ref_from_conv(conv)
                is_new = ref_val in new_refs

                session.add(models.ResponseDetails(
                    response_id=resp_row.id,
                    detail={
                        "converted_row": conv,
                        "issue_logs": row_issues,
                        "entry_number": entry_number,
                        "transformed_row": row_facts,
                        "is_new_entity": is_new,
                    },
                ))
                entry_number += 1

            session.commit()

        except Exception as e:
            session.rollback()
            raise e
