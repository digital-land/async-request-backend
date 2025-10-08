import os
import json
import shutil
from pathlib import Path
from typing import Dict, List, Optional

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


def _get_content_from_url(url: str, request_id: str, tmp_dir: str) -> Optional[str]:
    """Fetch content from a URL, save it, and return the filename."""
    log, content = utils.get_request(url)
    if content:
        if utils.check_content(content):
            return utils.save_content(content, tmp_dir)
        else:
            log = {
                "message": "Endpoint URL includes multiple dataset layers. Endpoint URL must include a single dataset layer only.",
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


@celery.task(base=CheckDataFileTask, name=CheckDataFileTask.name)
def check_datafile(request: Dict, directories=None):
    logger.info("check datafile")
    logger.info("Received payload:")
    logger.info(json.dumps(request, indent=2, default=str))

    request_schema = schemas.Request.model_validate(request)
    request_data = request_schema.params

    if request_schema.status == "COMPLETE":
        return _get_request(request_schema.id)

    if directories is None:
        directories = Directories()
    elif isinstance(directories, str):
        data_dict = json.loads(directories)
        directories = Directories()
        for key, value in data_dict.items():
            setattr(directories, key, value)

    fileName = ""
    tmp_dir = os.path.join(
        directories.COLLECTION_DIR, "resource", f"{request_schema.id}"
    )
    Path(tmp_dir).mkdir(parents=True, exist_ok=True)

    if request_data.type == "check_file":
        fileName = handle_check_file(request_schema, request_data, tmp_dir)

    elif request_data.type == "check_url":
        logger.info("== Incoming config for check_url ==")
        _log_incoming_config(request_data)
        fileName = _get_content_from_url(request_data.url, request_schema.id, tmp_dir)

    elif request_data.type == "add_data":
        logger.info("== Incoming config for add_data ==")
        _log_incoming_config(request_data)

        preview_url = getattr(request_data, "url", None)
        if preview_url:
            fileName = _get_content_from_url(preview_url, request_schema.id, tmp_dir)
        elif (
            hasattr(request_data, "source_request_id")
            and request_data.source_request_id
        ):
            logger.info(
                f"Processing add_data preview for source request {request_data.source_request_id}"
            )
            original_response = _get_response(request_data.source_request_id)
            if original_response and original_response.data:
                response = workflow.run_preview_workflow(
                    request_schema.id, request_data, directories, original_response.data
                )
                save_response_to_db(request_schema.id, response)
                return _get_request(request_schema.id)
        elif hasattr(request_data, "content") and request_data.content:
            content = request_data.content.encode("utf-8")
            fileName = utils.save_content(content, tmp_dir)

    # If no file could be processed (e.g. URL check failed, or it was a preview), return early
    if not fileName:
        logger.info("No file to process, returning.")
        return _get_request(request_schema.id)

    # Run the workflow and persist response
    try:
        org = (getattr(request_data, "organisation", "") or "").strip()
        response = workflow.run_workflow(
            fileName,
            request_schema.id,
            request_data.collection,
            request_data.dataset,
            org,
            directories,
            request_data=request_data,
        )
        save_response_to_db(request_schema.id, response)
    except Exception as e:
        err_log = {
            "message": f"Processing failed: {e}",
            "status": "",
            "exception_type": type(e).__name__,
        }
        logger.exception("Processing exception")
        try:
            save_response_to_db(request_schema.id, err_log)
        finally:
            raise

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


# -----------------------------
# Celery hooks
# -----------------------------
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

            # Error payload?
            if "message" in response_data and "exception_type" in response_data:
                error = CustomException(response_data)
                session.add(models.Response(request_id=request_id, error=error.detail))
                session.commit()
                return

            # Validate presence of all sections we expect from workflow
            required = (
                "column-field-log",
                "error-summary",
                "converted-csv",
                "issue-log",
                "transformed-csv",
            )
            if not all(k in response_data for k in required):
                # Store whatever we have to aid debugging
                session.add(models.Response(request_id=request_id, data=response_data))
                session.commit()
                return

            converted: List[dict] = response_data.get("converted-csv") or []
            issues: List[dict] = response_data.get("issue-log") or []
            facts: List[dict] = response_data.get("transformed-csv") or []

            # existing_entities_raw = response_data.get("existing-entities", [])
            # existing_entities = [{"reference": str(ref)} for ref in existing_entities_raw]
            existing_entities = response_data.get("existing-entities", [])
            existing_set = set((e["reference"], e["entity"]) for e in existing_entities)

            resource_entities = []
            for row in facts if facts else converted:
                ref = row.get("reference")
                ent = row.get("entity")
                if ref and ent:
                    resource_entities.append({"reference": ref, "entity": ent})

            by_entry_conv = {
                str(i): row or {} for i, row in enumerate(converted, start=1)
            }
            by_entry_facts: dict[str, List[dict]] = {}
            for f in facts:
                en = str(f.get("entry-number", "")).strip()
                if not en:
                    continue
                by_entry_facts.setdefault(en, []).append(f)

            # Pull organisation from original request (to attach to preview)
            req = _get_request(request_id)
            params = getattr(req, "params", None)
            try:
                params_dict = (
                    params.dict() if hasattr(params, "dict") else dict(params or {})
                )
            except Exception:
                params_dict = {}
            org_for_entities = params_dict.get("organisation", "") or ""

            def _facts_for_entry(entry_no: str) -> List[dict]:
                """Helper to get all fact rows for a given entry number."""
                return by_entry_facts.get(str(entry_no), [])

            def _fact_rows_for_field(entry_no: str, field_name: str) -> List[dict]:
                """Helper to get fact rows for a specific field within an entry."""
                return [
                    f
                    for f in _facts_for_entry(entry_no)
                    if (f.get("field") or "").lower() == field_name.lower()
                ]

            # Helpers to read values from transformed facts for an entry
            def _fact_value(entry_no: str, field_name: str) -> str:
                vals = [
                    r.get("value")
                    for r in by_entry_facts.get(entry_no, [])
                    if (r.get("field") or "").lower() == field_name.lower()
                ]
                return str(vals[0]).strip() if vals else ""

            def _ref_from_conv(row: dict) -> str:
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

            # # 1) Determine new entities (prefer explicit "unknown entity" issues)
            # new_refs = set()
            # new_entities = []
            # for iss in issues:
            #     en = str(iss.get("entry-number", "")).strip()
            #     itype = str(iss.get("issue-type", iss.get("type", ""))).lower()
            #     if not en or "unknown entity" not in itype:
            #         continue
            #     ref = (
            #         _fact_value(en, "reference")
            #         or _ref_from_conv(by_entry_conv.get(en, {}))
            #         or str(iss.get("reference", "")).strip()
            #     )

            #     entity_num = _fact_value(en, "reference-entity")
            #     if not ref or (ref, entity_num) in existing_set:
            #     # if not ref or any(
            #     #     ref == e.get("reference") and entity_num == e.get("entity")
            #     #     for e in existing_entities
            #     # ):
            #         continue
            #     prefix = _fact_value(en, "prefix")
            #     new_entities.append({
            #         "reference": ref,
            #         "entity": entity_num,
            #         "prefix": prefix,
            #         "organisation": org_for_entities,
            #     })
            #     # new_refs.add(ref)
            #     # Remove any new_entities that are also in existing_entities (just in case)
            #     new_entities = [e for e in new_entities if (e["reference"], e["entity"]) not in existing_set]

            # # 2) Fallback: reference present but reference-entity empty
            # if by_entry_facts:
            #     max_en = max([int(k) for k in by_entry_facts.keys()] or [0])
            # else:
            #     max_en = 0
            # for i in range(1, max_en + 1):
            #     en = str(i)
            #     ref = _fact_value(en, "reference")
            #     ref_entity = _fact_value(en, "reference-entity")
            #     if ref and not ref_entity and ref not in new_refs:
            #         prefix = _fact_value(en, "prefix")
            #         new_entities.append({
            #             "reference": ref,
            #             "prefix": prefix,
            #             "organisation": org_for_entities,
            #         })
            #         new_refs.add(ref)

            # # --- CLI-like entity summary for the FE (CHECK stage) ---
            # breakdown = [
            #     {"reference": e["reference"], "entity": e.get("entity")}
            #     for e in new_entities
            # ]
            # seen_refs_in_breakdown = set()
            # for iss in issues:
            #     itype = str(iss.get("issue-type") or iss.get("type") or "").lower()
            #     if "unknown entity" not in itype:
            #         continue
            #     en = str(iss.get("entry-number", "")).strip()
            #     if not en:
            #         continue
            #     ref = (
            #         _fact_value(en, "reference")
            #         or _ref_from_conv(by_entry_conv.get(en, {}))
            #         or str(iss.get("reference", "")).strip()
            #     )
            #     if not ref or ref in seen_refs_in_breakdown:
            #         continue
            #     breakdown.append({"reference": ref, "line-number": en})
            #     seen_refs_in_breakdown.add(ref)

            # # Add any remaining new refs (without line number) to keep totals consistent
            # for ref in sorted(new_refs):
            #     if ref not in seen_refs_in_breakdown:
            # #         breakdown.append({"reference": ref})

            # entity_summary = {
            #     "existing-in-resource": len(existing_entities),
            #     "new-in-resource": len(new_entities),
            #     "existing-entity-breakdown": existing_entities,
            #     "new-entity-breakdown": new_entities,
            # }

            # # Build top-level response data block
            # data = {
            #     "column-field-log": response_data.get("column-field-log", {}),
            #     "error-summary": response_data.get("error-summary", {}),
            #     "new-entities": new_entities,
            #     "new-entity-count": len(new_entities),
            #     "entity-summary": entity_summary,
            #     "existing-entities": existing_entities,
            # }
            # --- Partition resource entities into new and existing for summary ---
            # --- Existing entities from workflow (list of {"reference","entity"}) ---
            existing_entities = response_data.get("existing-entities", []) or []

            # Index existing by reference (for presence checks)
            existing_by_ref = {}
            for e in existing_entities:
                try:
                    ref = str((e or {}).get("reference", "")).strip()
                    ent = str((e or {}).get("entity", "")).strip()
                    if ref and ref not in existing_by_ref:
                        existing_by_ref[ref] = {"reference": ref, "entity": ent}
                except Exception:
                    continue

            # --- References present in this resource (from converted rows) ---
            resource_refs = set()
            if by_entry_conv:
                for en, conv in by_entry_conv.items():
                    r = _ref_from_conv(conv)
                    if r:
                        resource_refs.add(r)
            # Fallback to transformed facts if converted is empty (e.g., for WFS on add_data preview)
            elif by_entry_facts:
                for entry_no in by_entry_facts:
                    ref_facts = _fact_rows_for_field(entry_no, "reference")
                    if ref_facts:
                        resource_refs.add(str(ref_facts[0].get("value", "")).strip())

            # --- Existing-in-resource = existing refs that actually appear in this upload ---
            existing_entity_breakdown = [
                existing_by_ref[r] for r in resource_refs if r in existing_by_ref
            ]

            # We are not deriving "new" right now (you commented that logic out)
            new_entity_breakdown = []

            # Optional: log for sanity
            logger.info(
                "Entity counts — existing(list): %d, existing(in resource by ref): %d, new(in resource): %d",
                len(existing_entities),
                len(existing_entity_breakdown),
                len(new_entity_breakdown),
            )

            entity_summary = {
                "existing-in-resource": len(existing_entity_breakdown),
                "new-in-resource": len(new_entity_breakdown),
                "existing-entity-breakdown": existing_entity_breakdown,
                "new-entity-breakdown": new_entity_breakdown,
            }

            data = {
                "column-field-log": response_data.get("column-field-log", {}),
                "error-summary": response_data.get("error-summary", {}),
                "new-entities": new_entity_breakdown,  # empty for now
                "new-entity-count": len(new_entity_breakdown),
                "entity-summary": entity_summary,
                "existing-entities": existing_entities,  # full list from lookup.csv
            }
            if response_data.get("endpoint_url_validation"):
                data["endpoint_url_validation"] = response_data.get(
                    "endpoint_url_validation"
                )

            if "column-mapping" in response_data:
                data["column-mapping"] = response_data.get("column-mapping")

            # Persist top-level Response
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
                is_new = any(
                    ent["reference"] == ref_val and ent["entity"] == conv.get("entity")
                    for ent in new_entity_breakdown
                )
                session.add(
                    models.ResponseDetails(
                        response_id=resp_row.id,
                        detail={
                            "converted_row": conv,
                            "issue_logs": row_issues,
                            "entry_number": entry_number,
                            "transformed_row": row_facts,
                            "is_new_entity": is_new,
                        },
                    )
                )
                entry_number += 1

            session.commit()

        except Exception as e:
            session.rollback()
            raise e