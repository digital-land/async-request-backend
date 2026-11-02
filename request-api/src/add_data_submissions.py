"""Add Data Submission persistence; personal data never enters processing requests."""

import secrets

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from request_model import models
from add_data_submission_schemas import AddDataSubmissionCreate

TERMINAL_STATUSES = ("ADDED", "CLOSED")


def generate_reference():
    alphabet = "23456789ABCDEFGHJKMNPQRSTUVWXYZ"
    return "PDP-" + "".join(secrets.choice(alphabet) for _ in range(6))


def get_add_data_submission(db: Session, add_data_submission_id: str):
    add_data_submission = db.get(models.AddDataSubmission, add_data_submission_id)
    if add_data_submission is None:
        raise HTTPException(404, "Add Data Submission not found")
    return add_data_submission


def get_by_reference(db: Session, reference: str):
    add_data_submission = db.scalar(
        select(models.AddDataSubmission).where(
            models.AddDataSubmission.reference == reference.upper()
        )
    )
    if add_data_submission is None:
        raise HTTPException(404, "Add Data Submission not found")
    return add_data_submission


def _existing_origin(db, request_id, dataset):
    return db.scalar(
        select(models.AddDataSubmission).where(
            models.AddDataSubmission.origin_request_id == request_id,
            models.AddDataSubmission.dataset == dataset,
        )
    )


def _active_duplicate(db, endpoint_url, dataset, organisation):
    return db.scalar(
        select(models.AddDataSubmission).where(
            models.AddDataSubmission.endpoint_url == endpoint_url,
            models.AddDataSubmission.dataset == dataset,
            models.AddDataSubmission.organisation == organisation,
            models.AddDataSubmission.status.not_in(TERMINAL_STATUSES),
        )
    )


def _conflict(existing):
    raise HTTPException(
        409,
        {
            "message": "An active Add Data Submission already exists",
            "reference": existing.reference,
        },
    )


def _add_data_submission_values(db: Session, body: AddDataSubmissionCreate):
    origin = db.get(models.Request, body.check_request_id)
    if origin is None:
        raise HTTPException(404, "Check request not found")
    if origin.type != "check_url" or origin.status != "COMPLETE":
        raise HTTPException(422, "A completed URL check is required")
    params = origin.params or {}
    data = origin.response.data if origin.response else None
    detected = data.get("datasets-in-resource", []) if isinstance(data, dict) else []
    allowed = {params.get("dataset")}
    if isinstance(detected, list):
        allowed.update(
            value for value in detected if isinstance(value, str) and value.strip()
        )
    if body.dataset not in allowed:
        raise HTTPException(422, "Dataset was not checked or detected in this request")
    for key in ("organisationName", "url", "collection"):
        if not isinstance(params.get(key), str) or not params[key].strip():
            raise HTTPException(422, f"Check request is missing {key}")

    return dict(
        origin_request_id=origin.id,
        check_request_id=origin.id,
        organisation=params["organisationName"],
        dataset=body.dataset,
        collection=params["collection"],
        endpoint_url=params["url"],
        geom_type=params.get("geom_type"),
        documentation_url=body.documentation_url,
        contact_name=body.contact_name,
        contact_email=body.contact_email,
        source=body.source,
        jira_issue_key=body.jira_issue_key,
    )


def create_add_data_submission(db: Session, body: AddDataSubmissionCreate):
    existing = _existing_origin(db, body.check_request_id, body.dataset)
    if existing is not None:
        return existing, False
    values = _add_data_submission_values(db, body)
    existing = _active_duplicate(
        db, values["endpoint_url"], body.dataset, values["organisation"]
    )
    if existing is not None:
        _conflict(existing)

    for _ in range(10):
        add_data_submission = models.AddDataSubmission(
            **values, reference=generate_reference()
        )
        add_data_submission.events = [
            models.AddDataSubmissionEvent(event_type="created", to_status="TO_BE_ADDED")
        ]
        db.add(add_data_submission)
        try:
            db.commit()
        except IntegrityError as error:
            constraint = getattr(
                getattr(error.orig, "diag", None), "constraint_name", None
            )
            db.rollback()
            if constraint not in (
                "uq_add_data_submission_reference",
                "uq_add_data_submission_origin_dataset",
                "uq_add_data_submission_active_endpoint",
            ):
                raise
            # The winning transaction is now visible. Idempotency takes priority
            # even when PostgreSQL reports the active-endpoint constraint first.
            existing = _existing_origin(db, body.check_request_id, body.dataset)
            if existing is not None:
                return existing, False
            existing = _active_duplicate(
                db, values["endpoint_url"], body.dataset, values["organisation"]
            )
            if existing is not None:
                _conflict(existing)
            # A terminal transition may have released a competing endpoint;
            # retry in that case as well as for random reference collisions.
            continue
        db.refresh(add_data_submission)
        return add_data_submission, True
    raise HTTPException(
        503, "Could not allocate an Add Data Submission reference; please retry"
    )


def lock_for_request(db, request):
    add_data_submission = db.scalar(
        select(models.AddDataSubmission)
        .where(models.AddDataSubmission.id == request.add_data_submission_id)
        .with_for_update()
    )
    if add_data_submission is None:
        raise HTTPException(404, "Add Data Submission not found")
    if add_data_submission.status in TERMINAL_STATUSES:
        raise HTTPException(
            409, "Cannot attach requests to a terminal Add Data Submission"
        )
    params = request.params
    if params.type not in ("check_url", "add_data"):
        raise HTTPException(422, "Only URL checks and add-data previews can be linked")
    organisation = getattr(params, "organisation", None) or params.organisationName
    if (
        params.dataset != add_data_submission.dataset
        or params.url != add_data_submission.endpoint_url
        or organisation != add_data_submission.organisation
        or params.collection != add_data_submission.collection
        or (
            params.organisationName
            and params.organisationName != add_data_submission.organisation
        )
    ):
        raise HTTPException(
            422, "Request metadata does not match the Add Data Submission"
        )
    return add_data_submission
