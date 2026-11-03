"""Add Data Submission persistence; personal data never enters processing requests."""

import secrets

from fastapi import HTTPException
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session, selectinload

from request_model import models
from add_data_submission_schemas import (
    AddDataSubmissionCreate,
    AddDataSubmissionFilters,
    AddDataSubmissionPatch,
    AddDataSubmissionTransition,
)
from pagination_model import PaginationParams

TERMINAL_STATUSES = ("ADDED", "CLOSED")

TRANSITIONS = {
    "TO_BE_ADDED": {"IN_PROGRESS", "QUERIED", "CLOSED"},
    "IN_PROGRESS": {"TO_BE_ADDED", "IN_PROGRESS", "QUERIED", "ADDED", "CLOSED"},
    "QUERIED": {"TO_BE_ADDED", "IN_PROGRESS", "CLOSED"},
    "ADDED": set(),
    "CLOSED": set(),
}


def _lock_for_update(db: Session, submission_id: str, expected_version: int):
    submission = db.scalar(
        select(models.AddDataSubmission)
        .where(models.AddDataSubmission.id == submission_id)
        .with_for_update()
    )
    if submission is None:
        raise HTTPException(404, "Add Data Submission not found")
    if submission.version != expected_version:
        raise HTTPException(409, "Submission has changed; reload it before retrying")
    return submission


def transition_add_data_submission(
    db: Session, submission_id: str, body: AddDataSubmissionTransition, actor: str
):
    submission = _lock_for_update(db, submission_id, body.expected_version)
    if body.to not in TRANSITIONS[submission.status]:
        raise HTTPException(
            409, f"Cannot transition from {submission.status} to {body.to.value}"
        )
    if body.to == "QUERIED" and not submission.contact_email:
        raise HTTPException(422, "QUERIED requires a contact email")
    if body.related_add_data_submission_id:
        if (
            body.related_add_data_submission_id == submission.id
            or db.get(models.AddDataSubmission, body.related_add_data_submission_id)
            is None
        ):
            raise HTTPException(
                422, "Related submission must exist and differ from this submission"
            )

    previous_status = submission.status
    if body.to == "IN_PROGRESS":
        submission.assignee = body.assignee
    elif body.to == "TO_BE_ADDED":
        submission.assignee = None
    submission.status = body.to.value
    submission.version += 1
    submission.status_updated_at = func.clock_timestamp()
    submission.closed_reason = body.closed_reason
    submission.related_add_data_submission_id = body.related_add_data_submission_id
    db.add(
        models.AddDataSubmissionEvent(
            add_data_submission_id=submission.id,
            event_type="transition",
            from_status=previous_status,
            to_status=submission.status,
            actor=actor,
            assignee=submission.assignee,
            note=body.note,
            closed_reason=body.closed_reason,
            related_add_data_submission_id=body.related_add_data_submission_id,
        )
    )
    db.commit()
    db.refresh(submission)
    return submission


def patch_add_data_submission(
    db: Session, submission_id: str, body: AddDataSubmissionPatch
):
    submission = _lock_for_update(db, submission_id, body.expected_version)
    for name, value in body.model_dump(
        exclude_unset=True, exclude={"expected_version"}
    ).items():
        setattr(submission, name, value)
    submission.version += 1
    db.commit()
    db.refresh(submission)
    return submission


def list_add_data_submissions(db: Session, filters: AddDataSubmissionFilters):
    query = select(models.AddDataSubmission)
    for name in ("status", "assignee", "organisation", "dataset", "source"):
        value = getattr(filters, name)
        if value is not None:
            query = query.where(getattr(models.AddDataSubmission, name) == value)
    if filters.reference is not None:
        query = query.where(
            models.AddDataSubmission.reference.contains(
                filters.reference.upper(), autoescape=True
            )
        )
    order = models.AddDataSubmission.submitted_at
    if filters.sort == "-submitted_at":
        order = order.desc()
    # Use a fresh session: changing isolation on an already-started transaction
    # is too late. Keep the count, page and eager-loaded events in one snapshot
    # without committing or rolling back any caller-owned transaction.
    bind = db.get_bind().engine.execution_options(isolation_level="REPEATABLE READ")
    with Session(bind) as snapshot:
        total = snapshot.scalar(select(func.count()).select_from(query.subquery()))
        rows = snapshot.scalars(
            query.options(selectinload(models.AddDataSubmission.events))
            .order_by(order, models.AddDataSubmission.id)
            .offset(filters.offset)
            .limit(filters.limit)
        ).all()
    return {
        "params": PaginationParams(offset=filters.offset, limit=filters.limit),
        "total_results_available": total,
        "data": rows,
    }


def get_events(db: Session, submission_id: str):
    return get_add_data_submission(db, submission_id).events


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
