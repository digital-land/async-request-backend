"""HTTP endpoints for Add Data Submissions."""

from fastapi import APIRouter, Depends, HTTPException, Request, Response
from sqlalchemy.orm import Session

import add_data_submissions
from auth import require_auth
from add_data_submission_schemas import (
    AddDataSubmissionCreate,
    AddDataSubmissionRead,
    AddDataSubmissionReceipt,
    AddDataSubmissionEvent,
    AddDataSubmissionFilters,
    AddDataSubmissionList,
    AddDataSubmissionPatch,
    AddDataSubmissionTransition,
)
from dependencies import _get_db

router = APIRouter(prefix="/add-data-submissions")


@router.post("", status_code=201, response_model=AddDataSubmissionReceipt)
def create_add_data_submission(
    request: AddDataSubmissionCreate,
    response: Response,
    db: Session = Depends(_get_db),
):
    add_data_submission, created = add_data_submissions.create_add_data_submission(
        db, request
    )
    response.status_code = 201 if created else 200
    response.headers["Location"] = f"/add-data-submissions/{add_data_submission.id}"
    return add_data_submission


@router.get("/by-reference/{reference}", response_model=AddDataSubmissionRead)
@require_auth(roles={"staff", "admin"})
def read_add_data_submission_by_reference(
    reference: str, http_request: Request, db: Session = Depends(_get_db)
):
    return add_data_submissions.get_by_reference(db, reference)


@router.get("", response_model=AddDataSubmissionList)
@require_auth(roles={"staff", "admin"})
def list_add_data_submissions(
    http_request: Request,
    filters: AddDataSubmissionFilters = Depends(),
    db: Session = Depends(_get_db),
):
    return add_data_submissions.list_add_data_submissions(db, filters)


@router.post(
    "/{add_data_submission_id}/transitions", response_model=AddDataSubmissionRead
)
@require_auth(roles={"staff", "admin"})
def transition_add_data_submission(
    add_data_submission_id: str,
    request: AddDataSubmissionTransition,
    http_request: Request,
    db: Session = Depends(_get_db),
):
    principal = getattr(http_request.state, "principal", {})
    actor = principal.get("username")
    if not isinstance(actor, str) or not actor.strip():
        raise HTTPException(
            403, "A verified GitHub username is required for transitions"
        )
    return add_data_submissions.transition_add_data_submission(
        db, add_data_submission_id, request, actor
    )


@router.patch("/{add_data_submission_id}", response_model=AddDataSubmissionRead)
@require_auth(roles={"staff", "admin"})
def patch_add_data_submission(
    add_data_submission_id: str,
    request: AddDataSubmissionPatch,
    http_request: Request,
    db: Session = Depends(_get_db),
):
    return add_data_submissions.patch_add_data_submission(
        db, add_data_submission_id, request
    )


@router.get(
    "/{add_data_submission_id}/events", response_model=list[AddDataSubmissionEvent]
)
@require_auth(roles={"staff", "admin"})
def read_add_data_submission_events(
    add_data_submission_id: str, http_request: Request, db: Session = Depends(_get_db)
):
    return add_data_submissions.get_events(db, add_data_submission_id)


@router.get("/{add_data_submission_id}", response_model=AddDataSubmissionRead)
@require_auth(roles={"staff", "admin"})
def read_add_data_submission(
    add_data_submission_id: str, http_request: Request, db: Session = Depends(_get_db)
):
    return add_data_submissions.get_add_data_submission(db, add_data_submission_id)
