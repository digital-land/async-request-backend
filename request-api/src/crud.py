from typing import Optional
from sqlalchemy import func
from sqlalchemy.orm import Session

from pagination_model import PaginatedResult, PaginationParams
from request_model import models
from request_model import schemas


def get_request(db: Session, request_id: int):
    return db.query(models.Request).filter(models.Request.id == request_id).first()


def get_response_details(
    db: Session,
    request_id: int,
    jsonpath: str = None,
    issue_type: Optional[str] = None,
    field: Optional[str] = None,
    pagination_params=PaginationParams(),
):
    base_query = (
        db.query(models.ResponseDetails)
        .join(models.ResponseDetails.response)
        .filter(models.Response.request_id == request_id)
    )
    if jsonpath is not None:
        base_query = base_query.filter(
            func.jsonb_path_match(models.ResponseDetails.detail, jsonpath)
        )

    if issue_type is not None:
        base_query = base_query.filter(
            models.ResponseDetails.detail["issue_logs"].contains(
                [{"issue-type": issue_type}]
            )
        )

    if field is not None:
        base_query = base_query.filter(
            models.ResponseDetails.detail.contains({"issue_logs": [{"field": field}]})
        )

    response_details = (
        base_query.offset(pagination_params.offset).limit(pagination_params.limit).all()
    )
    return PaginatedResult(
        params=pagination_params,
        total_results_available=base_query.count(),
        data=response_details,
    )


def create_request(db: Session, request: schemas.RequestCreate):
    db_request = models.Request(
        status="NEW", type=request.params.type, params=request.params.model_dump()
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request
