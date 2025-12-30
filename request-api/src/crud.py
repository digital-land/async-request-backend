import logging
from sqlalchemy import func
from sqlalchemy.orm import Session
from sqlalchemy.exc import ProgrammingError, DataError

from pagination_model import PaginatedResult, PaginationParams
from request_model import models
from request_model import schemas

logger = logging.getLogger(__name__)


def get_request(db: Session, request_id: int):
    return db.query(models.Request).filter(models.Request.id == request_id).first()


def get_response_details(
    db: Session,
    request_id: int,
    jsonpath: str = None,
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

    try:
        response_details = (
            base_query.offset(pagination_params.offset)
            .limit(pagination_params.limit)
            .all()
        )
        total_results = base_query.count()
    except (ProgrammingError, DataError) as e:
        # jsonb_path_math can raise errors if the jsonpath is invalid
        logger.warning("Invalid JSONPath expression '%s': %s", jsonpath, str(e))
        response_details = []
        total_results = 0

    return PaginatedResult(
        params=pagination_params,
        total_results_available=total_results,
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
