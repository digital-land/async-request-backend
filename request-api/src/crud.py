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


def get_response_geometries(db: Session, request_id: int):
    transformed_rows = (
        db.query(models.ResponseDetails.detail["transformed_row"])
        .join(models.ResponseDetails.response)
        .filter(models.Response.request_id == request_id)
        .order_by(models.ResponseDetails.id)
        .all()
    )

    geometry_key = None
    for (transformed_row,) in transformed_rows:
        if not transformed_row:
            continue
        geometry_field = next(
            (
                field
                for field in transformed_row
                if field.get("field") in ("geometry", "point")
            ),
            None,
        )
        if geometry_field is not None:
            geometry_key = geometry_field.get("field")
            break

    geometries = []
    if geometry_key is not None:
        for (transformed_row,) in transformed_rows:
            if not transformed_row:
                continue
            geometry = next(
                (
                    field.get("value")
                    for field in transformed_row
                    if field.get("field") == geometry_key
                    and isinstance(field.get("value"), str)
                    and field.get("value").strip()
                ),
                None,
            )
            if geometry:
                geometries.append(geometry)

    return geometries


def create_request(db: Session, request: schemas.RequestCreate):
    db_request = models.Request(
        status="NEW", type=request.params.type, params=request.params.model_dump()
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request
