import logging
import os
from datetime import datetime
from typing import List, Dict, Any
import sentry_sdk

from botocore.exceptions import ClientError, BotoCoreError
from fastapi import FastAPI, Depends, Request, Response, HTTPException
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import crud
from auth import add_data_requires_auth, authentication_disabled, require_auth
from dependencies import _get_db, _get_sqs_client
from routers.add_data_submissions import router as add_data_submissions_router
from pagination_model import PaginationParams
from sentry_privacy import SensitiveFieldScrubber
from request_model import models, schemas
from schema import (
    ReadResponseDetailsParams,
    HealthCheckResponse,
    HealthStatus,
    DependencyHealth,
)
from task_interface.base_tasks import (
    celery,
    CheckDataFileTask,
    CheckDataUrlTask,
    AddDataTask,
)

CheckDataFileTask = celery.register_task(CheckDataFileTask())
CheckDataUrlTask = celery.register_task(CheckDataUrlTask())
AddDataTask = celery.register_task(AddDataTask())

standard_queue = os.environ.get("CELERY_TASK_STANDARD_QUEUE", "celery")
high_queue = os.environ.get("CELERY_TASK_HIGH_QUEUE", "celery")


def _queue_for_service(service: str | None) -> str:
    return high_queue if service == "manage" else standard_queue


# Keep ordinary debugging context and Sentry's existing secret scrubbing, while
# filtering contact fields in request data, nested dictionaries and frame locals.
sentry_event_scrubber = SensitiveFieldScrubber(
    fields={"contact_name", "contact_email", "email"}
)


if os.environ.get("SENTRY_ENABLED", "false").lower() == "true":
    sentry_sdk.init(
        enable_tracing=os.environ.get("SENTRY_TRACING_ENABLED", "false").lower()
        == "true",
        traces_sample_rate=float(os.environ.get("SENTRY_TRACING_SAMPLE_RATE", "0.01")),
        release=os.environ.get("GIT_COMMIT"),
        integrations=[
            StarletteIntegration(transaction_style="url"),
            FastApiIntegration(transaction_style="url"),
        ],
        debug=os.environ.get("SENTRY_DEBUG", "false").lower() == "true",
        enable_logs=True,
        event_scrubber=sentry_event_scrubber,
        custom_repr=sentry_event_scrubber.safe_repr,
        before_send_log=sentry_event_scrubber.scrub_log,
    )

# Refuse to start with authentication switched off in staging or production.
authentication_disabled()

app = FastAPI(docs_url=None, redoc_url=None, openapi_url=None)
app.include_router(add_data_submissions_router)

SECURITY_REJECTED_METRIC = "async.security.rejected"
# Validation error types refused for safety reasons, and the layer they count as.
_SECURITY_ERROR_LAYERS = {
    schemas.UNSAFE_URL_ERROR_TYPE: "url",
    schemas.UNSAFE_UPLOADED_FILENAME_ERROR_TYPE: "upload_name",
}


@app.exception_handler(RequestValidationError)
async def count_security_rejections(request: Request, exc: RequestValidationError):
    """
    Count requests refused for safety reasons (an unsafe URL, or an upload name
    that is not an upload id), then return FastAPI's usual 422 response.
    Matches the worker's async.security.rejected metric; no URL, filename or
    request detail is recorded.
    """
    layers = {
        _SECURITY_ERROR_LAYERS[error.get("type")]
        for error in exc.errors()
        if error.get("type") in _SECURITY_ERROR_LAYERS
    }
    for layer in sorted(layers):
        sentry_sdk.metrics.count(
            SECURITY_REJECTED_METRIC,
            1,
            attributes={"service": "request-api", "layer": layer, "source": "api"},
        )
    return await request_validation_exception_handler(request, exc)


@app.get("/health", response_model=HealthCheckResponse)
def healthcheck(
    response: Response, db: Session = Depends(_get_db), sqs=Depends(_get_sqs_client)
):
    try:
        db_result = db.execute(text("SELECT 1"))
        db_reachable = len(db_result.all()) == 1
    except SQLAlchemyError:
        logging.exception(
            "Health check of request-db failed",
        )
        db_reachable = False

    try:
        sqs.get_queue_url(QueueName="celery")
        queue_reachable = True
    except (ClientError, BotoCoreError):
        logging.exception("Health check of sqs failed")
        queue_reachable = False

    response.status_code = 200 if db_reachable & queue_reachable else 500

    return HealthCheckResponse(
        name="request-api",
        dependencies=[
            DependencyHealth(
                name="request-db",
                status=HealthStatus.HEALTHY if db_reachable else HealthStatus.UNHEALTHY,
            ),
            DependencyHealth(
                name="sqs",
                status=(
                    HealthStatus.HEALTHY if queue_reachable else HealthStatus.UNHEALTHY
                ),
            ),
        ],
    )


@app.post("/requests", status_code=202, response_model=schemas.Request)
@require_auth(roles={"submit", "staff", "admin"})
@require_auth(roles={"staff", "admin"}, when=add_data_requires_auth)
def create_request(
    request: schemas.RequestCreate,
    http_request: Request,
    http_response: Response,
    db: Session = Depends(_get_db),
):
    request_schema = _map_to_schema(request_model=crud.create_request(db, request))
    queue = _queue_for_service(request_schema.params.service)

    try:
        if request_schema.type == "check_file":
            CheckDataFileTask.apply_async(
                args=[request_schema.model_dump()], queue=queue
            )
        elif request_schema.type == "check_url":
            CheckDataUrlTask.apply_async(
                args=[request_schema.model_dump()], queue=queue
            )
        elif request_schema.type == "add_data":
            AddDataTask.apply_async(args=[request_schema.model_dump()], queue=queue)
        else:
            raise ValueError("invalid request type")

    except Exception as error:
        logging.error("Async call to celery task failed: %s", error)
        raise error

    http_response.headers["Location"] = f"/requests/{request_schema.id}"

    return request_schema


@app.get("/requests/{request_id}", response_model=schemas.Request)
@require_auth(roles={"submit", "staff", "admin"})
def read_request(
    request_id: str, http_request: Request, db: Session = Depends(_get_db)
):
    request_model = crud.get_request(db, request_id)
    if request_model is None:
        raise HTTPException(
            status_code=404,
            detail={
                "errCode": 400,
                "errType": "User Error",
                "errMsg": f"Response with ${request_id} was not found",
                "errTime": str(datetime.now()),
            },
        )
    request_schema = _map_to_schema(request_model)
    return request_schema


@app.get("/requests/{request_id}/response-details", response_model=List[Dict[Any, Any]])
@require_auth(roles={"submit", "staff", "admin"})
def read_response_details(
    request_id: str,
    http_request: Request,
    http_response: Response,
    params: ReadResponseDetailsParams = Depends(),
    db: Session = Depends(_get_db),
):
    pagination_params = PaginationParams(offset=params.offset, limit=params.limit)
    paginated_result = crud.get_response_details(
        db, request_id, params.jsonpath, pagination_params
    )
    http_response.headers["X-Pagination-Total-Results"] = str(
        paginated_result.total_results_available
    )
    http_response.headers["X-Pagination-Offset"] = str(paginated_result.params.offset)
    http_response.headers["X-Pagination-Limit"] = str(paginated_result.params.limit)
    return list(map(lambda detail: detail.detail, paginated_result.data))


@app.get("/requests/{request_id}/geometries", response_model=List[str])
@require_auth(roles={"submit", "staff", "admin"})
def read_response_geometries(
    request_id: str, http_request: Request, db: Session = Depends(_get_db)
):
    return crud.get_response_geometries(db, request_id)


def _map_to_schema(request_model: models.Request) -> schemas.Request:
    response = None
    if request_model.response:
        response_details = None
        if request_model.response.details:
            response_details = []
            for detail in request_model.response.details:
                response_details.append(detail.detail)
        response = schemas.ResponseModel(
            data=request_model.response.data, error=request_model.response.error
        )

    return schemas.Request(
        add_data_submission_id=request_model.add_data_submission_id,
        type=request_model.type,
        id=request_model.id,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        published_at=request_model.published_at,
        params=request_model.params,
        response=response,
    )
