import logging
import os
from datetime import datetime
from typing import List, Dict, Any

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from fastapi import FastAPI, Depends, Request, Response, HTTPException
from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import crud
from database import session_maker
from pagination_model import PaginationParams
from request_model import models, schemas
from schema import (
    ReadResponseDetailsParams,
    HealthCheckResponse,
    HealthStatus,
    DependencyHealth,
)
from task_interface.check_tasks import celery, CheckDataFileTask

CheckDataFileTask = celery.register_task(CheckDataFileTask())

app = FastAPI()


# Dependency
def _get_db():
    db = session_maker()()
    try:
        yield db
    finally:
        db.close()


def _get_sqs_client():
    return boto3.client("sqs")


@app.get("/health", response_model=HealthCheckResponse)
def healthcheck(response: Response, db: Session = Depends(_get_db), sqs=Depends(_get_sqs_client)):
    try:
        db_result = db.execute(text("SELECT 1"))
        db_reachable = len(db_result.all()) == 1
    except SQLAlchemyError:
        db_reachable = False

    try:
        sqs.get_queue_url(QueueName="celery")
        queue_reachable = True
    except (ClientError, BotoCoreError):
        queue_reachable = False

    response.status_code = 200 if db_reachable & queue_reachable else 500

    return HealthCheckResponse(
        name="request-api",
        version=os.environ.get("GIT_COMMIT", "unknown"),
        dependencies=[
            DependencyHealth(
                name="request-db",
                status=HealthStatus.HEALTHY if db_reachable else HealthStatus.UNHEALTHY,
            ),
            DependencyHealth(
                name="sqs",
                status=HealthStatus.HEALTHY
                if queue_reachable
                else HealthStatus.UNHEALTHY,
            ),
        ],
    )


@app.post("/requests", status_code=202, response_model=schemas.Request)
def create_request(
    request: schemas.RequestCreate,
    http_request: Request,
    http_response: Response,
    db: Session = Depends(_get_db),
):
    request_schema = _map_to_schema(request_model=crud.create_request(db, request))

    try:
        CheckDataFileTask.delay(request_schema.model_dump())

    except Exception as error:
        logging.error("Async call to celery check data file task failed: %s", error)
        raise error

    http_response.headers[
        "Location"
    ] = f"${http_request.headers['Host']}/requests/{request_schema.id}"

    return request_schema


@app.get("/requests/{request_id}", response_model=schemas.Request)
def read_request(request_id: str, db: Session = Depends(_get_db)):
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
def read_response_details(
    request_id: str,
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


def _map_to_schema(request_model: models.Request) -> schemas.Request:
    response = None
    if request_model.response:
        response_details = None
        if request_model.response.details:
            response_details = []
            for detail in request_model.response.details:
                response_details.append(detail.detail)
        response = schemas.ResponseModel(
            data=request_model.response.data,
            error=request_model.response.error,
        )

    return schemas.Request(
        type=request_model.type,
        id=request_model.id,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        params=request_model.params,
        response=response,
    )
