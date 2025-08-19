import logging
import os
from datetime import datetime
from typing import List, Dict, Any
import sentry_sdk

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from fastapi import FastAPI, Depends, Request, Response, HTTPException
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from slack_sdk import WebClient
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
    )

app = FastAPI()


def send_slack_alert(message):
    slack_token = os.environ.get("SLACK_BOT_TOKEN", "")
    slack_channel = os.environ.get("SLACK_CHANNEL", "")
    if not slack_token or not slack_channel:
        logging.warning("Slack token or channel is missing.")
        return
    client = WebClient(token=slack_token)
    bot_name = "SQS and DB"
    client.chat_postMessage(channel=slack_channel, text=message, username=bot_name)


def is_connection_restored(last_attempt_time, max_retry_duration=60):
    current_time = datetime.now().timestamp()
    last_attempt_timestamp = last_attempt_time.timestamp()
    return (current_time - last_attempt_timestamp) > max_retry_duration


def _get_db():
    retries = 5
    for attempt in range(retries):
        try:
            db = session_maker()()
            try:
                yield db
            finally:
                db.close()
            return
        except SQLAlchemyError as e:
            logging.exception(f"Database connection failed (Attempt {attempt+1}): {e}")
            if is_connection_restored(datetime.now()):
                break
    send_slack_alert("DB connection issue detected in async-request-backend..")


def _get_sqs_client():
    retries = 5
    for attempt in range(retries):
        try:
            logging.info(f"SQS client successfully connected on attempt {attempt}.")
            return boto3.client("sqs")
        except (ClientError, BotoCoreError) as e:
            logging.exception(
                f"SQS connection failed on attempt {attempt}. Retrying...Error: {e}"
            )
            if is_connection_restored(datetime.now()):
                break
    send_slack_alert("SQS connection issue detected in async-request-backend..")


@app.get("/health", response_model=HealthCheckResponse)
def healthcheck(
    response: Response, db: Session = Depends(_get_db), sqs=Depends(_get_sqs_client)
):
    try:
        db_result = db.execute(text("SELECT 1"))
        db_reachable = len(db_result.all()) == 1
    except SQLAlchemyError:
        logging.exception("Health check of request-db failed")
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
        version=os.environ.get("GIT_COMMIT", "unknown"),
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
def create_request(
    request: schemas.RequestCreate,
    http_request: Request,
    http_response: Response,
    db: Session = Depends(_get_db),
):
    request_model = crud.create_request(db, request)
    request_schema = _map_to_schema(request_model)

    try:
        CheckDataFileTask.delay(request_schema.model_dump(mode="json"))
    except Exception as error:
        logging.error("Async call to celery check data file task failed: %s", error)
        raise error

    http_response.headers[
        "Location"
    ] = f"${http_request.headers.get('host')}/requests/{request_schema.id}"
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
                "errMsg": f"Response with {request_id} was not found",
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
