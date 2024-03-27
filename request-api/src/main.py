import os
from functools import cache

import boto3

from botocore.exceptions import ClientError
from fastapi import FastAPI, Depends, Request, Response, HTTPException
from sqlalchemy.orm import Session
from datetime import datetime

import crud
from request_model import models, schemas
from database import session_maker
from task_interface.check_tasks import celery, CheckDataFileTask


CheckDataFileTask = celery.register_task(CheckDataFileTask())

use_celery = bool(os.environ.get("USE_CELERY"))


app = FastAPI()


@cache
def queue():
    sqs = boto3.resource("sqs")
    return sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])


# Dependency
def get_db():
    db = session_maker()()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def read_root():
    return {"msg": "Hello World"}


@app.post("/requests", status_code=202, response_model=schemas.Request)
def create_request(
    request: schemas.RequestCreate,
    http_request: Request,
    http_response: Response,
    db: Session = Depends(get_db),
):
    request_schema = _map_to_schema(request_model=crud.create_request(db, request))

    try:
        if use_celery:
            CheckDataFileTask.delay(request_schema.model_dump())
        else:
            queue().send_message(
                MessageBody=request_schema.model_dump_json(), MessageAttributes={}
            )

    except ClientError as error:
        print("Send message failed: %s", request_schema)
        raise error

    http_response.headers[
        "Location"
    ] = f"${http_request.headers['Host']}/requests/{request_schema.id}"

    return request_schema


@app.get("/requests/{request_id}")
def read_request(request_id: str, db: Session = Depends(get_db)):
    response_model = crud.get_response(db, request_id)
    if response_model is None:
        raise HTTPException(
            status_code=404,
            detail={
                "errCode": 400,
                "errType": "User Error",
                "errMsg": f"Response with ${request_id} was not found",
                "errTime": str(datetime.now()),
            },
        )
    response_schemas = _map_to_response_schema(response_model)
    return response_schemas


def _map_to_schema(request_model: models.Request) -> schemas.Request:
    return schemas.Request(
        type=request_model.type,
        id=request_model.id,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        params=request_model.params,
    )


def _map_to_response_schema(response_model: models.Response) -> schemas.Response:
    details_data = []
    if response_model.details:
        for detail in response_model.details:
            details_data.append(detail.detail)

    return schemas.Response(
        id=response_model.id,
        request_id=response_model.request_id,
        type=response_model.request.type,
        status=response_model.request.status,
        created=response_model.request.created,
        modified=response_model.request.modified,
        request=response_model.request.params,
        response={
            "data": {
                "error_summary": response_model.data["error-summary"],
                "column_field_log": response_model.data["column-field-log"],
            },
            "details": details_data,
            "error": response_model.error,
        },
    )
