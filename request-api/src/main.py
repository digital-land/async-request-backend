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


@app.get("/requests/{request_id}", response_model=schemas.Request)
def read_request(request_id: str, db: Session = Depends(get_db)):
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


def _map_to_schema(request_model: models.Request) -> schemas.Request:
    response = None
    if request_model.response:
        response_details = None
        if request_model.response.details:
            response_details = []
            for detail in request_model.response.details:
                response_details.append(detail.detail)

        response_error = None
        if request_model.response.details:
            for detail in request_model.response.details:
                response_details.append(detail.detail)

        response = schemas.ResponseModel(
            data=request_model.response.data,
            details=response_details,
            error=response_error
        )

    return schemas.Request(
        type=request_model.type,
        id=request_model.id,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        params=request_model.params,
        response=response
    )

