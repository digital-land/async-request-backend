import os
from functools import cache

import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, Depends, Request, Response, HTTPException
from sqlalchemy.orm import Session

import crud
from request_model import models, schemas
from database import SessionLocal, engine
from request_tasks import task

models.Base.metadata.create_all(bind=engine)

use_celery = bool(os.environ.get('USE_CELERY'))

app = FastAPI()


@cache
def queue():
    sqs = boto3.resource("sqs")
    return sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def read_root():
    return {"msg": "Hello World"}


@app.post("/requests", status_code=202, response_model=schemas.Request)
def create_request(request: schemas.RequestCreate, http_request: Request, http_response: Response, db: Session = Depends(get_db)):
    request_schema = _map_to_schema(
        request_model=crud.create_request(db, request)
    )

    try:
        if use_celery:
            task.check_datafile.delay(request_schema.model_dump())
        else:
            queue().send_message(
                MessageBody=request_schema.model_dump_json(), MessageAttributes={}
            )

    except ClientError as error:
        print("Send message failed: %s", request_schema)
        raise error

    http_response.headers['Location'] = f"${http_request.headers['Host']}/requests/{request_schema.id}"
    return request_schema


@app.get("/requests/{request_id}", response_model=schemas.Request)
def read_request(request_id: str, db: Session = Depends(get_db)):
    request_model = crud.get_request(db, request_id)
    if request_model is None:
        raise HTTPException(status_code=404, detail=f"Request with ${request_id} was not found")
    return _map_to_schema(request_model)


def _map_to_schema(request_model: models.Request) -> schemas.Request:
    return schemas.Request(
        id=request_model.id,
        user_email=request_model.user_email,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        data=request_model.data
    )
