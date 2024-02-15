import os
import boto3
from botocore.exceptions import ClientError
from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

import crud
from request_model import models
from request_model import schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])

app = FastAPI()


# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@app.get("/")
def read_root():
    return {"Hello": "World"}


@app.get("/requests", response_model=list[schemas.Request])
def read_requests(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    requests = crud.get_requests(db, skip=skip, limit=limit)
    return requests


@app.post("/requests", status_code=202, response_model=schemas.Request)
def create_request(request: schemas.RequestCreate, db: Session = Depends(get_db)):
    request = crud.create_request(db, request)
    response = schemas.Request.model_validate(request)
    try:
        queue.send_message(
            MessageBody=response.model_dump_json(), MessageAttributes={}
        )
    except ClientError as error:
        print("Send message failed: %s", request)
        raise error
    return response


@app.get("/request/{request_id}", response_model=schemas.Request)
def read_request(request_id: str, db: Session = Depends(get_db)):
    requests = crud.get_request(db, request_id)
    return requests
