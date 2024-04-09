import logging
from datetime import datetime

from fastapi import FastAPI, Depends, Request, Response, HTTPException
from sqlalchemy.orm import Session

import crud
from database import session_maker
from request_model import models, schemas
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


@app.get("/")
def read_root():
    return {"msg": "Hello World"}


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
            details=response_details,
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

