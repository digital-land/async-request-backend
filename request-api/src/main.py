from typing import Union

from fastapi import FastAPI, Depends
from sqlalchemy.orm import Session

import models
import crud
import schemas
from database import SessionLocal, engine

models.Base.metadata.create_all(bind=engine)

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


@app.get("/requests")
def read_requests(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    requests = crud.get_requests(db, skip=skip, limit=limit)
    return requests


@app.post("/requests", status_code=202, response_model=schemas.Request)
def create_request(request: schemas.RequestCreate, db: Session = Depends(get_db)):
    return crud.create_request(db, request)


@app.get("/request/{request_id}")
def read_request(request_id: str, db: Session = Depends(get_db)):
    requests = crud.get_request(db, request_id)
    return requests
