from sqlalchemy.orm import Session

from request_model import models
from request_model import schemas


def get_request(db: Session, request_id: int):
    return db.query(models.Request).filter(models.Request.id == request_id).first()


def get_requests(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Request).offset(skip).limit(limit).all()


def create_request(db: Session, request: schemas.RequestCreate):
    db_request = models.Request(
        status="NEW", type=request.params.type, params=request.params.model_dump()
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request
