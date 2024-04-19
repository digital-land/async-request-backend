from sqlalchemy.orm import Session

from request_model import models


def get_request(db: Session, request_id: int):
    return db.query(models.Request).filter(models.Request.id == request_id).first()


def get_response(db: Session, request_id: int):
    return (
        db.query(models.Response)
        .filter(models.Response.request_id == request_id)
        .first()
    )
