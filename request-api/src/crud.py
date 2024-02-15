from sqlalchemy.orm import Session

import models
import schemas


def get_request(db: Session, request_id: int):
    return db.query(models.Request).filter(models.User.id == request_id).first()


def get_request_by_email(db: Session, user_email: str):
    return db.query(models.User).filter(models.Request.user_email == user_email).first()


def get_requests(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.User).offset(skip).limit(limit).all()


def create_request(db: Session, request: schemas.RequestCreate):
    db_request = models.Request(user_email=request.user_email, status='NEW')
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request
