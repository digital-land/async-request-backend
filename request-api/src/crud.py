from sqlalchemy.orm import Session

from request_model import models
from request_model import schemas


def get_request(db: Session, request_id: int):
    return db.query(models.Request).filter(models.Request.id == request_id).first()


def get_request_by_email(db: Session, user_email: str):
    return db.query(models.Request).filter(models.Request.user_email == user_email).first()


def get_requests(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.Request).offset(skip).limit(limit).all()


def create_request(db: Session, request: schemas.RequestCreate):
    db_request = models.Request(
        user_email=request.user_email,
        status='NEW',
        data=models.RequestData(
            uploaded_file=models.UploadedFile(
                original_filename=request.uploaded_file.original_filename,
                uploaded_filename=request.uploaded_file.uploaded_filename
            ),
            response=None
        ).model_dump()
    )
    db.add(db_request)
    db.commit()
    db.refresh(db_request)
    return db_request
