from typing import Optional

from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, JSON, func
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Request(Base):
    __tablename__ = "request"

    id = Column(Integer, primary_key=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    modified = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())
    user_email = Column(String)
    status = Column(String)
    data = Column(JSON)


class UploadedFile(BaseModel):
    original_filename: str
    uploaded_filename: str


class ResponseData(BaseModel):
    message: str


# Used in JSON column
class RequestData(BaseModel):
    uploaded_file: UploadedFile
    response: Optional[ResponseData]
