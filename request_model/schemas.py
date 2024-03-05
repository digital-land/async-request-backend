import datetime
from typing import Optional, Dict
from pydantic import BaseModel


class UploadedFile(BaseModel):
    original_filename: str
    uploaded_filename: str


class RequestBase(BaseModel):
    user_email: str


class RequestCreate(RequestBase):
    uploaded_file: UploadedFile


class Request(RequestBase):
    id: int
    status: str
    created: datetime.datetime
    modified: datetime.datetime
    data: Optional[Dict] = None

    class Config:
        from_attributes = True
