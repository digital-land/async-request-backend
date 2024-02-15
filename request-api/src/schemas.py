import datetime
from typing import Optional

from pydantic import BaseModel, Json


class RequestBase(BaseModel):
    user_email: str


class RequestCreate(RequestBase):
    pass


class Request(RequestBase):
    id: int
    status: str
    created: datetime.datetime
    modified: datetime.datetime
    data: Optional[Json] = None

    class Config:
        from_attributes = True
