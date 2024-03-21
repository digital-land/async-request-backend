import datetime
from enum import Enum
from typing import Union, Literal, Optional
from pydantic import BaseModel, Field


class RequestTypeEnum(str, Enum):
    check_url = "check_url"
    check_file = "check_file"


class Params(BaseModel):
    type: RequestTypeEnum
    dataset: str
    collection: str


class CheckFileParams(Params):
    type: Literal[RequestTypeEnum.check_file] = RequestTypeEnum.check_file
    original_filename: str
    uploaded_filename: str
    geom_type: Optional[str] = None


class CheckUrlParams(Params):
    type: Literal[RequestTypeEnum.check_url] = RequestTypeEnum.check_url
    url: str
    geom_type: Optional[str] = None


class RequestBase(BaseModel):
    params: Union[CheckUrlParams, CheckFileParams] = Field(discriminator="type")


class RequestCreate(RequestBase):
    pass


class Request(RequestBase):
    id: int
    type: RequestTypeEnum
    status: str
    created: datetime.datetime
    modified: datetime.datetime

    class Config:
        from_attributes = True


class Reponse(BaseModel):
    pass
