import datetime
from enum import Enum
from typing import Union, Literal, Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


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


class ResponseModel(BaseModel):
    data: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]
    details: Optional[List[Dict[Any, Any]]]


class Request(RequestBase):
    id: str
    type: RequestTypeEnum
    status: str
    created: datetime.datetime
    modified: datetime.datetime
    response: Optional[ResponseModel]

    model_config: ConfigDict(from_attributes=True)


class ResponseData(BaseModel):
    error_summary: Optional[List[str]]
    column_field_log: Optional[List[Dict[str, Any]]]


class ResponseError(BaseModel):
    code: Optional[str]
    type: Optional[str]
    msg: Optional[str]
    time: Optional[datetime.datetime]

