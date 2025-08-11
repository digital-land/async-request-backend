import datetime
from enum import Enum
from datetime import date
from typing import Union, Literal, Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, AnyHttpUrl


class RequestTypeEnum(str, Enum):
    check_url = "check_url"
    check_file = "check_file"


class Params(BaseModel):
    type: RequestTypeEnum
    dataset: str
    collection: str
    column_mapping: Optional[Dict[str, str]] = None
    geom_type: Optional[str] = None
    documentation_url: Optional[AnyHttpUrl] = None
    licence: Optional[str] = None
    start_date: Optional[date] = None


class CheckFileParams(Params):
    type: Literal[RequestTypeEnum.check_file] = RequestTypeEnum.check_file
    original_filename: str
    uploaded_filename: str


class CheckUrlParams(Params):
    type: Literal[RequestTypeEnum.check_url] = RequestTypeEnum.check_url
    url: str


class RequestBase(BaseModel):
    params: Union[CheckUrlParams, CheckFileParams] = Field(discriminator="type")


class RequestCreate(RequestBase):
    pass


class ResponseData(BaseModel):
    pass


class ResponseDetail(BaseModel):
    pass


class CheckDataFileResponseDetails(ResponseDetail):
    converted_csv: Optional[Dict[str, Any]]
    issue_log: Optional[List[Dict[str, Any]]]
    entry_number: Optional[int]


class CheckDataFileResponse(ResponseData):
    error_summary: Optional[List[str]]
    column_field_log: Optional[List[Dict[str, Any]]]


class ResponseError(BaseModel):
    code: Optional[str]
    type: Optional[str]
    msg: Optional[str]
    time: Optional[datetime.datetime]


class ResponseModel(BaseModel):
    data: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]


class Request(RequestBase):
    id: str
    type: RequestTypeEnum
    status: str
    created: datetime.datetime
    modified: datetime.datetime
    response: Optional[ResponseModel]

    model_config = ConfigDict(from_attributes=True)
