import datetime
from enum import Enum
from typing import Union, Literal, Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


class RequestTypeEnum(str, Enum):
    check_url = "check_url"
    check_file = "check_file"
    add_data = "add_data"


class PluginTypeEnum(str, Enum):
    arcgis = "arcgis"
    wfs = "wfs"


class Params(BaseModel):
    type: RequestTypeEnum
    dataset: str
    collection: str
    column_mapping: Optional[Dict[str, str]] = None
    geom_type: Optional[str] = None


class CheckFileParams(Params):
    type: Literal[RequestTypeEnum.check_file] = RequestTypeEnum.check_file
    original_filename: str
    uploaded_filename: str


class CheckUrlParams(Params):
    type: Literal[RequestTypeEnum.check_url] = RequestTypeEnum.check_url
    url: str
    plugin: Optional[PluginTypeEnum] = None


class AddDataParams(Params):
    type: Literal[RequestTypeEnum.add_data] = RequestTypeEnum.add_data
    url: Optional[str] = None
    source_request_id: Optional[str] = None
    documentation_url: Optional[str] = None
    licence: Optional[str] = None
    start_date: Optional[str] = None
    organisation: Optional[str] = None
    column_mapping: Optional[Dict[str, str]] = None
    geom_type: Optional[str] = None


class RequestBase(BaseModel):
    params: Union[CheckUrlParams, CheckFileParams, AddDataParams] = Field(
        discriminator="type"
    )


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
