import datetime
import re
from enum import Enum
from typing import Union, Literal, Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict, model_validator
from pydantic_core import PydanticCustomError

from request_model.url_safety import UnsafeUrlError, check_fetch_url

# Pydantic error types for inputs refused for safety reasons, so callers can
# tell them apart from other validation errors (request-api counts these).
UNSAFE_URL_ERROR_TYPE = "unsafe_url"
UNSAFE_UPLOADED_FILENAME_ERROR_TYPE = "unsafe_uploaded_filename"

# Submit stores each upload in S3 under a uuidv4 key, which the worker joins
# onto a local path. Anything else could point outside the request's folder.
UPLOADED_FILENAME_PATTERN = re.compile(
    r"[0-9a-fA-F]{8}-(?:[0-9a-fA-F]{4}-){3}[0-9a-fA-F]{12}"
)
# The user's own filename; only stored and shown back. 255 is the usual
# filesystem limit for a file name.
MAX_ORIGINAL_FILENAME_LENGTH = 255
# add_data joins github_branch onto the config repo's download URL, so it must
# be a plain branch name: slash-separated segments of these characters, none of
# them "." or "..", so it cannot move the URL to another repo or file.
GITHUB_BRANCH_PATTERN = re.compile(r"[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*")
MAX_GITHUB_BRANCH_LENGTH = 255


def is_safe_github_branch(branch):
    return (
        isinstance(branch, str)
        and len(branch) <= MAX_GITHUB_BRANCH_LENGTH
        and bool(GITHUB_BRANCH_PATTERN.fullmatch(branch))
        and ".." not in branch
        and all(segment != "." for segment in branch.split("/"))
    )


class RequestTypeEnum(str, Enum):
    check_url = "check_url"
    check_file = "check_file"
    add_data = "add_data"


class PluginTypeEnum(str, Enum):
    arcgis = "arcgis"
    wfs = "wfs"


class Params(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: RequestTypeEnum
    service: Optional[str] = None
    organisationName: Optional[str] = None
    dataset: str
    collection: str
    column_mapping: Optional[Dict[str, str]] = None
    geom_type: Optional[str] = None
    endpoint_parameters: Optional[Any] = None


class CheckFileParams(Params):
    type: Literal[RequestTypeEnum.check_file] = RequestTypeEnum.check_file
    original_filename: str
    uploaded_filename: str


class CheckUrlParams(Params):
    type: Literal[RequestTypeEnum.check_url] = RequestTypeEnum.check_url
    url: str


class AddDataParams(Params):
    type: Literal[RequestTypeEnum.add_data] = RequestTypeEnum.add_data
    url: Optional[str] = None
    resource: Optional[str] = None
    documentation_url: Optional[str] = None
    authoritative: Optional[bool] = None
    authoritative_organisation: Optional[str] = None
    licence: Optional[str] = None
    start_date: Optional[str] = None
    organisation: Optional[str] = None
    plugin: Optional[PluginTypeEnum] = None
    github_branch: Optional[str] = None
    return_endpoint: Optional[str] = Field(
        default=None,
        description="Client return route identifier; not used by processing.",
    )
    excluded_references: Optional[List[str]] = None
    selected_redirects: Optional[List[Dict[str, str]]] = None


class RequestBase(BaseModel):
    add_data_submission_id: Optional[str] = Field(default=None, min_length=1)
    params: Union[CheckUrlParams, CheckFileParams, AddDataParams] = Field(
        discriminator="type"
    )


class RequestCreate(RequestBase):
    model_config = ConfigDict(extra="forbid")

    @model_validator(mode="after")
    def url_is_safe_to_fetch(self):
        # Only checked when a request is created, so stored requests created
        # before this check can still be read back.
        url = getattr(self.params, "url", None)
        if url:
            try:
                check_fetch_url(url)
            except UnsafeUrlError as e:
                raise PydanticCustomError(
                    UNSAFE_URL_ERROR_TYPE, "Invalid url: {reason}", {"reason": str(e)}
                ) from e
        return self

    @model_validator(mode="after")
    def uploaded_file_names_are_safe(self):
        # Only checked when a request is created, like the url check above.
        if not isinstance(self.params, CheckFileParams):
            return self
        if not UPLOADED_FILENAME_PATTERN.fullmatch(self.params.uploaded_filename):
            raise PydanticCustomError(
                UNSAFE_UPLOADED_FILENAME_ERROR_TYPE,
                "Invalid uploaded_filename: must be the upload's id",
            )
        original = self.params.original_filename
        if not original.strip() or len(original) > MAX_ORIGINAL_FILENAME_LENGTH:
            raise ValueError(
                "Invalid original_filename: must be 1 to "
                f"{MAX_ORIGINAL_FILENAME_LENGTH} characters"
            )
        return self

    @model_validator(mode="after")
    def github_branch_is_safe(self):
        # Only checked when a request is created, like the url check above.
        branch = getattr(self.params, "github_branch", None)
        if branch is not None and not is_safe_github_branch(branch):
            raise ValueError(
                "Invalid github_branch: must be a branch name of letters, digits, "
                "'.', '_', '-' and '/'"
            )
        return self


class ResponseData(BaseModel):
    pass


class ResponseDetail(BaseModel):
    pass


class CheckDataFileResponseDetails(ResponseDetail):
    converted_csv: Optional[Dict[str, Any]]
    issue_log: Optional[List[Dict[str, Any]]]
    entry_number: Optional[int]


class CheckDataFileResponse(ResponseData):
    pass


class ResponseError(BaseModel):
    code: Optional[str]
    type: Optional[str]
    msg: Optional[str]
    time: Optional[datetime.datetime]


class ResponseModel(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    data: Optional[Dict[str, Any]]
    error: Optional[Dict[str, Any]]


class Request(RequestBase):
    id: str
    type: RequestTypeEnum
    status: str = Field(
        description=(
            "Processing status. COMPLETE means processing finished, not that data was added. "
            "Submission workflow uses a separate status, including ADDED and CLOSED."
        )
    )
    created: datetime.datetime
    modified: datetime.datetime
    published_at: Optional[datetime.datetime] = None
    response: Optional[ResponseModel]
    model_config = ConfigDict(from_attributes=True)
