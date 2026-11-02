"""Contact-bearing schemas deliberately separate from worker request schemas."""

from datetime import datetime
from enum import Enum
from typing import Any, Literal, Optional

from pydantic import (
    BaseModel,
    ConfigDict,
    EmailStr,
    Field,
    HttpUrl,
    TypeAdapter,
    field_validator,
    model_validator,
)


class AddDataSubmissionStatus(str, Enum):
    TO_BE_ADDED = "TO_BE_ADDED"
    IN_PROGRESS = "IN_PROGRESS"
    QUERIED = "QUERIED"
    ADDED = "ADDED"
    CLOSED = "CLOSED"


class AddDataSubmissionCreate(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    check_request_id: str = Field(min_length=1)
    dataset: str = Field(min_length=1)
    documentation_url: str
    # Sentry serialises Pydantic objects in frame locals using their repr.
    # repr=False hides contacts there without changing persistence/API output.
    contact_name: Optional[str] = Field(
        default=None, min_length=1, max_length=200, repr=False
    )
    contact_email: Optional[EmailStr] = Field(default=None, repr=False)
    source: Literal["check_tool", "manage"] = "check_tool"
    jira_issue_key: Optional[str] = Field(
        default=None, pattern=r"^[A-Z][A-Z0-9_]*-[1-9][0-9]*$"
    )

    @field_validator("documentation_url")
    @classmethod
    def validate_url(cls, value):
        TypeAdapter(HttpUrl).validate_python(value)
        return value

    @model_validator(mode="after")
    def validate_contacts(self):
        if self.source == "check_tool" and not (
            self.contact_name and self.contact_email
        ):
            raise ValueError(
                "Check tool Add Data Submissions require contact name and email"
            )
        if self.source == "manage" and (self.contact_name or self.contact_email):
            raise ValueError("Manage Add Data Submissions do not have contact details")
        return self


class AddDataSubmissionReceipt(BaseModel):
    """Public acknowledgement; never include contact or workflow details here."""

    model_config = ConfigDict(from_attributes=True)

    id: str
    reference: str


class AddDataSubmissionEvent(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    add_data_submission_id: str
    event_type: str
    from_status: Optional[AddDataSubmissionStatus]
    to_status: AddDataSubmissionStatus
    actor: Optional[str]
    assignee: Optional[str]
    note: Optional[str]
    created: datetime


class AddDataSubmissionRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    reference: str
    created: datetime
    modified: datetime
    submitted_at: datetime
    source: Literal["check_tool", "manage"]
    status: AddDataSubmissionStatus = Field(
        description="Add Data Submission workflow state, separate from request processing status."
    )
    status_updated_at: datetime
    assignee: Optional[str]
    contact_name: Optional[str] = Field(repr=False)
    contact_email: Optional[str] = Field(repr=False)
    organisation: str
    organisation_name: Optional[str]
    dataset: str
    collection: str
    endpoint_url: str
    documentation_url: str
    geom_type: Optional[str]
    origin_request_id: str
    check_request_id: Optional[str]
    preview_request_id: Optional[str]
    add_data_details: Optional[dict[str, Any]]
    closed_reason: Optional[str]
    related_add_data_submission_id: Optional[str]
    jira_issue_key: Optional[str]
    events: list[AddDataSubmissionEvent]
