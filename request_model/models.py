import shortuuid
from pydantic import BaseModel
from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    func,
    ForeignKey,
    CheckConstraint,
    UniqueConstraint,
    Index,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Request(Base):
    __tablename__ = "request"

    id = Column(String, primary_key=True, default=lambda: shortuuid.uuid(), unique=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    modified = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    status = Column(String)
    params = Column(JSONB)
    type = Column(String)
    add_data_submission_id = Column(
        String, ForeignKey("add_data_submission.id"), index=True
    )

    response = relationship(
        "Response", uselist=False, back_populates="request", lazy="joined"
    )


class Response(Base):
    __tablename__ = "response"

    id = Column(Integer, primary_key=True)
    request_id = Column(String, ForeignKey("request.id"), index=True)
    data = Column(JSONB)
    error = Column(JSONB)

    request = relationship("Request", back_populates="response")
    details = relationship(
        "ResponseDetails", back_populates="response", uselist=True, lazy="noload"
    )


class ResponseDetails(Base):
    __tablename__ = "response_details"

    id = Column(Integer, primary_key=True)
    response_id = Column(Integer, ForeignKey("response.id"), index=True)
    detail = Column(JSONB)

    response = relationship("Response", back_populates="details")


class ResponseData(BaseModel):
    message: str


class AddDataSubmission(Base):
    __tablename__ = "add_data_submission"
    __table_args__ = (
        UniqueConstraint("reference", name="uq_add_data_submission_reference"),
        UniqueConstraint(
            "origin_request_id", "dataset", name="uq_add_data_submission_origin_dataset"
        ),
        CheckConstraint(
            "reference = upper(reference)",
            name="ck_add_data_submission_reference_upper",
        ),
        CheckConstraint(
            "status IN ('TO_BE_ADDED', 'IN_PROGRESS', 'QUERIED', 'ADDED', 'CLOSED')",
            name="ck_add_data_submission_status",
        ),
        CheckConstraint(
            "source IN ('check_tool', 'manage')", name="ck_add_data_submission_source"
        ),
        Index(
            "uq_add_data_submission_active_endpoint",
            "endpoint_url",
            "dataset",
            "organisation",
            unique=True,
            postgresql_where=text("status NOT IN ('ADDED', 'CLOSED')"),
        ),
    )

    id = Column(String, primary_key=True, default=lambda: shortuuid.uuid())
    reference = Column(String, nullable=False)
    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    modified = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )
    submitted_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    source = Column(String, nullable=False)
    status = Column(String, nullable=False, server_default="TO_BE_ADDED", index=True)
    status_updated_at = Column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    assignee = Column(String, index=True)
    contact_name = Column(String)
    contact_email = Column(String)
    organisation = Column(String, nullable=False)
    organisation_name = Column(String)
    dataset = Column(String, nullable=False)
    collection = Column(String, nullable=False)
    endpoint_url = Column(String, nullable=False)
    documentation_url = Column(String, nullable=False)
    geom_type = Column(String)
    origin_request_id = Column(String, ForeignKey("request.id"), nullable=False)
    check_request_id = Column(String, ForeignKey("request.id"), index=True)
    preview_request_id = Column(String, ForeignKey("request.id"), index=True)
    add_data_details = Column(JSONB)
    closed_reason = Column(String)
    related_add_data_submission_id = Column(
        String, ForeignKey("add_data_submission.id"), index=True
    )
    jira_issue_key = Column(String)
    events = relationship(
        "AddDataSubmissionEvent",
        order_by="AddDataSubmissionEvent.id",
        passive_deletes=True,
    )


class AddDataSubmissionEvent(Base):
    __tablename__ = "add_data_submission_event"

    id = Column(Integer, primary_key=True)
    add_data_submission_id = Column(
        String,
        ForeignKey("add_data_submission.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    event_type = Column(String, nullable=False)
    from_status = Column(String)
    to_status = Column(String, nullable=False)
    assignee = Column(String)
    actor = Column(String)
    note = Column(String)
    created = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
