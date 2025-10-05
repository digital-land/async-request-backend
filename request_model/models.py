import shortuuid
from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, func, ForeignKey
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
    plugin = Column(String, nullable=True)

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
