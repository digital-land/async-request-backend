from pydantic import BaseModel
from sqlalchemy import Column, Integer, String, DateTime, JSON, func, ForeignKey
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class Request(Base):
    __tablename__ = "request"

    id = Column(Integer, primary_key=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    modified = Column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now()
    )
    status = Column(String)
    params = Column(JSON)
    type = Column(String)

    response = relationship("Response", uselist=False, back_populates="request")


class Response(Base):
    __tablename__ = "response"

    id = Column(Integer, primary_key=True)
    request_id = Column(Integer, ForeignKey("request.id"))
    data = Column(JSON)
    error = Column(JSON)

    request = relationship("Request", back_populates="response")
    details = relationship("ResponseDetails", back_populates="response", uselist=False)


class ResponseDetails(Base):
    __tablename__ = "response_details"

    id = Column(Integer, primary_key=True)
    response_id = Column(Integer, ForeignKey("response.id"))
    detail = Column(JSON)

    response = relationship("Response", back_populates="details")


class ResponseData(BaseModel):
    message: str
