from sqlalchemy import Column, Integer, String, DateTime, JSON, func

from database import Base


class Request(Base):
    __tablename__ = "request"

    id = Column(Integer, primary_key=True)
    created = Column(DateTime(timezone=True), server_default=func.now())
    modified = Column(DateTime(timezone=True), server_default=func.now())
    user_email = Column(String)
    status = Column(String)
    data = Column(JSON)
