from enum import Enum
from typing import Optional, List

from pydantic import BaseModel, Field


class ReadResponseDetailsParams(BaseModel):
    offset: int = Field(0, ge=0)
    limit: int = Field(50, ge=1, le=100)
    jsonpath: Optional[str] = Field(None)
    issue_type: Optional[str] = None 
    field: Optional[str] = None


class HealthStatus(str, Enum):
    HEALTHY = "HEALTHY"
    UNHEALTHY = "UNHEALTHY"


class DependencyHealth(BaseModel):
    name: str
    status: HealthStatus


class HealthCheckResponse(BaseModel):
    name: str
    version: str
    dependencies: List[DependencyHealth]
