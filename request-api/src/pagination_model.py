from typing import List, Any

from pydantic import BaseModel, Field


class PaginationParams(BaseModel):
    offset: int = Field(0, ge=0)
    limit: int = Field(50, ge=1, le=100)


class PaginatedResult(BaseModel):
    params: PaginationParams
    total_results_available: int = Field(ge=0)
    data: List[Any]
