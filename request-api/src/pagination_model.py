from typing import List, Any

from pydantic import BaseModel


class PaginationParams(BaseModel):
    offset: int = 0
    limit: int = 50


class PaginatedResult(BaseModel):
    params: PaginationParams
    total_results_available: int
    data: List[Any]

