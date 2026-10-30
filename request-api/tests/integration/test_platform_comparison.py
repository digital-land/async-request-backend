"""Prepared rows remain separate from normal facts and are stably paginated."""

from uuid import uuid4

import pytest
from fastapi.testclient import TestClient

import crud
import database
import main
from pagination_model import PaginationParams
from request_model import models


@pytest.fixture
def comparison_response(db):
    session = database.session_maker()()
    request_id = uuid4().hex
    request = models.Request(
        id=request_id, type="add_data", status="COMPLETE", params={}
    )
    session.add(request)
    response = models.Response(request_id=request_id, data={})
    session.add(response)
    session.flush()
    session.add_all(
        [
            models.ResponseDetails(
                response_id=response.id,
                detail={"entry_number": 1, "transformed_row": []},
            ),
            models.ResponseDetails(
                response_id=response.id,
                detail={"platform-comparison": {"source": "resource", "entity": "100"}},
            ),
            models.ResponseDetails(
                response_id=response.id,
                detail={"entry_number": 2, "transformed_row": []},
            ),
            models.ResponseDetails(
                response_id=response.id,
                detail={"platform-comparison": {"source": "platform", "entity": "999"}},
            ),
        ]
    )
    session.commit()
    try:
        yield session, request_id
    finally:
        session.close()


def test_normal_details_exclude_prepared_rows(comparison_response):
    session, request_id = comparison_response
    result = crud.get_response_details(session, request_id)
    assert result.total_results_available == 2
    assert [row.detail["entry_number"] for row in result.data] == [1, 2]


def test_comparison_rows_have_their_own_stable_pagination(comparison_response):
    session, request_id = comparison_response
    first = crud.get_response_details(
        session,
        request_id,
        pagination_params=PaginationParams(offset=0, limit=1),
        comparison_only=True,
    )
    second = crud.get_response_details(
        session,
        request_id,
        pagination_params=PaginationParams(offset=1, limit=1),
        comparison_only=True,
    )
    assert first.total_results_available == second.total_results_available == 2
    assert first.data[0].detail["platform-comparison"]["entity"] == "100"
    assert second.data[0].detail["platform-comparison"]["entity"] == "999"


def test_comparison_api_returns_rows_and_total_header(comparison_response):
    session, request_id = comparison_response
    main.app.dependency_overrides[main._get_db] = lambda: session
    try:
        client = TestClient(main.app)
        first = client.get(f"/requests/{request_id}/platform-comparison?limit=1")
        second = client.get(
            f"/requests/{request_id}/platform-comparison?limit=1&offset=1"
        )
        normal = client.get(f"/requests/{request_id}/response-details")
    finally:
        main.app.dependency_overrides.clear()
    assert first.status_code == second.status_code == normal.status_code == 200
    assert first.headers["X-Pagination-Total-Results"] == "2"
    assert first.headers["X-Pagination-Offset"] == "0"
    assert first.headers["X-Pagination-Limit"] == "1"
    assert second.headers["X-Pagination-Offset"] == "1"
    assert first.json() == [{"source": "resource", "entity": "100"}]
    assert second.json() == [{"source": "platform", "entity": "999"}]
    assert normal.headers["X-Pagination-Total-Results"] == "2"
    assert [row["entry_number"] for row in normal.json()] == [1, 2]
