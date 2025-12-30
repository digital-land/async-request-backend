from unittest.mock import MagicMock

import pytest
from sqlalchemy.exc import ProgrammingError

import crud


def test_get_response_details_invalid_jsonpath_returns_empty(monkeypatch):
    base_query = MagicMock()
    base_query.join.return_value = base_query
    base_query.filter.return_value = base_query
    base_query.offset.return_value = base_query
    base_query.limit.return_value = base_query
    base_query.all.side_effect = ProgrammingError("bad jsonpath", None, None)
    base_query.count = MagicMock()

    db = MagicMock()
    db.query.return_value = base_query

    result = crud.get_response_details(db, request_id=1, jsonpath=";")

    assert result.data == []
    assert result.total_results_available == 0
    base_query.count.assert_not_called()
