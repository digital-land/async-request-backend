import datetime
import pytest
from fastapi.testclient import TestClient
from urllib.parse import quote

from main import app
import database
from request_model import schemas, models
from tests.integration.test_crud import test_request

client = TestClient(app)

expected_json = [
    {
        "line": 1,
        "issue_logs": [
            {
                "field": "organisation",
                "issue-type": "invalid geometry - fixed",
                "severity": "warning",
            }
        ],
    },
    {
        "line": 2,
        "issue_logs": [
            {
                "field": "geometry",
                "issue-type": "invalid organisation",
                "severity": "error",
            }
        ],
    },
    {"line": 3, "issue_logs": [{"severity": "warning"}]},
]


@pytest.mark.parametrize(
    "jsonpath, expected_json, expected_total, expected_pglimit",
    [
        (
            '$.issue_logs[*]."severity"=="warning"',
            [expected_json[0], expected_json[2]],
            "2",
            "1",
        ),
        (
            '$.issue_logs[*]."severity"=="error" && $.issue_logs[*]."field"=="geometry"',
            [expected_json[1]],
            "1",
            "1",
        ),
    ],
)
def test_read_response_details_jsonpath_filters(
    db, helpers, test_request, jsonpath, expected_json, expected_total, expected_pglimit
):
    jsonpath = quote(jsonpath)
    response = client.get(
        f"/requests/{test_request.id}/response-details?jsonpath={jsonpath}"
    )
    assert response.status_code == 200
    assert response.json() == expected_json
