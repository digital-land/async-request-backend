import datetime

import pytest
from fastapi.testclient import TestClient
from pydantic_core import ValidationError

import database
from main import app
from request_model import schemas, models

client = TestClient(app)


def test_create_request(db, sqs_queue, helpers):
    response = client.post("/requests", json=helpers.request_create_dict())
    request_id = response.json()["id"]
    assert response.status_code == 202
    assert response.headers["Location"] == f"$testserver/requests/{request_id}"


def test_create_request_missing_uploaded_file(db, sqs_queue, helpers):
    with pytest.raises(ValidationError):
        json_dict = helpers.request_create_dict(
            request=schemas.RequestCreate(
                params=schemas.CheckFileParams(uploaded_filename="generated.csv")
            )
        )
        response = client.post("/requests", json=json_dict)
        assert response.status_code == 422


def test_read_request(db, sqs_queue, helpers):
    creation_response = client.post("/requests", json=helpers.request_create_dict())
    request_id = creation_response.json()["id"]
    read_response = client.get(f"/requests/{request_id}")
    assert read_response.status_code == 200
    assert read_response.json() == creation_response.json()


expected_jsondata = [
    {"line": 1, "issue_logs": [{"severity": "warning"}, {"severity": "error"}]},
    {"line": 2, "issue_logs": [{"severity": "warning"}, {"severity": "error"}]},
    {"line": 3, "issue_logs": [{"severity": "warning"}]},
]


@pytest.mark.parametrize("limit, offset, expected_json, expected_total, expected_pglimit", [
    (50, 0, expected_jsondata, "3", "50"),  # test_read_response_details
    (1, 0, [expected_jsondata[0]], "3", "1"),  # test_read_response_details_limit_1
    (1, 1, [expected_jsondata[1]], "3", "1"),  # test_read_response_details_limit_offset1_limit1
    (1, 2, [expected_jsondata[2]], "3", "1"),  # test_read_response_details_limit_offset2_limit1
])
def test_read_response_details_limit_offset(db, helpers, test_request, limit, offset, expected_json, expected_total, expected_pglimit):
    read_details_response = client.get(f"/requests/{test_request.id}/response-details?offset={offset}&limit={limit}")
    assert_response_content(read_details_response, expected_json, expected_total, offset, expected_pglimit)


@pytest.mark.parametrize("jsonpath, offset, limit, expected_json, expected_total, expected_pglimit", [
    ('$.issue_logs[*].severity=="error"', 0, 1, [expected_jsondata[0]],
     "2", "1"),  # test_read_response_details_limit1_jsonpath
    ('$.issue_logs[*].severity=="error"', 1, 1, [expected_jsondata[1]],
     "2", "1"),  # test_read_response_details_offset1_limit1_jsonpath
])
def test_read_response_details_limit1_jsonpath(db, helpers, test_request, jsonpath, offset, limit, expected_json, expected_total, expected_pglimit):
    read_details_response = client.get(
        f"/requests/{test_request.id}/response-details?offset={offset}&limit={limit}&jsonpath={jsonpath}")
    assert_response_content(read_details_response, expected_json, expected_total, offset, expected_pglimit)


def test_read_unknown_request(db):
    response = client.get("/requests/0")
    assert response.status_code == 404


@pytest.fixture(scope="module")
def test_request():
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_file,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="COMPLETE",
        params=schemas.CheckFileParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            original_filename="article-direction-area.csv",
            uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
        ).model_dump(),
        response=models.Response(
            data='{ "some_key": "some_value" }',
            details=[
                models.ResponseDetails(
                    detail={
                        "line": 1,
                        "issue_logs": [{"severity": "warning"}, {"severity": "error"}],
                    }
                ),
                models.ResponseDetails(
                    detail={
                        "line": 2,
                        "issue_logs": [{"severity": "warning"}, {"severity": "error"}],
                    }
                ),
                models.ResponseDetails(
                    detail={"line": 3, "issue_logs": [{"severity": "warning"}]}
                ),
            ],
        ),
    )
    db_session = database.session_maker()
    with db_session() as session:
        session.add(request_model)
        session.commit()
        session.refresh(request_model)
        return request_model


def assert_response_content(read_details_response, expected_json, expected_total, offset, expected_pglimit):
    assert read_details_response.status_code == 200
    assert read_details_response.json() == expected_json
    assert read_details_response.headers["X-Pagination-Total-Results"] == expected_total
    assert read_details_response.headers["X-Pagination-Offset"] == str(offset)
    assert read_details_response.headers["X-Pagination-Limit"] == str(expected_pglimit)
