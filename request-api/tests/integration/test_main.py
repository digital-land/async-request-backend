import datetime

import pytest
from fastapi.testclient import TestClient
from pydantic_core import ValidationError

import database
from main import app
from request_model import schemas, models

client = TestClient(app)


def test_read_root():
    response = client.get("/")
    assert response.status_code == 200
    assert response.json() == {"msg": "Hello World"}


def test_create_request(db, sqs_queue, helpers):
    response = client.post("/requests", json=helpers.request_create_dict())
    print(response.json())
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


def test_read_response_details(db, helpers, test_request):
    read_details_response = client.get(f"/requests/{test_request.id}/response-details")
    assert read_details_response.status_code == 200
    assert read_details_response.json() == [
        {"line": 1, "some_key": "some_value"},
        {"line": 2, "some_key": "some_value"},
        {"line": 3, "some_key": "some_value"}
    ]
    assert read_details_response.headers['X-Pagination-Total-Results'] == "3"
    assert read_details_response.headers['X-Pagination-Offset'] == "0"
    assert read_details_response.headers['X-Pagination-Limit'] == "50"


def test_read_response_details_limit_1(db, helpers, test_request):
    read_details_response = client.get(f"/requests/{test_request.id}/response-details?limit=1")
    assert read_details_response.status_code == 200
    assert read_details_response.json() == [
        {"line": 1, "some_key": "some_value"}
    ]
    assert read_details_response.headers['X-Pagination-Total-Results'] == "3"
    assert read_details_response.headers['X-Pagination-Offset'] == "0"
    assert read_details_response.headers['X-Pagination-Limit'] == "1"


def test_read_response_details_limit_offset1_limit1(db, helpers, test_request):
    read_details_response = client.get(f"/requests/{test_request.id}/response-details?offset=1&limit=1")
    assert read_details_response.status_code == 200
    assert read_details_response.json() == [
        {"line": 2, "some_key": "some_value"}
    ]
    assert read_details_response.headers['X-Pagination-Total-Results'] == "3"
    assert read_details_response.headers['X-Pagination-Offset'] == "1"
    assert read_details_response.headers['X-Pagination-Limit'] == "1"


def test_read_response_details_limit_offset2_limit1(db, helpers, test_request):
    read_details_response = client.get(f"/requests/{test_request.id}/response-details?offset=2&limit=1")
    assert read_details_response.status_code == 200
    assert read_details_response.json() == [
        {"line": 3, "some_key": "some_value"}
    ]
    assert read_details_response.headers['X-Pagination-Total-Results'] == "3"
    assert read_details_response.headers['X-Pagination-Offset'] == "2"
    assert read_details_response.headers['X-Pagination-Limit'] == "1"


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
                    detail={"line": 1, "some_key": "some_value"}
                ),
                models.ResponseDetails(
                    detail={"line": 2, "some_key": "some_value"}
                ),
                models.ResponseDetails(
                    detail={"line": 3, "some_key": "some_value"}
                )
            ]
        )
    )
    db_session = database.session_maker()
    with db_session() as session:
        session.add(request_model)
        session.commit()
        session.refresh(request_model)
        return request_model
