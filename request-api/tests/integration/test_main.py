import pytest
from fastapi.testclient import TestClient
from pydantic_core import ValidationError

from main import app
from request_model import schemas

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


def test_read_unknown_request(db):
    response = client.get("/requests/0")
    assert response.status_code == 404
