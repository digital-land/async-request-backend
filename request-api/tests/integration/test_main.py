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


def test_create_request(db, sqs_queue):
    response = _post_create_request()
    request_id = response.json()['id']
    assert response.status_code == 202
    assert response.headers["Location"] == f"$testserver/requests/{request_id}"


def test_create_request_missing_uploaded_file(db, sqs_queue):
    with pytest.raises(ValidationError):
        response = _post_create_request(
            request=schemas.RequestCreate(
                params=schemas.CheckFileParams(
                    uploaded_filename="generated.csv"
                )
            )
        )
        assert response.status_code == 422


def test_read_request(db, sqs_queue):
    creation_response = _post_create_request()
    request_id = creation_response.json()['id']
    read_response = client.get(f"/requests/{request_id}")
    assert read_response.status_code == 200
    assert read_response.json() == creation_response.json()


def test_read_unknown_request(db):
    response = client.get("/requests/0")
    assert response.status_code == 404


def _post_create_request(request: schemas.RequestCreate = None):
    if request is None:
        request = schemas.RequestCreate(
            params=schemas.CheckFileParams(
                collection="tree-presevation-order",
                dataset="tree",
                original_filename="something.csv",
                uploaded_filename="generated.csv"
            )
        )
    return client.post("/requests", json=request.model_dump())