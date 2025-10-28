from datetime import datetime
from unittest.mock import patch, MagicMock, Mock

import pytest
from botocore.exceptions import BotoCoreError
from fastapi import HTTPException
from fastapi.testclient import TestClient
from kombu.exceptions import OperationalError
from sqlalchemy import Result
from sqlalchemy.engine.result import ResultMetaData
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

import main
from main import app
from request_model import models, schemas
from schema import HealthCheckResponse, DependencyHealth, HealthStatus

client = TestClient(app)

exception_msg = "Fake connection error message"


def _create_request_model():
    return models.Request(
        id="6WuEVYfuScqnW4oewgbyZd",
        type="check_file",
        created=datetime.now(),
        modified=datetime.now(),
        status="NEW",
        params=schemas.CheckFileParams(
            collection="tree-preservation-order",
            dataset="tree",
            original_filename="something.csv",
            uploaded_filename="generated.csv",
        ),
        response=None,
    )


@patch("crud.create_request", return_value=_create_request_model())
@patch(
    "task_interface.base_tasks.CheckDataFileTask.delay",
    side_effect=OperationalError(exception_msg),
)
def test_create_request_when_celery_throws_exception(
    mock_task_delay, mock_create_request, helpers
):
    with pytest.raises(OperationalError) as error:
        main.create_request(
            helpers.build_request_create(), http_request=None, http_response=None
        )
        assert exception_msg == error.value


@patch("crud.get_request", return_value=None)
def test_read_request_when_not_found(mock_get_request):
    with pytest.raises(HTTPException) as exception:
        main.read_request("unknown")
        assert 400 == exception.value.detail["errCode"]


@pytest.mark.parametrize(
    "db_status, sqs_status, expected_status, expected_response",
    [
        (
            True,
            True,
            200,
            [
                DependencyHealth(name="request-db", status=HealthStatus.HEALTHY),
                DependencyHealth(name="sqs", status=HealthStatus.HEALTHY),
            ],
        ),
        (
            False,
            True,
            500,
            [
                DependencyHealth(name="request-db", status=HealthStatus.UNHEALTHY),
                DependencyHealth(name="sqs", status=HealthStatus.HEALTHY),
            ],
        ),
        (
            True,
            False,
            500,
            [
                DependencyHealth(name="request-db", status=HealthStatus.HEALTHY),
                DependencyHealth(name="sqs", status=HealthStatus.UNHEALTHY),
            ],
        ),
        (
            False,
            False,
            500,
            [
                DependencyHealth(name="request-db", status=HealthStatus.UNHEALTHY),
                DependencyHealth(name="sqs", status=HealthStatus.UNHEALTHY),
            ],
        ),
    ],
)
def test_healthcheck(
    db_status,
    sqs_status,
    expected_status,
    expected_response,
    mock_response,
    mock_db,
    mock_sqs,
):
    if not db_status:
        mock_db.execute = MagicMock(side_effect=SQLAlchemyError())
    if not sqs_status:
        mock_sqs.get_queue_url = MagicMock(side_effect=BotoCoreError())

    response = main.healthcheck(response=mock_response, db=mock_db, sqs=mock_sqs)
    assert response == HealthCheckResponse(
        name="request-api",
        version="unknown",
        dependencies=expected_response,
    )
    assert mock_response.status_code == expected_status


@pytest.fixture
def mock_response():
    mock_response = Mock()
    return mock_response


@pytest.fixture
def mock_db():
    mock_result = Result(cursor_metadata=ResultMetaData())
    mock_result.all = MagicMock(return_value=[1])
    mock_session = Session()
    mock_session.execute = MagicMock(return_value=mock_result)
    return mock_session


@pytest.fixture
def mock_sqs():
    mock_sqs = Mock()
    return mock_sqs
