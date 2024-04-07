from datetime import datetime
from unittest.mock import patch, Mock

import pytest
from fastapi.testclient import TestClient
from kombu.exceptions import OperationalError

import main
from main import app
from request_model import models, schemas

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
                uploaded_filename="generated.csv"
            ),
            response=None
    )


@patch('crud.create_request', return_value=_create_request_model())
@patch('task_interface.check_tasks.CheckDataFileTask.delay', side_effect=OperationalError(exception_msg))
def test_create_request_when_celery_throws_exception(mock_task_delay, mock_create_request, helpers):
    mock_create_request.return_value = _create_request_model()
    with pytest.raises(OperationalError) as error:
        main.create_request(helpers.build_request_create(), http_request=None, http_response=None)
        assert exception_msg == error.value




