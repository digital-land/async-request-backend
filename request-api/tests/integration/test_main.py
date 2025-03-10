import datetime
from unittest import mock
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from pydantic_core import ValidationError
from sqlalchemy.exc import SQLAlchemyError
from botocore.exceptions import ClientError

from crud import get_response_details
import database
from main import app, _get_db, _get_sqs_client, send_slack_alert
from request_model import schemas, models

client = TestClient(app)


@patch("main.boto3.client")
def test_get_sqs_client_success(mock_boto_client):
    mock_sqs_client = MagicMock()
    mock_boto_client.return_value = mock_sqs_client
    sqs_client_instance = _get_sqs_client()
    assert sqs_client_instance == mock_sqs_client
    mock_boto_client.assert_called_once()


@patch("main.boto3.client")
def test_get_sqs_client_retry(mock_boto_client):
    mock_sqs_client = MagicMock()
    mock_boto_client.return_value = mock_sqs_client

    # Simulate the first 3 attempts failing, and the 4th attempt succeeding
    mock_boto_client.side_effect = [
        ClientError({"Error": {"Code": "ThrottlingException"}}, "CreateQueue"),
        ClientError({"Error": {"Code": "ThrottlingException"}}, "CreateQueue"),
        ClientError({"Error": {"Code": "ThrottlingException"}}, "CreateQueue"),
        mock_sqs_client,  # 4th attempt succeeds
    ]
    sqs_client_instance = _get_sqs_client()
    assert mock_boto_client.call_count == 4
    assert sqs_client_instance == mock_sqs_client


@patch("main.boto3.client")
@patch("main.send_slack_alert")
@patch("main.is_connection_restored", return_value=False)
def test_get_sqs_client_fails_after_retries(
    mock_restored, mock_slack, mock_boto_client
):
    mock_boto_client.side_effect = ClientError(
        {"Error": {"Code": "ThrottlingException"}}, "CreateQueue"
    )
    with patch("main.logging.exception") as mock_log:
        sqs_client = _get_sqs_client()
        assert sqs_client is None  # Should return None after all retries

        assert mock_boto_client.call_count == 5
        mock_slack.assert_called_once_with(
            "SQS connection issue detected in async-request-backend.."
        )
        mock_log.assert_called()


@pytest.fixture
def mock_session_maker():
    with mock.patch("main.session_maker") as mock_session:
        yield mock_session


def test_get_db_success(mock_session_maker):
    mock_db_instance = mock.MagicMock()
    mock_session_maker.return_value = mock.MagicMock(return_value=mock_db_instance)

    # Run the _get_db function
    db_instance = next(_get_db())
    assert db_instance == mock_db_instance


def test_get_db_retry(mock_session_maker):
    mock_db_instance = mock.MagicMock()

    def side_effect():
        if mock_session_maker.call_count < 3:
            raise SQLAlchemyError("DB connection failed")
        return (
            lambda: mock_db_instance
        )  # session_maker()() should return mock_db_instance

    mock_session_maker.side_effect = side_effect
    generator = _get_db()
    db_instance = next(generator)
    assert isinstance(db_instance, mock.MagicMock)

    # Ensure that db.close() is called once
    db_instance.close.assert_not_called()
    next(generator, None)  # triggers `finally` block
    db_instance.close.assert_called_once()


@patch("main.session_maker", side_effect=SQLAlchemyError("DB connection failed"))
@patch("main.send_slack_alert")
@patch("main.is_connection_restored", return_value=False)
def test_get_db_fails_after_retries(mock_restored, mock_slack, mock_session_maker):
    with patch("main.logging.exception") as mock_log:
        generator = _get_db()
        assert next(generator, None) is None

        assert mock_session_maker.call_count == 5
        mock_slack.assert_called_once_with(
            "DB connection issue detected in async-request-backend.."
        )
        mock_log.assert_called()


@patch("main.WebClient")
@patch(
    "main.os.environ.get",
    side_effect=lambda k, d=None: "fake_token"
    if k == "SLACK_BOT_TOKEN"
    else "fake_channel",
)
def test_send_slack_alert(mock_env, mock_webclient):
    mock_client_instance = mock_webclient.return_value
    send_slack_alert("Test alert")
    mock_client_instance.chat_postMessage.assert_called_once_with(
        channel="fake_channel", text="Test alert", username="SQS and DB"
    )


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
    {
        "line": 1,
        "issue_logs": [
            {
                "severity": "warning",
                "field": "organisation",
                "issue-type": "invalid geometry - fixed",
            },
            {"severity": "error"},
        ],
    },
    {
        "line": 2,
        "issue_logs": [
            {
                "severity": "error",
                "field": "geometry",
                "issue-type": "invalid organisation",
            },
            {"severity": "error"},
        ],
    },
    {"line": 3, "issue_logs": [{"severity": "warning"}]},
]


@pytest.mark.parametrize(
    "limit, offset, expected_json, expected_total, expected_pglimit",
    [
        (50, 0, expected_jsondata, "3", "50"),  # test_read_response_details
        (1, 0, [expected_jsondata[0]], "3", "1"),  # test_read_response_details_limit_1
        (
            1,
            1,
            [expected_jsondata[1]],
            "3",
            "1",
        ),  # test_read_response_details_limit_offset1_limit1
        (
            1,
            2,
            [expected_jsondata[2]],
            "3",
            "1",
        ),  # test_read_response_details_limit_offset2_limit1
    ],
)
def test_read_response_details_limit_offset(
    db,
    helpers,
    test_request,
    limit,
    offset,
    expected_json,
    expected_total,
    expected_pglimit,
):
    read_details_response = client.get(
        f"/requests/{test_request.id}/response-details?offset={offset}&limit={limit}"
    )
    assert_response_content(
        read_details_response, expected_json, expected_total, offset, expected_pglimit
    )


@pytest.mark.parametrize(
    "jsonpath, offset, limit, expected_json, expected_total, expected_pglimit",
    [
        (
            '$.issue_logs[*].severity=="error"',
            0,
            1,
            [expected_jsondata[0]],
            "2",
            "1",
        ),  # test_read_response_details_limit1_jsonpath
        (
            '$.issue_logs[*].severity=="error"',
            1,
            1,
            [expected_jsondata[1]],
            "2",
            "1",
        ),  # test_read_response_details_offset1_limit1_jsonpath
    ],
)
def test_read_response_details_limit1_jsonpath(
    db,
    helpers,
    test_request,
    jsonpath,
    offset,
    limit,
    expected_json,
    expected_total,
    expected_pglimit,
):
    read_details_response = client.get(
        f"/requests/{test_request.id}/response-details?offset={offset}&limit={limit}&jsonpath={jsonpath}"
    )
    assert_response_content(
        read_details_response, expected_json, expected_total, offset, expected_pglimit
    )


def test_read_unknown_request(db):
    response = client.get("/requests/0")
    assert response.status_code == 404


@pytest.fixture(scope="module")
def db_session(db):
    session = database.session_maker()
    session = session()

    try:
        yield session
    finally:
        session.rollback()
        session.close()


@pytest.mark.parametrize(
    "jsonpath, expected_lines",
    [
        (
            '$.issue_logs[*]."severity"=="error" && $.issue_logs[*]."field"=="geometry"',
            [2],
        ),
        (
            '$.issue_logs[*]."severity"=="warning" && $.issue_logs[*]."issue-type"=="invalid geometry - fixed"',
            [1],
        ),
        ('$.issue_logs[*]."severity"=="warning"', [1, 3]),
    ],
)
def test_get_response_details(db_session, test_request, jsonpath, expected_lines):
    # Direct query test
    result = get_response_details(db_session, test_request.id, jsonpath)

    # Extract line numbers from the result
    result_lines = [detail.detail["line"] for detail in result.data]

    assert sorted(result_lines) == sorted(expected_lines)
    assert result.total_results_available >= len(expected_lines)


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
                        "issue_logs": [
                            {
                                "field": "organisation",
                                "issue-type": "invalid geometry - fixed",
                                "severity": "warning",
                            },
                            {"severity": "error"},
                        ],
                    }
                ),
                models.ResponseDetails(
                    detail={
                        "line": 2,
                        "issue_logs": [
                            {
                                "field": "geometry",
                                "issue-type": "invalid organisation",
                                "severity": "error",
                            },
                            {"severity": "error"},
                        ],
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


def assert_response_content(
    read_details_response, expected_json, expected_total, offset, expected_pglimit
):
    assert read_details_response.status_code == 200
    assert read_details_response.json() == expected_json
    assert read_details_response.headers["X-Pagination-Total-Results"] == expected_total
    assert read_details_response.headers["X-Pagination-Offset"] == str(offset)
    assert read_details_response.headers["X-Pagination-Limit"] == str(expected_pglimit)
