import datetime
from fastapi.testclient import TestClient
import pytest

import database
from main import app
from request_model import models, schemas

client = TestClient(app)


@pytest.fixture(scope="module")
def sqs_queue(sqs_client):
    sqs_client.create_queue(QueueName="celery")
    return sqs_client


def test_healthcheck(db, sqs_queue):
    response = client.get("/health")
    response_json = response.json()
    assert response.status_code == 200
    assert response_json["dependencies"][0]["status"] == "HEALTHY"
    assert response_json["dependencies"][1]["status"] == "HEALTHY"


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
            "50",
        ),
        (
            '$.issue_logs[*]."severity"=="error" && $.issue_logs[*]."field"=="geometry"',
            [expected_json[1]],
            "1",
            "50",
        ),
    ],
)
def test_read_response_details_jsonpath_filters(
    db,
    helpers,
    create_test_request,
    jsonpath,
    expected_json,
    expected_total,
    expected_pglimit,
):
    response = client.get(
        f"/requests/{create_test_request.id}/response-details?jsonpath={jsonpath}"
    )
    assert response.status_code == 200
    assert response.json() == expected_json
    assert response.headers["X-Pagination-Total-Results"] == expected_total
    assert response.headers["X-Pagination-Limit"] == str(expected_pglimit)


@pytest.fixture(scope="module")
def create_test_request():
    test_request_model = models.Request(
        type=schemas.RequestTypeEnum.check_file,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="COMPLETE",
        params=schemas.CheckFileParams(
            collection="conservation-area",
            dataset="conservation-area",
            original_filename="conservation-area.csv",
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
                            }
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
                            }
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
        session.add(test_request_model)
        session.commit()
        session.refresh(test_request_model)
        return test_request_model
