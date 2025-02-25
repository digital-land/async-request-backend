import datetime
import pytest
import database

from src.pagination_model import PaginationParams
from request_model import schemas, models
from src.crud import get_response_details

expected_jsondata = [
    {
        "line": 1,
        "issue_logs": [
            {
                "severity": "warning",
                "field": "organisation",
                "issue-type": "invalid geometry - fixed",
            },
        ],
    },
    {
        "line": 2,
        "issue_logs": [
            {
                "severity": "error",
                "field": "geometry",
                "issue-type": "invalid organisation",
            }
        ],
    },
    {"line": 3, "issue_logs": [{"severity": "warning"}]},
]


@pytest.mark.parametrize(
    "jsonpath, expected_lines",
    [
        ('$.issue_logs[*]."severity"=="error"', [2]),
        ('$.issue_logs[*]."severity"=="warning"', [1, 3]),
        ('$.issue_logs[*]."field"=="geometry"', [2]),
    ],
)
def test_get_response_details(db, create_test_request, jsonpath, expected_lines):
    pagination_params = {"offset": 0, "limit": 10}
    pagination_params = PaginationParams(**pagination_params)

    result = get_response_details(
        db, create_test_request.id, jsonpath, pagination_params
    )

    # Extract line numbers from returned details
    result_lines = [detail.detail["line"] for detail in result.data]

    # Assert expected lines match actual result
    assert sorted(result_lines) == sorted(expected_lines)
    assert result.total_results_available >= len(expected_lines)


@pytest.fixture(scope="module")
def create_test_request():
    request_model = models.Request(
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
        session.add(request_model)
        session.commit()
        session.refresh(request_model)
        return request_model
