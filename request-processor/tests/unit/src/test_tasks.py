import datetime
import database
import pytest
import json
from src import tasks
from src.tasks import save_response_to_db
from request_model import models, schemas
from unittest.mock import patch, MagicMock


@pytest.mark.parametrize(
    "test_name, request_type, params, response_data, expected_keys",
    [
        (
            "success_check_file",
            schemas.RequestTypeEnum.check_file,
            schemas.CheckFileParams(
                collection="article-4-direction",
                dataset="article-4-direction-area",
                original_filename="article-direction-area.csv",
                uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
            ),
            {
                "column-field-log": {},
                "error-summary": {},
                "transformed-csv": [
                    {"column1": "value1", "column2": "value2-transformed"},
                    {"column1": "value3", "column2": "value4"},
                ],
                "converted-csv": [
                    {"column1": "value1", "column2": "value2"},
                    {"column1": "value3", "column2": "value4"},
                ],
                "issue-log": [
                    {"entry-number": "1", "issue": "Issue 1"},
                    {"entry-number": "2", "issue": "Issue 2"},
                ],
            },
            ["error-summary", "column-field-log"],
        ),
        (
            "exception_check_url",
            schemas.RequestTypeEnum.check_url,
            schemas.CheckUrlParams(
                collection="article-4-direction",
                dataset="article-4-direction-area",
                url="invalidurl.geojson",
            ),
            {"message": "Test message", "status": "404"},
            ["errCode", "errMsg", "errTime", "errType"],
        ),
    ],
)
def test_save_response_to_db(
    db, test_name, request_type, params, response_data, expected_keys
):
    request_model = models.Request(
        type=request_type,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=params.model_dump(),
    )
    db_session = database.session_maker()
    with db_session() as session:
        session.add(request_model)
        session.commit()
        session.refresh(request_model)
    request = schemas.Request(
        id=request_model.id,
        type=request_model.type,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        params=request_model.params,
        response=None,
    )
    # Call the function
    save_response_to_db(request.id, response_data)

    with db_session() as session:
        # Check if response table has data|| Check if table has error
        response_query = (
            session.query(models.Response)
            .filter_by(request_id=request_model.id)
            .first()
        )
        assert response_query is not None, "Response table should contain data"
        data = (
            response_query.data
            if test_name == "success_check_file"
            else response_query.error
        )

        for key in expected_keys:
            assert key in data, f"{key} should be present in data"

        if test_name == "success_check_file":
            # Check if response_details table has details
            response_details_query = (
                session.query(models.ResponseDetails)
                .filter_by(response_id=response_query.id)
                .first()
            )
            assert (
                response_details_query is not None
            ), "ResponseDetails table should contain details"
            detail = response_details_query.detail
            assert "converted_row" in detail, "converted_row should be present in data"
            assert "issue_logs" in detail, "issue_logs should be present in data"
            assert "entry_number" in detail, "entry_number should be present in data"
            assert (
                "transformed_row" in detail
            ), "transformed_row should be present in data"


def test_add_data_task_success(monkeypatch):
    request = {"id": "req-123", "status": "NEW", "params": {"collection": "col", "dataset": "ds", "organisation": "org",
                                                            "url": "http://example.com/data.csv"}}
    directories_dict = {"COLLECTION_DIR": "/tmp/collection", "PIPELINE_DIR": "/tmp/pipeline"}
    directories_json = json.dumps(directories_dict)
    request_schema = MagicMock()
    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-123"
    request_schema.params = MagicMock()
    request_schema.params.collection = "col"
    request_schema.params.dataset = "ds"
    request_schema.params.organisation = "org"
    request_schema.params.url = "http://example.com/data.csv"

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: ("file.csv", {}))
    monkeypatch.setattr(tasks.workflow, "add_data_workflow", lambda *a, **kw: {"result": "ok"})
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = tasks.add_data_task(request, directories_json)

    assert result["id"] == "req-123"
    assert result["status"] == "COMPLETE"


def test_add_data_task_fail(monkeypatch):
    request = {"id": "req-123", "status": "NEW", "params": {"collection": "col", "dataset": "ds", "organisation": "org",
                                                            "url": "http://example.com/data.csv"}}
    directories_dict = {"COLLECTION_DIR": "/tmp/collection", "PIPELINE_DIR": "/tmp/pipeline"}
    directories_json = json.dumps(directories_dict)
    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-123"
    request_schema.params = MagicMock()
    request_schema.params.collection = "col"
    request_schema.params.dataset = "ds"
    request_schema.params.organisation = "org"
    request_schema.params.url = "http://example.com/data.csv"

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)

    def raise_custom(*a, **kw): raise tasks.CustomException({"message": "fail", "status": "404"})

    monkeypatch.setattr(tasks, "_fetch_resource", raise_custom)
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "FAILED"})

    with pytest.raises(tasks.CustomException):
        tasks.add_data_task(request, directories_json)