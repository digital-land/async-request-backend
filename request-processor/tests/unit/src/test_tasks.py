import datetime
import database
import pytest
import json
from src import tasks
from src.tasks import save_response_to_db,_fetch_resource,check_dataurl
from request_model import models, schemas
from unittest.mock import patch, MagicMock
from application.exceptions.customExceptions import CustomException
from application.configurations.config import Directories



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

def test_check_dataurl_success(monkeypatch):
    """Test successful execution of check_dataurl task"""
    request = {
        "id": "req-001",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/data.csv",
            "geom_type": "point",
            "column_mapping": {"col1": "field1"},
            "plugin": None
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline",
        "SPECIFICATION_DIR": "/tmp/specification",
        "CACHE_DIR": "/tmp/cache"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-001"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/data.csv"
    request_schema.params.geom_type = "point"
    request_schema.params.column_mapping = {"col1": "field1"}
    request_schema.params.plugin = None

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: "test-file.csv")
    monkeypatch.setattr(tasks.workflow, "run_workflow", lambda *a, **kw: {
        "converted-csv": [{"col1": "val1"}],
        "issue-log": [],
        "column-field-log": {},
        "error-summary": {},
        "transformed-csv": [{"field1": "val1"}]
    })
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = check_dataurl(request, directories_json)

    assert result["id"] == "req-001"
    assert result["status"] == "COMPLETE"


def test_check_dataurl_with_default_directories(monkeypatch):
    """Test check_dataurl with default directories (None)"""
    request = {
        "id": "req-002",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/data.csv"
        }
    }

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-002"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/data.csv"
    request_schema.params.geom_type = ""
    request_schema.params.column_mapping = {}
    request_schema.params.plugin = None

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: "test-file.csv")
    monkeypatch.setattr(tasks.workflow, "run_workflow", lambda *a, **kw: {"result": "ok"})
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = check_dataurl(request, None)

    assert result["id"] == "req-002"
    assert result["status"] == "COMPLETE"


def test_check_dataurl_already_complete(monkeypatch):
    """Test check_dataurl when request status is already COMPLETE"""
    request = {
        "id": "req-003",
        "status": "COMPLETE",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/data.csv"
        }
    }

    request_schema = MagicMock()
    request_schema.status = "COMPLETE"
    request_schema.id = "req-003"
    request_schema.params = MagicMock()

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = check_dataurl(request, None)

    assert result["id"] == "req-003"
    assert result["status"] == "COMPLETE"


def test_check_dataurl_fetch_resource_fails(monkeypatch):
    """Test check_dataurl when _fetch_resource raises CustomException"""
    request = {
        "id": "req-004",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/invalid.csv"
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-004"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/invalid.csv"
    request_schema.params.plugin = None

    def raise_custom_exception(*a, **kw):
        error_detail = {
            "errMsg": "URL fetch failed",
            "errCode": "404",
            "errType": "FetchError"
        }
        raise CustomException(error_detail)

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", raise_custom_exception)
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "FAILED"})

    result = check_dataurl(request, directories_json)

    assert result["id"] == "req-004"
    assert result["status"] == "FAILED"


def test_check_dataurl_no_file_fetched(monkeypatch):
    """Test check_dataurl when _fetch_resource returns empty string"""
    request = {
        "id": "req-005",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/data.csv"
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-005"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/data.csv"
    request_schema.params.plugin = None

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: "")
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "NEW"})

    result = check_dataurl(request, directories_json)

    assert result["id"] == "req-005"


def test_check_dataurl_with_plugin(monkeypatch):
    """Test check_dataurl with plugin parameter"""
    request = {
        "id": "req-006",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/arcgis/rest/services",
            "plugin": "arcgis"
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-006"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/arcgis/rest/services"
    request_schema.params.plugin = "arcgis"
    request_schema.params.geom_type = ""
    request_schema.params.column_mapping = {}

    fetch_calls = []

    def mock_fetch_resource(collection_dir, resource_dir, log_dir, url, plugin):
        fetch_calls.append({
            "url": url,
            "plugin": plugin
        })
        return "test-file.csv"

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", mock_fetch_resource)
    monkeypatch.setattr(tasks.workflow, "run_workflow", lambda *a, **kw: {"result": "ok"})
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = check_dataurl(request, directories_json)

    assert result["id"] == "req-006"
    assert len(fetch_calls) == 1
    assert fetch_calls[0]["plugin"] == "arcgis"
    assert fetch_calls[0]["url"] == "http://example.com/arcgis/rest/services"


def test_check_dataurl_without_geom_type_and_column_mapping(monkeypatch):
    """Test check_dataurl without optional geom_type and column_mapping"""
    request = {
        "id": "req-007",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/data.csv"
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-007"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/data.csv"
    request_schema.params.plugin = None

    del request_schema.params.geom_type
    del request_schema.params.column_mapping

    workflow_calls = []

    def mock_run_workflow(file_name, req_id, collection, dataset, org, geom_type, column_mapping, directories):
        workflow_calls.append({
            "geom_type": geom_type,
            "column_mapping": column_mapping
        })
        return {"result": "ok"}

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: "test-file.csv")
    monkeypatch.setattr(tasks.workflow, "run_workflow", mock_run_workflow)
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = check_dataurl(request, directories_json)

    assert result["id"] == "req-007"
    assert len(workflow_calls) == 1
    assert workflow_calls[0]["geom_type"] == ""
    assert workflow_calls[0]["column_mapping"] == {}


def test_check_dataurl_custom_exception_with_missing_keys(monkeypatch):
    """Test check_dataurl when CustomException has missing error keys"""
    request = {
        "id": "req-008",
        "status": "NEW",
        "params": {
            "collection": "test-collection",
            "dataset": "test-dataset",
            "url": "http://example.com/data.csv"
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-008"
    request_schema.params = MagicMock()
    request_schema.params.collection = "test-collection"
    request_schema.params.dataset = "test-dataset"
    request_schema.params.url = "http://example.com/data.csv"
    request_schema.params.plugin = None

    def raise_custom_exception_minimal(*a, **kw):
        error_detail = {}
        raise CustomException(error_detail)

    save_calls = []

    def mock_save_response(req_id, response_data):
        save_calls.append(response_data)

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", raise_custom_exception_minimal)
    monkeypatch.setattr(tasks, "save_response_to_db", mock_save_response)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "FAILED"})

    result = check_dataurl(request, directories_json)

    assert result["id"] == "req-008"
    assert len(save_calls) == 1
    assert save_calls[0]["message"] == "An error occurred"
    assert save_calls[0]["status"] == "ERROR"
    assert "exception_type" in save_calls[0]


def test_check_dataurl_workflow_called_with_correct_params(monkeypatch):
    """Test that run_workflow is called with correct parameters"""
    request = {
        "id": "req-009",
        "status": "NEW",
        "params": {
            "collection": "brownfield-land",
            "dataset": "brownfield-land",
            "url": "http://example.com/data.csv",
            "geom_type": "polygon",
            "column_mapping": {"SiteReference": "reference"}
        }
    }

    directories_dict = {
        "COLLECTION_DIR": "/tmp/collection",
        "PIPELINE_DIR": "/tmp/pipeline",
        "SPECIFICATION_DIR": "/tmp/specification",
        "CACHE_DIR": "/tmp/cache"
    }
    directories_json = json.dumps(directories_dict)

    request_schema = MagicMock()
    request_schema.status = "NEW"
    request_schema.id = "req-009"
    request_schema.params = MagicMock()
    request_schema.params.collection = "brownfield-land"
    request_schema.params.dataset = "brownfield-land"
    request_schema.params.url = "http://example.com/data.csv"
    request_schema.params.geom_type = "polygon"
    request_schema.params.column_mapping = {"SiteReference": "reference"}
    request_schema.params.plugin = None

    workflow_calls = []

    def mock_run_workflow(file_name, req_id, collection, dataset, org, geom_type, column_mapping, directories):
        workflow_calls.append({
            "file_name": file_name,
            "req_id": req_id,
            "collection": collection,
            "dataset": dataset,
            "org": org,
            "geom_type": geom_type,
            "column_mapping": column_mapping
        })
        return {"result": "ok"}

    monkeypatch.setattr(tasks.schemas.Request, "model_validate", lambda r: request_schema)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: "brownfield-data.csv")
    monkeypatch.setattr(tasks.workflow, "run_workflow", mock_run_workflow)
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a, **kw: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: {"id": rid, "status": "COMPLETE"})

    result = check_dataurl(request, directories_json)

    assert len(workflow_calls) == 1
    assert workflow_calls[0]["file_name"] == "brownfield-data.csv"
    assert workflow_calls[0]["req_id"] == "req-009"
    assert workflow_calls[0]["collection"] == "brownfield-land"
    assert workflow_calls[0]["dataset"] == "brownfield-land"
    assert workflow_calls[0]["org"] == ""
    assert workflow_calls[0]["geom_type"] == "polygon"
    assert workflow_calls[0]["column_mapping"] == {"SiteReference": "reference"}


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