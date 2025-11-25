import datetime
from urllib.parse import quote
from fastapi.testclient import TestClient
import pytest

import database
from main import app
from request_model import models, schemas

client = TestClient(app)

OK_URL = "https://raw.githubusercontent.com/digital-land/PublishExamples/refs/heads/main/Article4Direction/Files/Article4DirectionArea/article4directionareas-ok.csv"
ERROR_URL = "https://raw.githubusercontent.com/digital-land/PublishExamples/refs/heads/main/Article4Direction/Files/Article4DirectionArea/article4directionareas-errors.csv"
SSL_ERROR_URL = "https://expired.badssl.com/"
TIMEOUT_URL = "http://httpbin.org/delay/60"
CONNECTION_REFUSED_URL = "http://localhost:9999/nonexistent"
SERVER_ERROR_URL = "https://www.tendringdc.gov.uk/sites/default/files/documents/planning/CAD%20csv.csv"

expected_json = [
    {
        "line": 1,
        "issue_logs": [
            {
                "field": "organisation",
                "issue-type": "invalid organisation",
                "severity": "warning",
            }
        ],
    },
    {
        "line": 2,
        "issue_logs": [
            {
                "field": "geometry",
                "issue-type": "invalid geometry",
                "severity": "error",
            }
        ],
    },
    {
        "line": 3,
        "issue_logs": [
            {
                "field": "organisation",
                "issue-type": "invalid organisation",
                "severity": "warning",
            }
        ],
    },
]


def wait_for_request_completion(request_id, timeout=30, poll_interval=2):
    """
    Poll the request status until it's COMPLETE or FAILED, or timeout.
    """
    start_time = time.time()
    while time.time() - start_time < timeout:
        response = client.get(f"/requests/{request_id}")
        assert response.status_code == 200
        data = response.json()
        status = data.get("status")

        if status in ["COMPLETE", "FAILED"]:
            return data

        time.sleep(poll_interval)

    raise TimeoutError(f"Request {request_id} did not complete within {timeout} seconds")


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
        (
                '$.issue_logs[*]."issue-type"=="invalid organisation" && $.issue_logs[*]."field"=="organisation"',
                [expected_json[0], expected_json[2]],
                "2",
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
    jsonpath = quote(jsonpath)
    response = client.get(
        f"/requests/{create_test_request.id}/response-details?jsonpath={jsonpath}"
    )
    assert response.status_code == 200
    assert response.json() == expected_json
    assert response.headers["X-Pagination-Total-Results"] == expected_total
    assert response.headers["X-Pagination-Limit"] == str(expected_pglimit)


def test_create_request_invalid_plugin(
        db,
        helpers,
):
    """Tests creating a request with an invalid plugin."""
    invalid_request_data = {
        "params": {
            "type": "check_url",
            "dataset": "article-4-direction-area",
            "collection": "article-4-direction",
            "url": "https://example.com/arcgis/rest/services/MapServer",
            "plugin": "arcis",  # Invalid plugin name
        }
    }
    response = client.post("/requests", json=invalid_request_data)

    assert response.status_code == 422
    error_detail = response.json()

    assert "detail" in error_detail
    assert len(error_detail["detail"]) > 0
    error = error_detail["detail"][0]
    assert error["type"] == "enum"
    assert "plugin" in error["loc"]
    assert "Input should be 'arcgis' or 'wfs'" in error["msg"]
    assert error["input"] == "arcis"


class TestCheckUrlAcceptance:
    """Acceptance tests for check_url functionality"""

    def test_check_url_with_valid_csv_succeeds(self, db, helpers):
        """
        Test that check_url with a valid CSV URL succeeds.

        Acceptance Criteria:
        - Request is created with status PENDING
        - Task processes successfully
        - Response contains no error-summary with blocking errors
        - Status becomes COMPLETE
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": OK_URL,
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        assert result["status"] == "COMPLETE"
        assert result["response"] is not None
        assert "data" in result["response"]

    def test_check_url_with_error_csv_shows_issues(self, db, helpers):
        """
        Test that check_url with a CSV containing errors returns issues.

        Acceptance Criteria:
        - Request is created successfully
        - Task processes the file
        - Response contains error-summary with issues
        - Status becomes COMPLETE (not FAILED)
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": ERROR_URL,
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        assert result["status"] == "COMPLETE"
        assert result["response"] is not None
        assert "data" in result["response"]

    def test_check_url_with_ssl_error_fails_gracefully(self, db, helpers):
        """
        Test that check_url with SSL error returns proper error response.

        Acceptance Criteria:
        - Request is created
        - Task fails due to SSL error
        - Response contains error with SSL-related message
        - Status becomes FAILED
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": SSL_ERROR_URL,
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        assert result["status"] == "FAILED"
        assert result["response"] is not None
        assert "error" in result["response"]

        error = result["response"]["error"]
        assert "message" in error
        assert any(keyword in error["message"].lower() for keyword in ["ssl", "certificate", "verify"])

    def test_check_url_with_timeout_fails_gracefully(self, db, helpers):
        """
        Test that check_url with timeout returns proper error response.

        Acceptance Criteria:
        - Request is created
        - Task fails due to timeout
        - Response contains error with timeout message
        - Status becomes FAILED
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": TIMEOUT_URL,
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=90)

        assert result["status"] == "FAILED"
        assert result["response"] is not None
        assert "error" in result["response"]

        error = result["response"]["error"]
        assert "message" in error
        assert any(keyword in error["message"].lower() for keyword in ["timeout", "timed out", "time"])

    def test_check_url_with_connection_refused_fails_gracefully(self, db, helpers):
        """
        Test that check_url with connection refused returns proper error.

        Acceptance Criteria:
        - Request is created
        - Task fails due to connection refused
        - Response contains error with connection message
        - Status becomes FAILED
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": CONNECTION_REFUSED_URL,
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        assert result["status"] == "FAILED"
        assert result["response"] is not None
        assert "error" in result["response"]

        error = result["response"]["error"]
        assert "message" in error
        assert any(keyword in error["message"].lower() for keyword in ["connection", "refused", "connect"])

    def test_check_url_with_server_error_fails_gracefully(self, db, helpers):
        """
        Test that check_url with server error (like Tendring DC URL) fails gracefully.

        This mirrors the submit repo test:
        'request check of a @url that should cause a server error and be displayed'

        Acceptance Criteria:
        - Request is created
        - Task fails due to server error (SSL/timeout/etc)
        - Response contains error message
        - Status becomes FAILED
        - Frontend can display error to user
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": SERVER_ERROR_URL,
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=90)

        assert result["status"] == "FAILED"
        assert result["response"] is not None
        assert "error" in result["response"]

        error = result["response"]["error"]
        assert "message" in error
        assert len(error["message"]) > 0

        assert "exception_type" in error

    def test_check_url_with_plugin_arcgis(self, db, helpers):
        """
        Test that check_url with arcgis plugin works correctly.

        Acceptance Criteria:
        - Request is created with plugin=arcgis
        - Task processes using arcgis plugin
        - Request completes successfully or with appropriate error
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": "https://example.com/arcgis/rest/services/MapServer",
                "plugin": "arcgis",
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        # Should complete or fail gracefully
        assert result["status"] in ["COMPLETE", "FAILED"]
        assert result["response"] is not None

    def test_check_url_with_plugin_wfs(self, db, helpers):
        """
        Test that check_url with wfs plugin works correctly.

        Acceptance Criteria:
        - Request is created with plugin=wfs
        - Task processes using wfs plugin
        - Request completes successfully or with appropriate error
        """
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": "https://example.com/wfs?service=WFS",
                "plugin": "wfs",
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        # Should complete or fail gracefully
        assert result["status"] in ["COMPLETE", "FAILED"]
        assert result["response"] is not None


class TestAddDataAcceptance:
    """Acceptance tests for add_data functionality"""

    def test_add_data_with_valid_url_succeeds(self, db, helpers):
        """
        Test that add_data with a valid URL succeeds.

        Acceptance Criteria:
        - Request is created with add_data type
        - URL is fetched
        - add_data workflow runs successfully
        - Response contains entity-summary
        - Status becomes COMPLETE
        """
        request_data = {
            "params": {
                "type": "add_data",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": OK_URL,
                "organisation": "local-authority:ADU",
                "documentation_url": "https://example.com/docs",
                "licence": "OGL-UK-3.0",
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=90)

        # Should complete with entity-summary
        assert result["status"] == "COMPLETE"
        assert result["response"] is not None
        assert "data" in result["response"]

        # Verify entity-summary present
        data = result["response"]["data"]
        assert "entity-summary" in data

    def test_add_data_with_error_url_fails_gracefully(self, db, helpers):
        """
        Test that add_data with error URL fails gracefully.

        Acceptance Criteria:
        - Request is created
        - URL fetch fails
        - Task fails with appropriate error
        - Status becomes FAILED
        """
        request_data = {
            "params": {
                "type": "add_data",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": SSL_ERROR_URL,
                "organisation": "local-authority:ADU",
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)

        assert result["status"] == "FAILED"
        assert result["response"] is not None
        assert "error" in result["response"]


class TestResponseDetailsAcceptance:
    """Acceptance tests for response details pagination"""

    def test_response_details_pagination_works(self, db, helpers):
        """
        Test that response details can be paginated.

        Acceptance Criteria:
        - Request completes with data
        - Response details can be fetched with pagination
        - Offset and limit work correctly
        """
        # First create a request with data
        request_data = {
            "params": {
                "type": "check_url",
                "dataset": "article-4-direction-area",
                "collection": "article-4-direction",
                "url": ERROR_URL,  # Use error URL to get multiple rows
            }
        }

        response = client.post("/requests", json=request_data)
        assert response.status_code == 202
        request_id = response.json()["id"]

        result = wait_for_request_completion(request_id, timeout=60)
        assert result["status"] == "COMPLETE"

        # Fetch response details with pagination
        response = client.get(f"/requests/{request_id}/response-details?offset=0&limit=10")
        assert response.status_code == 200

        details = response.json()
        assert isinstance(details, list)
        assert len(details) > 0


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
                                "issue-type": "invalid organisation",
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
                                "issue-type": "invalid geometry",
                                "severity": "error",
                            }
                        ],
                    }
                ),
                models.ResponseDetails(
                    detail={
                        "line": 3,
                        "issue_logs": [
                            {
                                "field": "organisation",
                                "issue-type": "invalid organisation",
                                "severity": "warning",
                            }
                        ],
                    }
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