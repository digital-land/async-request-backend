from unittest.mock import Mock, patch
from types import SimpleNamespace

import pytest
from fastapi.testclient import TestClient

import main


@pytest.mark.parametrize("creation_type", ["check_tool", "manage", "jira"])
def test_creation_is_public(creation_type):
    payload = {
        "check_request_id": "check",
        "dataset": "tree",
        "documentation_url": "https://example.org/docs",
    }
    if creation_type == "manage":
        payload["source"] = "manage"
    else:
        payload.update(
            contact_name="Person",
            contact_email="person@example.org",
        )
        if creation_type == "jira":
            payload["jira_issue_key"] = "DATA-123"
    with patch.dict(
        main.app.dependency_overrides, {main._get_db: lambda: Mock()}
    ), patch(
        "main.add_data_submissions.create_add_data_submission",
        return_value=(SimpleNamespace(id="id", reference="PDP-123456"), True),
    ) as create:
        response = TestClient(main.app).post(
            "/add-data-submissions",
            json=payload,
        )
    assert response.status_code == 201
    assert response.json() == {"id": "id", "reference": "PDP-123456"}
    create.assert_called_once()


@pytest.mark.parametrize(
    "path",
    [
        "/add-data-submissions/test-id",
        "/add-data-submissions/by-reference/PDP-7K3F9Q",
    ],
)
@pytest.mark.parametrize(
    "authentication,status", [("missing", 401), ("invalid", 401), ("viewer", 403)]
)
def test_private_read_routes_require_staff(path, authentication, status, auth_headers):
    headers = {} if authentication == "missing" else {"Authorization": "Bearer invalid"}
    if authentication == "viewer":
        headers = auth_headers("viewer")
    db = Mock()
    with patch.dict(main.app.dependency_overrides, {main._get_db: lambda: db}):
        response = TestClient(main.app, raise_server_exceptions=False).get(
            path, headers=headers
        )
    assert response.status_code == status
    db.get.assert_not_called()
    db.scalar.assert_not_called()


@pytest.mark.parametrize("role", ["staff", "admin"])
@pytest.mark.parametrize(
    "path",
    [
        "/add-data-submissions/missing",
        "/add-data-submissions/by-reference/PDP-7K3F9Q",
    ],
)
def test_authorised_reads_reach_the_handler(role, path, auth_headers):
    db = Mock()
    db.get.return_value = None
    db.scalar.return_value = None
    with patch.dict(main.app.dependency_overrides, {main._get_db: lambda: db}):
        response = TestClient(main.app).get(path, headers=auth_headers(role))
    assert response.status_code == 404


@pytest.mark.parametrize("kind", ["check_url", "check_file", "add_data"])
@pytest.mark.parametrize("service", [None, "manage", "provide"])
def test_submission_link_requires_auth_before_persistence_or_dispatch(kind, service):
    params = {"type": kind, "dataset": "tree", "collection": "tpo", "service": service}
    if kind == "check_file":
        params.update(original_filename="trees.csv", uploaded_filename="uploaded.csv")
    else:
        params["url"] = "https://example.org/trees.csv"
    db = Mock()
    with patch.dict(main.app.dependency_overrides, {main._get_db: lambda: db}), patch(
        "main.CheckDataUrlTask.apply_async"
    ) as queued:
        response = TestClient(main.app, raise_server_exceptions=False).post(
            "/requests", json={"params": params, "add_data_submission_id": "test-id"}
        )
    assert response.status_code == 401
    db.scalar.assert_not_called()
    db.add.assert_not_called()
    db.commit.assert_not_called()
    queued.assert_not_called()
