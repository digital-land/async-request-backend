import re
import uuid
from threading import Barrier
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, select, text, event
from sqlalchemy.orm import Session
from testcontainers.postgres import PostgresContainer
from alembic import command
from alembic.config import Config

from main import app, _get_db
from request_model import models


@pytest.fixture(scope="module")
def add_data_submission_engine():
    # A dedicated database, independent of the API's cached production engine.
    with PostgresContainer("postgres:16.2-alpine") as postgres:
        url = postgres.get_connection_url()
        config = Config("alembic.ini")
        with patch.dict("os.environ", {"DATABASE_URL": url}):
            command.upgrade(config, "head")
        engine = create_engine(url)
        yield engine
        engine.dispose()


@pytest.fixture
def api(add_data_submission_engine, auth_headers):
    def get_db():
        with Session(add_data_submission_engine) as session:
            yield session

    with patch.dict(app.dependency_overrides, {_get_db: get_db}):
        yield TestClient(app, headers=auth_headers())


@pytest.fixture
def origin(add_data_submission_engine):
    with Session(add_data_submission_engine) as session:
        request = models.Request(
            type="check_url",
            status="COMPLETE",
            params={
                "type": "check_url",
                "dataset": "tree",
                "collection": "tpo",
                "url": "https://example.org/" + uuid.uuid4().hex,
                "organisationName": "local-authority:ABC",
                "geom_type": "point",
            },
            response=models.Response(
                data={"datasets-in-resource": ["tree", "tree-preservation-zone"]}
            ),
        )
        session.add(request)
        session.commit()
        return {
            "id": request.id,
            "params": request.params,
            "modified": request.modified,
        }


def payload(origin, **overrides):
    return {
        "check_request_id": origin["id"],
        "dataset": "tree",
        "contact_name": "Test Person",
        "contact_email": "person@example.org",
        "documentation_url": "https://example.org/docs",
        **overrides,
    }


def test_create_read_and_idempotency_without_leaking_contacts(
    api, origin, add_data_submission_engine
):
    response = api.post(
        "/add-data-submissions",
        json=payload(origin, jira_issue_key="DATA-123"),
        headers={"Authorization": ""},
    )
    assert response.status_code == 201, response.text
    receipt = response.json()
    assert set(receipt) == {"id", "reference"}
    add_data_submission = api.get("/add-data-submissions/" + receipt["id"]).json()
    assert re.fullmatch(
        r"PDP-[23456789ABCDEFGHJKMNPQRSTUVWXYZ]{6}", add_data_submission["reference"]
    )
    assert add_data_submission["status"] == "TO_BE_ADDED"
    assert add_data_submission["assignee"] is None
    assert add_data_submission["organisation"] == "local-authority:ABC"
    assert add_data_submission["organisation_name"] is None
    assert add_data_submission["endpoint_url"] == origin["params"]["url"]
    assert add_data_submission["collection"] == "tpo"
    assert add_data_submission["geom_type"] == "point"
    assert add_data_submission["jira_issue_key"] == "DATA-123"
    assert (
        add_data_submission["origin_request_id"]
        == add_data_submission["check_request_id"]
        == origin["id"]
    )
    assert add_data_submission["preview_request_id"] is None
    assert len(add_data_submission["events"]) == 1
    assert add_data_submission["events"][0]["to_status"] == "TO_BE_ADDED"
    assert (
        api.get(f'/add-data-submissions/{add_data_submission["id"]}').json()
        == add_data_submission
    )
    assert (
        api.get(
            "/add-data-submissions/by-reference/"
            + add_data_submission["reference"].lower()
        ).json()
        == add_data_submission
    )
    again = api.post(
        "/add-data-submissions",
        json=payload(origin, contact_name="Changed"),
        headers={"Authorization": ""},
    )
    assert again.status_code == 200
    assert again.json() == receipt
    assert (
        api.get("/add-data-submissions/" + receipt["id"]).json() == add_data_submission
    )
    public = api.get("/requests/" + origin["id"])
    assert public.status_code == 200
    assert "person@example.org" not in public.text
    assert "Test Person" not in public.text
    assert "contact_email" not in public.text
    with Session(add_data_submission_engine) as session:
        request = session.get(models.Request, origin["id"])
        assert request.add_data_submission_id is None
        assert request.modified == origin["modified"]


def test_public_check_tool_creation_omits_restricted_fields(api, origin):
    body = payload(origin)
    assert "source" not in body
    assert "jira_issue_key" not in body
    response = api.post(
        "/add-data-submissions", json=body, headers={"Authorization": ""}
    )
    assert response.status_code == 201
    assert set(response.json()) == {"id", "reference"}


@pytest.mark.parametrize(
    "field,value",
    [("type", "check_file"), ("status", "PROCESSING"), ("status", "FAILED")],
)
def test_rejects_ineligible_origin(
    api, origin, add_data_submission_engine, field, value
):
    with Session(add_data_submission_engine) as session:
        setattr(session.get(models.Request, origin["id"]), field, value)
        session.commit()
    assert api.post("/add-data-submissions", json=payload(origin)).status_code == 422


def test_dataset_selection_and_untrusted_metadata(api, origin):
    assert (
        api.post(
            "/add-data-submissions", json=payload(origin, dataset="invented")
        ).status_code
        == 422
    )
    assert (
        api.post(
            "/add-data-submissions", json=payload(origin, organisation="spoofed")
        ).status_code
        == 422
    )
    first = api.post("/add-data-submissions", json=payload(origin)).json()
    other = api.post(
        "/add-data-submissions", json=payload(origin, dataset="tree-preservation-zone")
    )
    assert other.status_code == 201
    assert other.json()["id"] != first["id"]
    assert (
        api.get("/add-data-submissions/" + other.json()["id"]).json()[
            "origin_request_id"
        ]
        == origin["id"]
    )
    assert (
        api.get("/add-data-submissions/" + first["id"]).json()["origin_request_id"]
        == origin["id"]
    )


def test_active_duplicate_returns_reference_but_terminal_allows_new(
    api, origin, add_data_submission_engine
):
    first = api.post("/add-data-submissions", json=payload(origin)).json()
    with Session(add_data_submission_engine) as session:
        second = models.Request(
            type="check_url", status="COMPLETE", params=origin["params"]
        )
        session.add(second)
        session.commit()
        second_id = second.id
    result = api.post(
        "/add-data-submissions", json=payload(origin, check_request_id=second_id)
    )
    assert result.status_code == 409
    assert result.json()["detail"]["reference"] == first["reference"]
    with Session(add_data_submission_engine) as session:
        session.get(models.AddDataSubmission, first["id"]).status = "CLOSED"
        session.commit()
    result = api.post(
        "/add-data-submissions", json=payload(origin, check_request_id=second_id)
    )
    assert result.status_code == 201
    assert result.json()["reference"] != first["reference"]


def test_concurrent_idempotency(api, origin):
    from add_data_submissions import generate_reference

    barrier = Barrier(2)

    def simultaneous_reference():
        barrier.wait(timeout=10)
        return generate_reference()

    with patch(
        "add_data_submissions.generate_reference", side_effect=simultaneous_reference
    ), ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda _: api.post("/add-data-submissions", json=payload(origin)),
                range(2),
            )
        )
    assert sorted(result.status_code for result in results) == [200, 201]
    assert results[0].json()["id"] == results[1].json()["id"]
    assert (
        len(
            api.get("/add-data-submissions/" + results[0].json()["id"]).json()["events"]
        )
        == 1
    )


def test_concurrent_active_duplicates(api, origin, add_data_submission_engine):
    from add_data_submissions import generate_reference

    with Session(add_data_submission_engine) as session:
        other = models.Request(
            type="check_url", status="COMPLETE", params=origin["params"]
        )
        session.add(other)
        session.commit()
        ids = [origin["id"], other.id]
    barrier = Barrier(2)

    def simultaneous_reference():
        barrier.wait(timeout=10)
        return generate_reference()

    with patch(
        "add_data_submissions.generate_reference", side_effect=simultaneous_reference
    ), ThreadPoolExecutor(max_workers=2) as executor:
        results = list(
            executor.map(
                lambda request_id: api.post(
                    "/add-data-submissions",
                    json=payload(origin, check_request_id=request_id),
                ),
                ids,
            )
        )
    assert sorted(result.status_code for result in results) == [201, 409]
    winner = next(result.json() for result in results if result.status_code == 201)
    loser = next(result.json() for result in results if result.status_code == 409)
    assert loser["detail"]["reference"] == winner["reference"]


def test_pointer_update_failure_rolls_back_new_request(
    api, origin, add_data_submission_engine
):
    add_data_submission = api.post("/add-data-submissions", json=payload(origin)).json()
    with Session(add_data_submission_engine) as session:
        before = session.query(models.Request).count()

    def fail_pointer_update(
        connection, cursor, statement, parameters, context, executemany
    ):
        if statement.startswith("UPDATE add_data_submission SET"):
            raise RuntimeError("Simulated pointer update failure")

    event.listen(
        add_data_submission_engine, "before_cursor_execute", fail_pointer_update
    )
    try:
        with pytest.raises(RuntimeError, match="Simulated pointer"):
            api.post(
                "/requests",
                json={
                    "params": origin["params"],
                    "add_data_submission_id": add_data_submission["id"],
                },
            )
    finally:
        event.remove(
            add_data_submission_engine, "before_cursor_execute", fail_pointer_update
        )
    with Session(add_data_submission_engine) as session:
        assert session.query(models.Request).count() == before
        assert (
            session.get(
                models.AddDataSubmission, add_data_submission["id"]
            ).check_request_id
            == origin["id"]
        )


def test_linked_file_missing_and_terminal_add_data_submission(
    api, origin, add_data_submission_engine
):
    assert (
        api.post(
            "/requests",
            json={"params": origin["params"], "add_data_submission_id": "missing"},
        ).status_code
        == 404
    )
    add_data_submission = api.post("/add-data-submissions", json=payload(origin)).json()
    params = {
        "type": "check_file",
        "dataset": "tree",
        "collection": "tpo",
        "original_filename": "trees.csv",
        "uploaded_filename": "upload.csv",
    }
    assert (
        api.post(
            "/requests",
            json={
                "params": params,
                "add_data_submission_id": add_data_submission["id"],
            },
        ).status_code
        == 422
    )
    with Session(add_data_submission_engine) as session:
        session.get(
            models.AddDataSubmission, add_data_submission["id"]
        ).status = "ADDED"
        session.commit()
    assert (
        api.post(
            "/requests",
            json={
                "params": origin["params"],
                "add_data_submission_id": add_data_submission["id"],
            },
        ).status_code
        == 409
    )


def test_missing_origin_organisation_rejected(api, origin, add_data_submission_engine):
    with Session(add_data_submission_engine) as session:
        request = session.get(models.Request, origin["id"])
        request.params = {**request.params, "organisationName": None}
        session.commit()
    assert api.post("/add-data-submissions", json=payload(origin)).status_code == 422


def test_migration_preserves_old_requests_through_upgrade_and_downgrade():
    with PostgresContainer("postgres:16.2-alpine") as postgres:
        url = postgres.get_connection_url()
        engine = create_engine(url)
        config = Config("alembic.ini")
        with patch.dict("os.environ", {"DATABASE_URL": url}):
            command.upgrade(config, "d45c986e2727")
            with engine.begin() as connection:
                connection.execute(
                    text(
                        "INSERT INTO request (id, type, status, params) VALUES ('old', 'check_url', 'COMPLETE', '{}')"
                    )
                )
                before = connection.execute(
                    text("SELECT created, modified FROM request WHERE id = 'old'")
                ).one()
            command.upgrade(config, "head")
            with engine.connect() as connection:
                row = connection.execute(
                    text(
                        "SELECT created, modified, add_data_submission_id FROM request WHERE id = 'old'"
                    )
                ).one()
                assert tuple(row[:2]) == tuple(before)
                assert row.add_data_submission_id is None
            command.downgrade(config, "d45c986e2727")
            with engine.connect() as connection:
                assert (
                    connection.execute(
                        text("SELECT status FROM request WHERE id = 'old'")
                    ).scalar_one()
                    == "COMPLETE"
                )
            command.upgrade(config, "head")
        engine.dispose()


def test_reference_collision_retries(api, origin, add_data_submission_engine):
    api.post("/add-data-submissions", json=payload(origin))
    with Session(add_data_submission_engine) as session:
        existing = session.scalar(select(models.AddDataSubmission.reference).limit(1))
    with patch(
        "add_data_submissions.generate_reference", side_effect=[existing, "PDP-222222"]
    ):
        result = api.post(
            "/add-data-submissions",
            json=payload(origin, dataset="tree-preservation-zone"),
        )
    assert result.status_code == 201
    assert result.json()["reference"] == "PDP-222222"


def test_linked_requests_update_pointers_and_keep_payload_private(api, origin):
    add_data_submission = api.post("/add-data-submissions", json=payload(origin)).json()
    for kind, pointer, task in [
        ("check_url", "check_request_id", "CheckDataUrlTask"),
        ("add_data", "preview_request_id", "AddDataTask"),
    ]:
        params = {**origin["params"], "type": kind}
        with patch(f"main.{task}.apply_async") as queued:
            response = api.post(
                "/requests",
                json={
                    "params": params,
                    "add_data_submission_id": add_data_submission["id"],
                },
            )
        assert response.status_code == 202, response.text
        assert response.json()["add_data_submission_id"] == add_data_submission["id"]
        assert (
            api.get("/add-data-submissions/" + add_data_submission["id"]).json()[
                pointer
            ]
            == response.json()["id"]
        )
        assert "contact_email" not in str(queued.call_args)
        assert "person@example.org" not in str(queued.call_args)


def test_invalid_link_does_not_create_request_or_change_pointer(
    api, origin, add_data_submission_engine
):
    add_data_submission = api.post("/add-data-submissions", json=payload(origin)).json()
    with Session(add_data_submission_engine) as session:
        before = session.query(models.Request).count()
    for changes in [
        {"url": "https://different.org"},
        {"dataset": "wrong"},
        {"organisationName": "local-authority:OTHER"},
    ]:
        response = api.post(
            "/requests",
            json={
                "add_data_submission_id": add_data_submission["id"],
                "params": {**origin["params"], **changes},
            },
        )
        assert response.status_code == 422
    with Session(add_data_submission_engine) as session:
        assert session.query(models.Request).count() == before
    assert (
        api.get("/add-data-submissions/" + add_data_submission["id"]).json()[
            "check_request_id"
        ]
        == origin["id"]
    )


def test_manage_contacts_and_missing_records(api, origin):
    response = api.post(
        "/add-data-submissions",
        json=payload(origin, source="manage", contact_name=None, contact_email=None),
    )
    assert response.status_code == 201
    assert (
        api.get("/add-data-submissions/" + response.json()["id"]).json()["source"]
        == "manage"
    )
    assert api.get("/add-data-submissions/missing").status_code == 404
    assert api.get("/add-data-submissions/by-reference/PDP-999999").status_code == 404
    assert (
        api.post(
            "/add-data-submissions", json=payload(origin, check_request_id="missing")
        ).status_code
        == 404
    )


@pytest.mark.parametrize(
    "changes",
    [
        {"contact_email": "invalid"},
        {"contact_name": " "},
        {"documentation_url": "file:///etc/passwd"},
        {"contact_email": None},
    ],
)
def test_invalid_contact_and_documentation(api, origin, changes):
    assert (
        api.post("/add-data-submissions", json=payload(origin, **changes)).status_code
        == 422
    )
