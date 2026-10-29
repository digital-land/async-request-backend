import json
from unittest.mock import patch

import pytest
import sentry_sdk
from sentry_sdk import logger as sentry_logger
from fastapi.testclient import TestClient
from sentry_sdk.integrations.fastapi import FastApiIntegration
from sentry_sdk.integrations.starlette import StarletteIntegration
from sentry_sdk.transport import Transport

import main
from add_data_submission_schemas import AddDataSubmissionCreate


class MemoryTransport(Transport):
    def __init__(self):
        super().__init__()
        self.events = []
        self.logs = []

    def capture_envelope(self, envelope):
        for item in envelope.items:
            event = item.get_event()
            if event is not None:
                self.events.append(event)
            if item.headers.get("type") == "log":
                self.logs.extend(item.payload.json["items"])


@pytest.fixture
def captured_events():
    transport = MemoryTransport()
    with sentry_sdk.init(
        dsn="http://public@localhost/1",
        transport=transport,
        default_integrations=False,
        integrations=[
            StarletteIntegration(transaction_style="url"),
            FastApiIntegration(transaction_style="url"),
        ],
        event_scrubber=main.sentry_event_scrubber,
        custom_repr=main.sentry_event_scrubber.safe_repr,
    ):
        yield transport.events


def test_creation_exception_hides_contacts_but_keeps_debug_context(captured_events):
    def fail_creation(db, body):
        values = body.model_dump()
        # This mirrors the dictionary built by the persistence service before
        # an insert fails, as well as the Pydantic body in the route's frame.
        assert values["dataset"] == "tree"
        raise RuntimeError("Simulated database failure")

    with patch.dict(main.app.dependency_overrides, {main._get_db: lambda: None}), patch(
        "main.add_data_submissions.create_add_data_submission", fail_creation
    ):
        response = TestClient(main.app, raise_server_exceptions=False).post(
            "/add-data-submissions",
            json={
                "check_request_id": "privacy-check",
                "dataset": "tree",
                "documentation_url": "https://example.org/docs",
                "contact_name": "CONTACT_NAME_MARKER",
                "contact_email": "contact-marker@example.org",
            },
        )
    assert response.status_code == 500
    assert captured_events
    encoded = json.dumps(captured_events)
    assert "CONTACT_NAME_MARKER" not in encoded
    assert "contact-marker@example.org" not in encoded
    assert "privacy-check" in encoded
    assert "https://example.org/docs" in encoded
    assert "Simulated database failure" in encoded
    frames = captured_events[0]["exception"]["values"][0]["stacktrace"]["frames"]
    body_frame = next(
        frame for frame in frames if frame["function"] == "create_add_data_submission"
    )
    assert "dataset='tree'" in body_frame["vars"]["request"]


def test_structured_contacts_are_scrubbed_without_removing_other_fields(
    captured_events,
):
    sentry_sdk.capture_event(
        {
            "message": "Submission diagnostic",
            "request": {
                "data": {
                    "contact_name": "CONTACT_NAME_MARKER",
                    "contact_email": "contact-marker@example.org",
                    "dataset": "tree",
                }
            },
            "extra": {
                "details": [
                    {
                        "contact_name": "CONTACT_NAME_MARKER",
                        "contact_email": "contact-marker@example.org",
                        "reference": "PDP-7K3F9Q",
                        "password": "PASSWORD_MARKER",
                    }
                ]
            },
        }
    )
    assert captured_events
    encoded = json.dumps(captured_events)
    assert "CONTACT_NAME_MARKER" not in encoded
    assert "contact-marker@example.org" not in encoded
    assert "PASSWORD_MARKER" not in encoded
    assert captured_events[0]["request"]["data"]["dataset"] == "tree"
    assert captured_events[0]["extra"]["details"][0]["reference"] == "PDP-7K3F9Q"


def test_contacts_remain_available_for_persistence_and_api_serialisation():
    body = AddDataSubmissionCreate(
        check_request_id="privacy-check",
        dataset="tree",
        documentation_url="https://example.org/docs",
        contact_name="CONTACT_NAME_MARKER",
        contact_email="contact-marker@example.org",
    )
    assert body.model_dump()["contact_name"] == "CONTACT_NAME_MARKER"
    assert body.model_dump()["contact_email"] == "contact-marker@example.org"
    assert "CONTACT_NAME_MARKER" not in repr(body)
    assert "contact-marker@example.org" not in str(body)


def test_emails_are_redacted_from_captured_event_text(captured_events):
    sentry_sdk.capture_event(
        {
            "message": "Failed for other+test@example.org; dataset=tree",
            "extra": {"email": "invalid-address", "notes": ["other+test@example.org"]},
        }
    )
    event = captured_events[0]
    assert "other+test@example.org" not in json.dumps(event)
    assert event["message"] == "Failed for [Filtered]; dataset=tree"
    assert event["extra"]["email"] == "[Filtered]"


def test_log_transport_redacts_contacts_without_disabling_logs():
    transport = MemoryTransport()
    client = sentry_sdk.Client(
        dsn="http://public@localhost/1",
        transport=transport,
        default_integrations=False,
        enable_logs=True,
        before_send_log=main.sentry_event_scrubber.scrub_log,
    )
    try:
        with sentry_sdk.isolation_scope() as scope:
            scope.set_client(client)
            sentry_logger.info(
                "Submission for {contact_name}: {contact_email}",
                contact_name="PRIVATE_NAME_MARKER",
                contact_email="private@example.org",
                attributes={"contact_name": "PRIVATE_NAME_MARKER", "dataset": "tree"},
            )
            client.flush()
        assert transport.logs
        encoded = json.dumps(transport.logs)
        assert "PRIVATE_NAME_MARKER" not in encoded
        assert "private@example.org" not in encoded
        assert transport.logs[0]["body"] == "Submission for [Filtered]: [Filtered]"
        assert transport.logs[0]["attributes"]["dataset"]["value"] == "tree"
    finally:
        client.close()
