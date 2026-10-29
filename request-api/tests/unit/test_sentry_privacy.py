import ast
import json

import pytest
from sentry_sdk.serializer import serialize

from sentry_privacy import SensitiveFieldScrubber


@pytest.mark.parametrize("as_bytes", [False, True])
def test_configured_and_default_fields_are_filtered_in_json(as_bytes):
    scrubber = SensitiveFieldScrubber(fields={"Private_Note"})
    value = json.dumps(
        {"items": [{"PRIVATE_NOTE": "secret", "password": "secret", "dataset": "tree"}]}
    )
    if as_bytes:
        value = value.encode()
    expected = {
        "items": [
            {"PRIVATE_NOTE": "[Filtered]", "password": "[Filtered]", "dataset": "tree"}
        ]
    }
    assert json.loads(ast.literal_eval(scrubber.safe_repr(value))) == expected
    context = {"body": value}
    scrubber.scrub_dict(context)
    assert json.loads(context["body"]) == expected


def test_field_configuration_is_isolated_and_preserves_other_context():
    scrubber = SensitiveFieldScrubber(fields={"private_note"})
    context = {"nested": [{"private_note": "secret", "dataset": "tree"}]}
    scrubber.scrub_dict(context)
    assert serialize(context)["nested"] == [
        {"private_note": "[Filtered]", "dataset": "tree"}
    ]
    value = '{"private_note": "visible"}'
    assert SensitiveFieldScrubber().safe_repr(value) is None
    assert scrubber.safe_repr("ordinary diagnostic") is None


@pytest.mark.parametrize("as_bytes", [False, True])
def test_email_addresses_in_text_and_json_locals(as_bytes):
    scrubber = SensitiveFieldScrubber(fields={"email"})
    text = "Failed for Person+check@Example.GOV.UK and second@example.org; dataset=tree"
    value = text.encode() if as_bytes else text
    result = ast.literal_eval(scrubber.safe_repr(value))
    if as_bytes:
        result = result.decode()
    assert result == "Failed for [Filtered] and [Filtered]; dataset=tree"
    value = json.dumps({"email": "not-an-address", "note": text})
    redacted = json.loads(ast.literal_eval(scrubber.safe_repr(value)))
    assert redacted["email"] == "[Filtered]"
    assert "@" not in redacted["note"]


def test_email_detection_covers_event_messages_and_nested_lists():
    scrubber = SensitiveFieldScrubber(fields={"email"})
    event = {
        "message": "Unable to notify user@example.org",
        "exception": {"values": [{"value": "Recipient user@example.org failed"}]},
        "extra": {"items": [["user@example.org", "tree"]], "EMAIL": "invalid"},
        "breadcrumbs": {"values": [{"message": "Sent to user@example.org"}]},
    }
    scrubber.scrub_event(event)
    result = serialize(event)
    assert "user@example.org" not in json.dumps(result)
    assert result["message"] == "Unable to notify [Filtered]"
    assert result["extra"]["EMAIL"] == "[Filtered]"
    assert result["extra"]["items"] == [["[Filtered]", "tree"]]


@pytest.mark.parametrize(
    "text", ["dataset=tree", "https://example.org/path", "@mention"]
)
def test_non_email_text_is_preserved(text):
    assert SensitiveFieldScrubber().safe_repr(text) is None
