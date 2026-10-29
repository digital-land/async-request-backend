"""Filter configured fields from structured and JSON-encoded Sentry context."""

import json
import re

from sentry_sdk.scrubber import DEFAULT_DENYLIST, EventScrubber

EMAIL_PATTERN = re.compile(
    r"[a-zA-Z0-9.!#$%&'*+/=?^_`{|}~-]+@"
    r"[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?"
    r"(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]*[a-zA-Z0-9])?)+"
)


def _redact_emails(value):
    """Redact conventional email addresses in text, including nested event values."""
    if isinstance(value, str):
        return EMAIL_PATTERN.sub("[Filtered]", value)
    if isinstance(value, bytes):
        return re.sub(EMAIL_PATTERN.pattern.encode(), b"[Filtered]", value)
    if isinstance(value, dict):
        for key, child in value.items():
            value[key] = _redact_emails(child)
    elif isinstance(value, list):
        for index, child in enumerate(value):
            value[index] = _redact_emails(child)
    elif isinstance(value, tuple):
        return tuple(_redact_emails(child) for child in value)
    return value


def _filter_json_fields(value, fields):
    """Redact named fields in a decoded JSON value; retain unrelated fields."""
    changed = False
    if isinstance(value, dict):
        for key, child in value.items():
            if key.lower() in fields:
                value[key] = "[Filtered]"
                changed = True
            else:
                changed = _filter_json_fields(child, fields) or changed
    elif isinstance(value, list):
        for child in value:
            changed = _filter_json_fields(child, fields) or changed
    return changed


def _redacted_json(value, fields):
    if not isinstance(value, (str, bytes)):
        return None
    try:
        decoded = json.loads(value)
    except (ValueError, UnicodeDecodeError):
        return None
    if _filter_json_fields(decoded, fields):
        redacted = json.dumps(decoded)
        return redacted.encode() if isinstance(value, bytes) else redacted
    return None


class SensitiveFieldScrubber(EventScrubber):
    """Extend Sentry's default denylist with application-specific field names."""

    def __init__(self, fields=()):
        self.fields = frozenset(field.lower() for field in [*DEFAULT_DENYLIST, *fields])
        super().__init__(denylist=list(self.fields), recursive=True)

    def safe_repr(self, value):
        """Filter raw JSON locals before the SDK serialises/truncates their repr."""
        redacted = _redacted_json(value, self.fields)
        if isinstance(value, (str, bytes)):
            redacted = _redact_emails(value if redacted is None else redacted)
            return repr(redacted) if redacted != value else None
        return repr(redacted) if redacted is not None else None

    def scrub_event(self, event):
        request = event.get("request", {})
        body = request.get("data")
        super().scrub_event(event)
        # Sentry's default scrubber does not visit message/exception text.
        _redact_emails(event)
        if isinstance(body, (str, bytes)):
            try:
                json.loads(body)
            except (ValueError, UnicodeDecodeError):
                # Raw request bodies are outside JSON field redaction.
                request["data"] = body
            else:
                redacted = _redacted_json(body, self.fields)
                request["data"] = _redact_emails(body if redacted is None else redacted)

    def scrub_log(self, log, hint):
        """Logs bypass event scrubbing; keep their values JSON-serialisable."""
        attributes = log.get("attributes", {})
        for key, value in attributes.items():
            field = key.removeprefix("sentry.message.parameter.").lower()
            if field in self.fields:
                # The SDK may already have interpolated these attributes into body.
                if (
                    isinstance(value, str)
                    and value
                    and isinstance(log.get("body"), str)
                ):
                    log["body"] = log["body"].replace(value, "[Filtered]")
                attributes[key] = "[Filtered]"
        _filter_json_fields(log, self.fields)
        body = _redacted_json(log.get("body"), self.fields)
        if body is not None:
            log["body"] = body
        return _redact_emails(log)

    def scrub_dict(self, values):
        super().scrub_dict(values)
        if not isinstance(values, dict):
            return
        # Request data and extra context may still contain JSON strings/bytes.
        # Frame locals are additionally protected by safe_repr.
        for key, value in values.items():
            redacted = _redacted_json(value, self.fields)
            if redacted is not None:
                values[key] = redacted
