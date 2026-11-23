import sentry_sdk

from application.logging.monitoring import (
    SECURITY_REJECTED_METRIC,
    count_security_rejection,
)


def test_counts_rejection_with_low_cardinality_attributes(monkeypatch):
    calls = []
    monkeypatch.setattr(
        sentry_sdk.metrics,
        "count",
        lambda *args, **kwargs: calls.append((args, kwargs)),
    )
    count_security_rejection("content", "upload")
    assert calls == [
        (
            (SECURITY_REJECTED_METRIC, 1),
            {
                "attributes": {
                    "service": "request-processor",
                    "layer": "content",
                    "source": "upload",
                }
            },
        )
    ]
