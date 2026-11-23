"""
Sentry setup, metrics and error reporting for the request-processor worker.

Metric attributes are kept low-cardinality: no URLs, hosts, filenames or
request ids.
"""

import logging
import os

import sentry_sdk
from sentry_sdk.integrations.logging import ignore_logger
from sentry_sdk.scrubber import DEFAULT_DENYLIST, EventScrubber

logger = logging.getLogger(__name__)

SECURITY_REJECTED_METRIC = "async.security.rejected"
TASK_FAILURE_METRIC = "request_processor.task_failure"
TASK_REDELIVERED_METRIC = "async.task.redelivered"
CHECK_URL_SUCCESS_METRIC = "async.url_submission.success"
CHECK_URL_USER_ERROR_METRIC = "async.check_url.user_error"

# Field names Sentry redacts wherever they appear in an event, on top of its
# defaults (password, secret, token, ...). Covers the worker's own settings.
SENTRY_DENYLIST = [
    *DEFAULT_DENYLIST,
    "database_url",
    "celery_broker_url",
    "aws_access_key_id",
    "aws_secret_access_key",
    "aws_session_token",
    "aws_security_token",
]


# Setup


def sentry_privacy_options():
    """
    Keep secrets out of Sentry events. Frame local variables are not sent: the
    worker holds credentials and connection strings in locals, which field
    name scrubbing can't reliably catch.
    """
    return {
        "include_local_variables": False,
        "send_default_pii": False,
        "event_scrubber": EventScrubber(denylist=SENTRY_DENYLIST, recursive=True),
    }


def init_sentry():
    if os.environ.get("SENTRY_ENABLED", "false").lower() != "true":
        return
    # esridump logs ERROR-level on handled/expected failures (e.g. probing
    # non-ArcGIS URLs with the arcgis fallback plugin), which would otherwise
    # be captured as spurious Sentry issues.
    ignore_logger("esridump.dumper")
    sentry_sdk.init(
        enable_tracing=os.environ.get("SENTRY_TRACING_ENABLED", "false").lower()
        == "true",
        traces_sample_rate=float(os.environ.get("SENTRY_TRACING_SAMPLE_RATE", "0.01")),
        release=os.environ.get("GIT_COMMIT"),
        debug=os.environ.get("SENTRY_DEBUG", "false").lower() == "true",
        enable_logs=True,
        **sentry_privacy_options(),
    )


# Security


def count_security_rejection(layer, source):
    """
    Count one input refused for safety reasons. Every refusal is counted as the
    same metric so they can be graphed and alerted on together.

    layer: the check that refused it: "url" (the URL text), "connection" (the
        address a host resolves to, including after a redirect), "content"
        (a file that is unsafe to convert, such as a SQLite
        virtual table or a zip bomb), "size" (a fetched
        response larger than the worker will read), "message" (a task
        message that request-api could not have sent) or "params" (a real
        request whose stored params the worker will not use).
    source: where the input came from: "fetch" (a user-supplied URL),
        "upload" (a file uploaded through Submit) or "broker" (the task queue).
    """
    sentry_sdk.metrics.count(
        SECURITY_REJECTED_METRIC,
        1,
        attributes={"service": "request-processor", "layer": layer, "source": source},
    )


# Tasks


def count_task_failure(exception, task_name):
    """
    Count a failed task by its cause. Handled failures all raise
    CustomException, so use the cause it records.
    """
    cause = getattr(exception, "exception_type", None) or type(exception).__name__
    sentry_sdk.metrics.count(
        TASK_FAILURE_METRIC,
        1,
        attributes={"exception": str(cause), "task": task_name or "unknown"},
    )


def count_task_redelivered(task_name):
    """
    Count a message delivered again for a request that is already PROCESSING.
    Tasks are acknowledged late, so SQS redelivers when a worker was lost
    (killed, stopped, or past the visibility timeout).
    """
    sentry_sdk.metrics.count(
        TASK_REDELIVERED_METRIC, 1, attributes={"task": task_name or "unknown"}
    )


# check_url


def count_check_url_success():
    sentry_sdk.metrics.count(CHECK_URL_SUCCESS_METRIC, 1)


def record_check_url_user_error(error_log, request_id):
    """
    Record an expected check_url failure, such as an unreachable, refused or
    unsupported URL: a metric plus a warning log (Sentry Logs, not an Issue),
    since it isn't an application bug.

    error_log is CustomException.detail after create_user_friendly_error_log.
    """
    sentry_sdk.metrics.count(
        CHECK_URL_USER_ERROR_METRIC,
        1,
        attributes={
            "task": "CheckURL",
            "exception": error_log.get("exceptionType"),
            "status": error_log.get("errCode"),
            "plugin": error_log.get("plugin"),
        },
    )
    logger.warning(
        f"Check URL user error: {error_log.get('message', 'Error occurred')}",
        extra={"request_id": request_id, "task": "CheckURL", "error_log": error_log},
    )


def log_check_url_failure(stage, request_id):
    """
    Report an unexpected check_url failure. Call from an except block:
    logger.exception creates one Sentry Issue with the stack trace, grouped by
    the exception and code path rather than its message.
    """
    logger.exception(
        f"check_url {stage} failed",
        extra={"request_id": request_id, "task": "CheckURL", "stage": stage},
    )
