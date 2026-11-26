"""
Rejects task messages that request-api could not have sent.

Anyone who can write to the queue can send the worker a message. request-api
only ever enqueues args=[request] for a NEW request row it has just committed,
on the task matching the request type, so a message that differs from that was
put on the queue by something else.

A message that matches a stored request but carries params the worker will not
use (an unsafe URL, or a value that could escape its directory) is a real
request, not a forged one, so it raises UnsafeRequestParams instead: the request
is failed with an error rather than left untouched.

tasks.py wires this into the Celery tasks: it subclasses BrokerMessageGuard to
look requests up in the database.
"""

import re

from celery._state import get_current_task
from pydantic import ValidationError

import request_model.schemas as schemas
from request_model.url_safety import UnsafeUrlError, check_fetch_url

# request-api generates ids with shortuuid.uuid(): 22 alphanumeric characters.
REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9]{22}")
SLUG_PATTERN = re.compile(r"[a-z0-9-]+")
# digital-land resource ids are the sha256 hex digest of the content.
RESOURCE_PATTERN = re.compile(r"[0-9a-f]{64}")
# Stored statuses a delivered message may run for. request-api only enqueues NEW
# requests, but tasks are acknowledged late, so SQS redelivers a message whose
# worker was lost mid-task, by which time the row is PROCESSING. A finished
# request is never re-sent, so a message for one is a replay.
RUNNABLE_STATUSES = ("NEW", "PROCESSING")


class UntrustedTaskMessage(Exception):
    """A task message on the broker that request-api could not have sent."""


class UnsafeRequestParams(Exception):
    """A real request whose params the worker will not use."""

    def __init__(self, reason, user_message):
        super().__init__(reason)
        self.user_message = user_message


_UNSAFE_URL_MESSAGE = (
    "We cannot check this link. Use a publicly accessible link to the data file."
)
_UNSAFE_PARAMS_MESSAGE = (
    "We could not process this request. Check the details you entered and try " "again."
)


def is_valid_request_id(request_id):
    return isinstance(request_id, str) and bool(
        REQUEST_ID_PATTERN.fullmatch(request_id)
    )


def validate_broker_message(args, kwargs, expected_type, get_stored_request):
    """
    Raise UntrustedTaskMessage unless the message is one request-api sent.

    get_stored_request(request_id) returns the stored request row, or None.
    Anything other than args=[request] (such as a directories override), an
    unknown id, another status or type, a request that has already finished, or
    params that differ from the stored row is rejected.

    Raise UnsafeRequestParams if the message matches the stored request but its
    params fail validate_params.

    Return the stored request's status: PROCESSING means SQS redelivered the
    message after its worker was lost.
    """
    if kwargs or len(args) != 1 or not isinstance(args[0], dict):
        raise UntrustedTaskMessage("Unexpected task arguments")

    message = args[0]
    request_id = message.get("id")
    if not is_valid_request_id(request_id):
        raise UntrustedTaskMessage(f"Invalid request id {request_id!r}")
    if message.get("status") != "NEW":
        raise UntrustedTaskMessage(
            f"Unexpected status {message.get('status')!r} for request {request_id}"
        )
    if message.get("type") != expected_type:
        raise UntrustedTaskMessage(
            f"Request type {message.get('type')!r} sent to the {expected_type} task"
        )

    try:
        message_params = schemas.RequestBase.model_validate(
            {"params": message.get("params")}
        ).params
    except ValidationError as e:
        raise UntrustedTaskMessage(f"Malformed params for request {request_id}") from e

    stored = get_stored_request(request_id)
    if stored is None:
        raise UntrustedTaskMessage(f"Request {request_id} not found in database")

    if stored.status not in RUNNABLE_STATUSES:
        raise UntrustedTaskMessage(f"Request {request_id} is already {stored.status!r}")

    stored_params = schemas.RequestBase.model_validate({"params": stored.params}).params
    if stored.type != expected_type or message_params.model_dump(
        mode="json"
    ) != stored_params.model_dump(mode="json"):
        raise UntrustedTaskMessage(
            f"Params for request {request_id} do not match the stored request"
        )

    validate_params(stored_params)
    return stored.status


def validate_params(params):
    """Raise UnsafeRequestParams if any param could escape what it is for."""
    url = getattr(params, "url", None)
    if url:
        try:
            check_fetch_url(url)
        except UnsafeUrlError as e:
            raise UnsafeRequestParams(
                f"URL not allowed: {e}", _UNSAFE_URL_MESSAGE
            ) from e

    # add_data_task only downloads a resource when it is non-empty, so an empty
    # resource alongside a URL is fine.
    resource = getattr(params, "resource", None)
    if resource and not RESOURCE_PATTERN.fullmatch(resource):
        raise UnsafeRequestParams(
            f"Invalid resource {resource!r}", _UNSAFE_PARAMS_MESSAGE
        )

    uploaded_filename = getattr(params, "uploaded_filename", None)
    if (
        uploaded_filename is not None
        and not schemas.UPLOADED_FILENAME_PATTERN.fullmatch(uploaded_filename)
    ):
        raise UnsafeRequestParams(
            f"Invalid uploaded_filename {uploaded_filename!r}", _UNSAFE_PARAMS_MESSAGE
        )

    # add_data joins github_branch onto the config repo's download URL.
    github_branch = getattr(params, "github_branch", None)
    if github_branch is not None and not schemas.is_safe_github_branch(github_branch):
        raise UnsafeRequestParams(
            f"Invalid github_branch {github_branch!r}", _UNSAFE_PARAMS_MESSAGE
        )

    # Collection and dataset are joined onto local paths. Whether the dataset
    # exists is left to the workflow, which fails the request with an error.
    for field in ("collection", "dataset"):
        value = getattr(params, field)
        if not SLUG_PATTERN.fullmatch(value or ""):
            raise UnsafeRequestParams(
                f"Invalid {field} {value!r}", _UNSAFE_PARAMS_MESSAGE
            )


class BrokerMessageGuard:
    """
    Mixin for Celery tasks that validates delivered messages before the body runs.

    Celery's tracer calls a task's custom __call__ with called_directly=False.
    Direct calls from tests and debug scripts keep called_directly=True and are
    not checked.

    A direct call made while another task is running is refused. Celery's
    built-in tasks (celery.starmap, celery.map, celery.chunks) call the task
    they are given directly, so a message for one of them would otherwise run
    this task without the check. request-api never sends those.

    A task_prerun handler should call validate_message first, so a forged
    message never changes the request's status. The result is cached on the
    task request so each delivery is only validated once. validate_message
    returns the stored status, so the handler can tell a redelivery apart.

    Subclasses set expected_type and implement get_stored_request.
    """

    expected_type = None

    def get_stored_request(self, request_id):
        raise NotImplementedError

    def validate_message(self, args, kwargs):
        request = self.request
        if not hasattr(request, "broker_message_error"):
            request.broker_message_status = None
            try:
                request.broker_message_status = validate_broker_message(
                    args, kwargs, self.expected_type, self.get_stored_request
                )
                request.broker_message_error = None
            except (UntrustedTaskMessage, UnsafeRequestParams) as e:
                request.broker_message_error = e
        if request.broker_message_error is not None:
            raise request.broker_message_error
        return request.broker_message_status

    def __call__(self, *args, **kwargs):
        if not self.request.called_directly:
            self.validate_message(args, kwargs)
        elif get_current_task() is not None:
            raise UntrustedTaskMessage(
                f"{self.name} called from inside task {get_current_task().name}"
            )
        return super().__call__(*args, **kwargs)
