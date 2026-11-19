"""
Rejects task messages that request-api could not have sent.

Anyone who can write to the queue can send the worker a message. request-api
only ever enqueues args=[request] for a NEW request row it has just committed,
on the task matching the request type, so a message that differs from that was
put on the queue by something else.

tasks.py wires this into the Celery tasks: it subclasses BrokerMessageGuard to
look requests up in the database.
"""

import re

from pydantic import ValidationError

import application.core.utils as core_utils
import request_model.schemas as schemas
from application.configurations.config import Directories
from request_model.url_safety import UnsafeUrlError, check_fetch_url

# request-api generates ids with shortuuid.uuid(): 22 alphanumeric characters.
REQUEST_ID_PATTERN = re.compile(r"[A-Za-z0-9]{22}")
SLUG_PATTERN = re.compile(r"[a-z0-9-]+")
# digital-land resource ids are the sha256 hex digest of the content.
RESOURCE_PATTERN = re.compile(r"[0-9a-f]{64}")


class UntrustedTaskMessage(Exception):
    """A task message on the broker that request-api could not have sent."""


def is_valid_request_id(request_id):
    return isinstance(request_id, str) and bool(
        REQUEST_ID_PATTERN.fullmatch(request_id)
    )


def validate_broker_message(args, kwargs, expected_type, get_stored_request):
    """
    Raise UntrustedTaskMessage unless the message is one request-api sent.

    get_stored_request(request_id) returns the stored request row, or None.
    Anything other than args=[request] (such as a directories override), an
    unknown id, another status or type, or params that differ from the stored
    row is rejected.
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

    stored_params = schemas.RequestBase.model_validate({"params": stored.params}).params
    if stored.type != expected_type or message_params.model_dump(
        mode="json"
    ) != stored_params.model_dump(mode="json"):
        raise UntrustedTaskMessage(
            f"Params for request {request_id} do not match the stored request"
        )

    validate_params(stored_params)


def validate_params(params):
    """Raise UntrustedTaskMessage if any param could escape what it is for."""
    url = getattr(params, "url", None)
    if url:
        try:
            check_fetch_url(url)
        except UnsafeUrlError as e:
            raise UntrustedTaskMessage(f"URL not allowed: {e}") from e

    # add_data_task only downloads a resource when it is non-empty, so an empty
    # resource alongside a URL is fine.
    resource = getattr(params, "resource", None)
    if resource and not RESOURCE_PATTERN.fullmatch(resource):
        raise UntrustedTaskMessage(f"Invalid resource {resource!r}")

    uploaded_filename = getattr(params, "uploaded_filename", None)
    if (
        uploaded_filename is not None
        and not schemas.UPLOADED_FILENAME_PATTERN.fullmatch(uploaded_filename)
    ):
        raise UntrustedTaskMessage(f"Invalid uploaded_filename {uploaded_filename!r}")

    if not SLUG_PATTERN.fullmatch(params.collection or ""):
        raise UntrustedTaskMessage(f"Invalid collection {params.collection!r}")

    if not SLUG_PATTERN.fullmatch(params.dataset or ""):
        raise UntrustedTaskMessage(f"Invalid dataset {params.dataset!r}")
    if params.dataset not in core_utils.load_specification(Directories).dataset:
        raise UntrustedTaskMessage(f"Dataset {params.dataset!r} not in specification")


class BrokerMessageGuard:
    """
    Mixin for Celery tasks that validates delivered messages before the body runs.

    Celery's tracer calls a task's custom __call__ with called_directly=False.
    Direct calls (tests, debug scripts) keep called_directly=True and are not
    checked, so they can still pass a directories override.

    A task_prerun handler should call validate_message first, so a rejected
    message never changes the request's status. The result is cached on the
    task request so each delivery is only validated once.

    Subclasses set expected_type and implement get_stored_request.
    """

    expected_type = None

    def get_stored_request(self, request_id):
        raise NotImplementedError

    def validate_message(self, args, kwargs):
        request = self.request
        if not hasattr(request, "broker_message_error"):
            try:
                validate_broker_message(
                    args, kwargs, self.expected_type, self.get_stored_request
                )
                request.broker_message_error = None
            except UntrustedTaskMessage as e:
                request.broker_message_error = e
        if request.broker_message_error is not None:
            raise request.broker_message_error

    def __call__(self, *args, **kwargs):
        if not self.request.called_directly:
            self.validate_message(args, kwargs)
        return super().__call__(*args, **kwargs)
