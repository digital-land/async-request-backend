from types import SimpleNamespace

import pytest

from application.utils import task_guard
from application.utils.task_guard import (
    UntrustedTaskMessage,
    validate_broker_message,
    validate_params,
)

VALID_ID = "AbCdEfGhIjKlMnOpQrStUv"
VALID_PARAMS = {
    "type": "check_file",
    "collection": "article-4-direction",
    "dataset": "article-4-direction-area",
    "original_filename": "data.csv",
    "uploaded_filename": "492f15d8-45e4-427e-bde0-f60d69889f40",
}


def _message(request_id=VALID_ID, params=None):
    return {
        "id": request_id,
        "type": "check_file",
        "status": "NEW",
        "params": dict(params or VALID_PARAMS),
    }


@pytest.fixture(autouse=True)
def specification(monkeypatch):
    monkeypatch.setattr(
        task_guard.core_utils,
        "load_specification",
        lambda _dirs: SimpleNamespace(dataset={"article-4-direction-area": {}}),
    )


@pytest.fixture
def stored_request():
    """The request row as stored by request-api."""
    return SimpleNamespace(type="check_file", params=dict(VALID_PARAMS))


@pytest.fixture
def validate(stored_request):
    def lookup(request_id):
        return stored_request if request_id == VALID_ID else None

    def _validate(args, kwargs=None, expected_type="check_file"):
        validate_broker_message(args, kwargs or {}, expected_type, lookup)

    return _validate


def _url_params(**values):
    return SimpleNamespace(
        collection="article-4-direction", dataset="article-4-direction-area", **values
    )


def test_accepts_message_matching_stored_request(validate):
    validate([_message()])


@pytest.mark.parametrize(
    "args, kwargs",
    [
        ([_message(), '{"COLLECTION_DIR": "/"}'], {}),
        ([_message()], {"directories": '{"COLLECTION_DIR": "/"}'}),
        ([], {}),
        (["not-a-dict"], {}),
    ],
)
def test_rejects_unexpected_arguments(validate, args, kwargs):
    with pytest.raises(UntrustedTaskMessage):
        validate(args, kwargs)


@pytest.mark.parametrize(
    "request_id", ["..", "/", "notarequestid", "../" + VALID_ID, VALID_ID + "/..", None]
)
def test_rejects_invalid_request_ids(validate, request_id):
    with pytest.raises(UntrustedTaskMessage):
        validate([_message(request_id=request_id)])


def test_rejects_unknown_request_id(validate):
    with pytest.raises(UntrustedTaskMessage, match="not found"):
        validate([_message(request_id="Z" * 22)])


@pytest.mark.parametrize("status", ["COMPLETE", "PROCESSING", None])
def test_rejects_status_other_than_new(validate, status):
    message = _message()
    message["status"] = status
    with pytest.raises(UntrustedTaskMessage, match="status"):
        validate([message])


def test_rejects_request_type_sent_to_wrong_task(validate):
    with pytest.raises(UntrustedTaskMessage, match="check_url task"):
        validate([_message()], expected_type="check_url")


def test_malformed_params_are_untrusted(validate):
    params = dict(VALID_PARAMS, unexpected_field="x")
    with pytest.raises(UntrustedTaskMessage, match="Malformed"):
        validate([_message(params=params)])


def test_rejects_params_that_differ_from_stored_request(validate):
    params = dict(VALID_PARAMS, uploaded_filename="../../tmp/example.txt")
    with pytest.raises(UntrustedTaskMessage, match="do not match"):
        validate([_message(params=params)])


@pytest.mark.parametrize(
    "field, value",
    [
        ("uploaded_filename", "../../tmp/example.txt"),
        ("uploaded_filename", "../492f15d8-45e4-427e-bde0-f60d69889f40"),
        ("dataset", "../../etc"),
        ("dataset", "not-a-real-dataset"),
        ("collection", "../config"),
    ],
)
def test_rejects_bad_stored_params(validate, stored_request, field, value):
    # Even if the row itself was written with bad values, the guard rejects it.
    stored_request.params[field] = value
    params = dict(VALID_PARAMS, **{field: value})
    with pytest.raises(UntrustedTaskMessage):
        validate([_message(params=params)])


@pytest.mark.parametrize(
    "resource", ["../../etc/passwd", "/etc/passwd", "resource-hash", "A" * 64]
)
def test_rejects_invalid_resource(resource):
    with pytest.raises(UntrustedTaskMessage, match="resource"):
        validate_params(_url_params(resource=resource))


def test_accepts_sha256_resource():
    validate_params(_url_params(resource="a" * 64))


@pytest.mark.parametrize("resource", ["", None])
def test_accepts_empty_resource_on_url_requests(resource):
    # add_data_task only uses resource when it is non-empty; otherwise the URL
    # path is taken.
    validate_params(_url_params(resource=resource, url="https://example.com/data.csv"))


@pytest.mark.parametrize(
    "url",
    [
        "file:///proc/self/environ",
        "http://169.254.170.2/v2/credentials/x",
        "http://2852039166/",
        "exampleurl.csv",
    ],
)
def test_rejects_unsafe_urls_in_stored_params(url):
    with pytest.raises(UntrustedTaskMessage, match="URL not allowed"):
        validate_params(_url_params(url=url))


def test_guard_requires_a_stored_request_lookup():
    with pytest.raises(NotImplementedError):
        task_guard.BrokerMessageGuard().get_stored_request(VALID_ID)
