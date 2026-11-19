from types import SimpleNamespace

import pytest
import requests

from src import tasks
from application.utils import task_guard

VALID_ID = "AbCdEfGhIjKlMnOpQrStUv"
UPLOAD_ID = "492f15d8-45e4-427e-bde0-f60d69889f40"
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


VRT_POINTING_ELSEWHERE = (
    b"<OGRVRTDataSource><OGRVRTLayer name='x'><SrcDataSource>"
    b"/vsicurl/http://169.254.170.2/v2/credentials/abc"
    b"</SrcDataSource></OGRVRTLayer></OGRVRTDataSource>"
)
UPLOADED_VRT_READING_S3 = (
    b"<OGRVRTDataSource><OGRVRTLayer name='x'><SrcDataSource>"
    b"/vsisparse//vsis3/bucket/492f15d8-45e4-427e-bde0-f60d69889f41"
    b"</SrcDataSource></OGRVRTLayer></OGRVRTDataSource>"
)


def _fake_fetch_writing(content):
    def fake_fetch(self, url, **kwargs):
        (self.resource_dir / "resource").write_bytes(content)
        return tasks.FetchStatus.OK, {"status": "200"}

    return fake_fetch


def _fake_s3_download(content):
    def download(bucket, key, path, max_size):
        with open(path, "wb") as f:
            f.write(content)

    return download


def test_fetch_resource_refuses_gdal_pointer_file(tmp_path, monkeypatch):
    # A public https URL serving a VRT named .gml.
    monkeypatch.setattr(
        tasks.Collector, "fetch", _fake_fetch_writing(VRT_POINTING_ELSEWHERE)
    )
    with pytest.raises(tasks.CustomException) as raised:
        tasks._fetch_resource(tmp_path, "https://example.org/boundary.gml")
    assert raised.value.detail["exceptionType"] == "UnsafeResourceError"
    assert "another data source" in raised.value.detail["errMsgDetail"]


def test_fetch_resource_accepts_ordinary_file(tmp_path, monkeypatch):
    content = b'{"type": "FeatureCollection", "features": []}'
    monkeypatch.setattr(tasks.Collector, "fetch", _fake_fetch_writing(content))
    file_name, log = tasks._fetch_resource(tmp_path, "https://example.org/a.json")
    assert file_name == "resource"
    assert log["plugin"] is None


@pytest.mark.parametrize(
    "content",
    [
        UPLOADED_VRT_READING_S3,
        b'{"type": "gdal_streamed_alg", "command_line": "gdal vector pipeline '
        b'! read /proc/self/environ ! write -f stream streamed_dataset"}',
    ],
)
def test_check_file_refuses_upload_that_points_gdal_elsewhere(
    tmp_path, monkeypatch, content
):
    saved = []
    monkeypatch.setenv("REQUEST_FILES_BUCKET_NAME", "bucket")
    monkeypatch.setattr(
        tasks.s3_transfer_manager,
        "download_with_default_configuration",
        _fake_s3_download(content),
    )
    monkeypatch.setattr(
        tasks, "save_response_to_db", lambda request_id, log: saved.append(log)
    )
    request_data = SimpleNamespace(uploaded_filename=UPLOAD_ID)

    with pytest.raises(tasks.CustomException):
        tasks.handle_check_file(SimpleNamespace(id=VALID_ID), request_data, tmp_path)

    assert saved[0]["exception_type"] == "UnsafeResourceError"
    assert saved[0]["message"] == "The uploaded file is not supported"


def test_check_file_accepts_ordinary_upload(tmp_path, monkeypatch):
    monkeypatch.setenv("REQUEST_FILES_BUCKET_NAME", "bucket")
    monkeypatch.setattr(
        tasks.s3_transfer_manager,
        "download_with_default_configuration",
        _fake_s3_download(b"reference,name\nTPO1,Oak\n"),
    )
    request_data = SimpleNamespace(uploaded_filename=UPLOAD_ID)
    assert (
        tasks.handle_check_file(SimpleNamespace(id=VALID_ID), request_data, tmp_path)
        == UPLOAD_ID
    )


@pytest.mark.parametrize(
    "url", ["file:///proc/self/environ", "http://169.254.169.254/latest/meta-data/"]
)
def test_fetch_resource_refuses_unsafe_urls(tmp_path, url):
    with pytest.raises(tasks.CustomException) as raised:
        tasks._fetch_resource(tmp_path, url)
    assert "not allowed" in str(raised.value)


def test_fetch_resource_removes_file_adapter(tmp_path, monkeypatch):
    seen = {}

    def fake_fetch(self, url, **kwargs):
        seen["adapters"] = list(self.session.adapters)
        # A redirect to file: would be resolved by the same session.
        with pytest.raises(requests.exceptions.InvalidSchema):
            self.session.get("file:///etc/hosts")
        return tasks.FetchStatus.FAILED, {"status": "404"}

    monkeypatch.setattr(tasks.Collector, "fetch", fake_fetch)
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(tmp_path, "https://example.org/data.csv")
    assert "file:" not in seen["adapters"]


def test_fetch_resource_reports_why_url_was_refused(tmp_path):
    with pytest.raises(tasks.CustomException) as raised:
        tasks._fetch_resource(tmp_path, "file:///etc/passwd")
    assert raised.value.detail["exceptionType"] == "UnsafeUrlError"
    assert raised.value.detail["errMsgDetail"] == "Only http and https URLs are allowed"


def test_fetch_resource_checks_dns_for_paged_wfs(tmp_path, monkeypatch):
    def refuse(url):
        raise tasks.UnsafeUrlError("resolves to a non-public address")

    monkeypatch.setattr(tasks, "check_host_resolves_publicly", refuse)
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(
            tmp_path, "https://wfs.example.org/wfs", {"page_size": 100}
        )


@pytest.fixture
def rejections(monkeypatch):
    """Security rejections counted by tasks, as (layer, source) pairs."""
    counted = []
    monkeypatch.setattr(
        tasks,
        "count_security_rejection",
        lambda layer, source: counted.append((layer, source)),
    )
    return counted


def test_refused_url_is_counted(tmp_path, rejections):
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(tmp_path, "file:///etc/passwd")
    assert rejections == [("url", "fetch")]


def test_refused_fetched_file_is_counted(tmp_path, monkeypatch, rejections):
    monkeypatch.setattr(
        tasks.Collector, "fetch", _fake_fetch_writing(VRT_POINTING_ELSEWHERE)
    )
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(tmp_path, "https://example.org/boundary.gml")
    assert rejections == [("content", "fetch")]


def test_refused_upload_is_counted(tmp_path, monkeypatch, rejections):
    monkeypatch.setenv("REQUEST_FILES_BUCKET_NAME", "bucket")
    monkeypatch.setattr(
        tasks.s3_transfer_manager,
        "download_with_default_configuration",
        _fake_s3_download(UPLOADED_VRT_READING_S3),
    )
    monkeypatch.setattr(tasks, "save_response_to_db", lambda request_id, log: None)
    with pytest.raises(tasks.CustomException):
        tasks.handle_check_file(
            SimpleNamespace(id=VALID_ID),
            SimpleNamespace(uploaded_filename=UPLOAD_ID),
            tmp_path,
        )
    assert rejections == [("content", "upload")]


def test_ordinary_fetch_is_not_counted(tmp_path, monkeypatch, rejections):
    content = b'{"type": "FeatureCollection", "features": []}'
    monkeypatch.setattr(tasks.Collector, "fetch", _fake_fetch_writing(content))
    tasks._fetch_resource(tmp_path, "https://example.org/a.json")
    assert rejections == []


def test_paged_wfs_host_that_does_not_resolve_is_not_counted(
    tmp_path, monkeypatch, rejections
):
    # Only internal addresses are security rejections; safe_fetch counts
    # those. A DNS failure must not be counted here as a URL rejection.
    def unresolvable(url):
        raise tasks.UnsafeUrlError("URL host is not allowed: did not resolve")

    monkeypatch.setattr(tasks, "check_host_resolves_publicly", unresolvable)
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(
            tmp_path, "https://wfs.example.org/wfs", {"page_size": 100}
        )
    assert rejections == []


@pytest.fixture
def stored_request(monkeypatch):
    """Pretend the request row exists with VALID_PARAMS, and stub the spec."""
    stored = SimpleNamespace(type="check_file", params=dict(VALID_PARAMS))
    monkeypatch.setattr(
        tasks, "_get_request", lambda rid: stored if rid == VALID_ID else None
    )
    monkeypatch.setattr(
        task_guard.core_utils,
        "load_specification",
        lambda _dirs: SimpleNamespace(dataset={"article-4-direction-area": {}}),
    )
    return stored


def test_worker_delivered_call_is_guarded(stored_request):
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        with pytest.raises(tasks.UntrustedTaskMessage):
            task(_message(request_id=".."), '{"COLLECTION_DIR": "/"}')
    finally:
        task.pop_request()


def test_failure_handler_ignores_untrusted_messages(monkeypatch):
    calls = []
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: calls.append(a))
    monkeypatch.setattr(tasks, "clean_up_request_files", lambda *a: calls.append(a))
    tasks.after_task_failure(
        "task-id",
        tasks.UntrustedTaskMessage("forged"),
        None,
        None,
        [_message()],
    )
    assert calls == []


@pytest.mark.parametrize("handler", ["before_task", "after_task_failure"])
def test_signal_handlers_ignore_invalid_request_ids(monkeypatch, handler):
    calls = []
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: calls.append(a))
    monkeypatch.setattr(tasks, "clean_up_request_files", lambda *a: calls.append(a))
    if handler == "before_task":
        tasks.before_task("task-id", None, [_message(request_id="..")])
    else:
        tasks.after_task_failure(
            "task-id", ValueError("boom"), None, None, [_message(request_id="..")]
        )
    assert calls == []


def test_before_task_does_not_set_processing_for_rejected_message(
    stored_request, monkeypatch
):
    calls = []
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: calls.append(a))
    task = tasks.check_datafile
    message = _message()
    message["status"] = "COMPLETE"
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [message], kwargs={})
        assert calls == []
        # __call__ re-raises the cached rejection without running the task body.
        with pytest.raises(tasks.UntrustedTaskMessage, match="status"):
            task(message)
    finally:
        task.pop_request()


def test_before_task_sets_processing_for_valid_message(stored_request, monkeypatch):
    calls = []
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: calls.append(a))
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [_message()], kwargs={})
    finally:
        task.pop_request()
    assert calls == [(VALID_ID, "PROCESSING")]


def test_forged_message_is_counted(stored_request, monkeypatch, rejections):
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: None)
    task = tasks.check_datafile
    message = _message(request_id="Z" * 22)  # well-formed id, no stored row
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [message], kwargs={})
    finally:
        task.pop_request()
    assert rejections == [("message", "broker")]


def test_trusted_message_is_not_counted(stored_request, monkeypatch, rejections):
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: None)
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [_message()], kwargs={})
    finally:
        task.pop_request()
    assert rejections == []
