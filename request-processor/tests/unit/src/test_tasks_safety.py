from types import SimpleNamespace

import pytest
import requests

from src import tasks

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
    """Pretend the request row exists with VALID_PARAMS."""
    stored = SimpleNamespace(type="check_file", status="NEW", params=dict(VALID_PARAMS))
    monkeypatch.setattr(
        tasks, "_get_request", lambda rid: stored if rid == VALID_ID else None
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


def test_replayed_message_for_finished_request_is_left_untouched(
    stored_request, monkeypatch, rejections
):
    calls = []
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: calls.append(a))
    stored_request.status = "COMPLETE"
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [_message()], kwargs={})
        with pytest.raises(tasks.UntrustedTaskMessage, match="already"):
            task(_message())
    finally:
        task.pop_request()
    assert calls == []
    assert rejections == [("message", "broker")]


def test_unsafe_params_on_real_request_fail_it_with_an_error(
    stored_request, monkeypatch, rejections
):
    calls = []
    saved = []
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: calls.append(a))
    monkeypatch.setattr(tasks, "clean_up_request_files", lambda *a: None)
    monkeypatch.setattr(
        tasks, "save_response_to_db", lambda rid, data: saved.append((rid, data))
    )
    # A row stored before request-api checked upload names.
    params = dict(VALID_PARAMS, uploaded_filename="../other-request/file")
    stored_request.params = params
    message = _message(params=params)
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [message], kwargs={})
        # Not marked PROCESSING, and the task body does not run.
        assert calls == []
        with pytest.raises(tasks.UnsafeRequestParams) as raised:
            task(message)
    finally:
        task.pop_request()
    assert rejections == [("params", "broker")]

    tasks.after_task_failure("task-id", raised.value, None, None, [message])
    assert calls == [(VALID_ID, "FAILED")]
    assert saved == [
        (
            VALID_ID,
            {
                "message": raised.value.user_message,
                "status": "",
                "exception": "UnsafeRequestParams",
            },
        )
    ]


@pytest.fixture
def metrics(monkeypatch):
    """Sentry metrics counted by tasks, as (name, attributes) pairs."""
    counted = []

    def count(name, value, unit=None, attributes=None):
        counted.append((name, attributes))

    monkeypatch.setattr(tasks.monitoring.sentry_sdk.metrics, "count", count)
    return counted


def test_redelivery_for_processing_request_is_counted(
    stored_request, monkeypatch, metrics
):
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: None)
    stored_request.status = "PROCESSING"
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [_message()], kwargs={})
    finally:
        task.pop_request()
    assert metrics == [("async.task.redelivered", {"task": task.name})]


def test_first_delivery_is_not_counted_as_redelivery(
    stored_request, monkeypatch, metrics
):
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: None)
    task = tasks.check_datafile
    task.push_request(called_directly=False)
    try:
        tasks.before_task("task-id", task, [_message()], kwargs={})
    finally:
        task.pop_request()
    assert metrics == []


@pytest.mark.parametrize(
    "exception, expected",
    [
        (
            tasks.CustomException(
                {"message": "x", "exception_type": "UnsafeResourceError"}
            ),
            "UnsafeResourceError",
        ),
        (
            tasks.CustomException({"message": "x", "exception": "ConnectionError"}),
            "ConnectionError",
        ),
        (tasks.CustomException({"message": "x"}), "CustomException"),
        (ValueError("boom"), "ValueError"),
    ],
)
def test_task_failure_metric_records_the_cause(
    monkeypatch, metrics, exception, expected
):
    monkeypatch.setattr(tasks, "_update_request_status", lambda *a: None)
    monkeypatch.setattr(tasks, "clean_up_request_files", lambda *a: None)
    tasks.after_task_failure(
        "task-id", exception, None, None, [_message()], sender=tasks.check_datafile
    )
    assert metrics == [
        (
            "request_processor.task_failure",
            {"exception": expected, "task": tasks.check_datafile.name},
        )
    ]


def test_check_url_user_error_metric_reads_custom_exception_detail(metrics):
    detail = tasks.CustomException(
        {"message": "x", "status": "404", "exception": "HTTPError"}
    ).detail
    tasks.monitoring.record_check_url_user_error(detail, VALID_ID)
    assert metrics == [
        (
            "async.check_url.user_error",
            {
                "task": "CheckURL",
                "exception": "HTTPError",
                "status": "404",
                "plugin": None,
            },
        )
    ]


def test_unexpected_check_url_failure_is_one_issue_with_a_stack_trace(
    monkeypatch, metrics
):
    """One logger.exception call: Sentry makes it a single Issue with exc_info."""
    request = _message(
        params={
            "type": "check_url",
            "collection": "article-4-direction",
            "dataset": "article-4-direction-area",
            "url": "https://example.com/data.csv",
        }
    )
    request.update(
        type="check_url",
        created="2026-01-01T00:00:00",
        modified="2026-01-01T00:00:00",
        response=None,
    )
    monkeypatch.setattr(
        tasks, "_fetch_resource", lambda *a, **kw: (_ for _ in ()).throw(KeyError("x"))
    )
    monkeypatch.setattr(tasks, "save_response_to_db", lambda *a: None)
    monkeypatch.setattr(tasks, "_get_request", lambda rid: None)
    records = []
    monkeypatch.setattr(
        tasks.monitoring.logger, "handle", lambda record: records.append(record)
    )

    tasks.check_dataurl(request)

    errors = [r for r in records if r.levelno >= 40]
    assert len(errors) == 1
    assert errors[0].getMessage() == "check_url fetch failed"
    assert errors[0].exc_info[0] is KeyError
    assert errors[0].request_id == VALID_ID
    # Unexpected failures are Issues, not the user-error metric.
    assert metrics == []


def test_worker_sentry_events_have_no_local_variables_and_scrub_secrets():
    options = tasks.monitoring.sentry_privacy_options()
    assert options["include_local_variables"] is False
    assert options["send_default_pii"] is False

    event = {
        "extra": {
            "DATABASE_URL": "postgresql://user:pw@host/db",
            "env": {"AWS_SECRET_ACCESS_KEY": "s", "AWS_SESSION_TOKEN": "t"},
            "request_id": VALID_ID,
        }
    }
    options["event_scrubber"].scrub_event(event)

    def value(v):
        # The scrubber returns an AnnotatedValue holding the replacement.
        return getattr(v, "value", v)

    assert value(event["extra"]["DATABASE_URL"]) == "[Filtered]"
    assert value(event["extra"]["env"]["AWS_SECRET_ACCESS_KEY"]) == "[Filtered]"
    assert value(event["extra"]["env"]["AWS_SESSION_TOKEN"]) == "[Filtered]"
    assert event["extra"]["request_id"] == VALID_ID
