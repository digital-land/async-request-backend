from types import SimpleNamespace

import pytest

from src import tasks
from application.core import workflow

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


@pytest.fixture
def stored_request(monkeypatch):
    """Pretend the request row exists with VALID_PARAMS, and stub the spec."""
    stored = SimpleNamespace(type="check_file", params=dict(VALID_PARAMS))
    monkeypatch.setattr(
        tasks, "_get_request", lambda rid: stored if rid == VALID_ID else None
    )
    monkeypatch.setattr(
        tasks.utils,
        "load_specification",
        lambda _dirs: SimpleNamespace(dataset={"article-4-direction-area": {}}),
    )
    return stored


def test_accepts_message_matching_stored_request(stored_request):
    tasks._validate_broker_message([_message()], {}, "check_file")


@pytest.mark.parametrize(
    "args, kwargs",
    [
        ([_message(), '{"COLLECTION_DIR": "/"}'], {}),
        ([_message()], {"directories": '{"COLLECTION_DIR": "/"}'}),
        ([], {}),
        (["not-a-dict"], {}),
    ],
)
def test_rejects_unexpected_arguments(stored_request, args, kwargs):
    with pytest.raises(tasks.UntrustedTaskMessage):
        tasks._validate_broker_message(args, kwargs, "check_file")


@pytest.mark.parametrize(
    "request_id", ["..", "/", "i11590pth3", "../" + VALID_ID, VALID_ID + "/..", None]
)
def test_rejects_invalid_request_ids(stored_request, request_id):
    with pytest.raises(tasks.UntrustedTaskMessage):
        tasks._validate_broker_message(
            [_message(request_id=request_id)], {}, "check_file"
        )


def test_rejects_unknown_request_id(stored_request):
    with pytest.raises(tasks.UntrustedTaskMessage, match="not found"):
        tasks._validate_broker_message(
            [_message(request_id="Z" * 22)], {}, "check_file"
        )


def test_rejects_params_that_differ_from_stored_request(stored_request):
    params = dict(
        VALID_PARAMS,
        uploaded_filename="usr/local/lib/python3.12/site-packages/zzz.pth",
    )
    with pytest.raises(tasks.UntrustedTaskMessage, match="do not match"):
        tasks._validate_broker_message([_message(params=params)], {}, "check_file")


@pytest.mark.parametrize(
    "field, value",
    [
        ("uploaded_filename", "usr/local/lib/python3.12/site-packages/zzz.pth"),
        ("uploaded_filename", "../492f15d8-45e4-427e-bde0-f60d69889f40"),
        ("dataset", "../../etc"),
        ("dataset", "i11590-nonexistent"),
        ("collection", "../config"),
    ],
)
def test_rejects_bad_stored_params(stored_request, field, value):
    # Even if the row itself was written with bad values, the guard rejects it.
    stored_request.params[field] = value
    params = dict(VALID_PARAMS, **{field: value})
    with pytest.raises(tasks.UntrustedTaskMessage):
        tasks._validate_broker_message([_message(params=params)], {}, "check_file")


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


def _make_collection_dir(tmp_path, monkeypatch):
    collection_dir = tmp_path / "collection"
    resource_dir = collection_dir / "resource"
    (resource_dir / VALID_ID).mkdir(parents=True)
    (resource_dir / VALID_ID / "file.csv").write_text("x")
    (resource_dir / "other-request").mkdir()
    (resource_dir / "other-request" / "keep.csv").write_text("x")
    (collection_dir / "keep.csv").write_text("x")
    monkeypatch.setattr(tasks.Directories, "COLLECTION_DIR", str(collection_dir))
    return collection_dir, resource_dir


@pytest.mark.parametrize("request_id", ["..", "../..", "/", str("/tmp")])
def test_clean_up_request_files_refuses_paths_outside_resource_dir(
    tmp_path, monkeypatch, request_id
):
    collection_dir, resource_dir = _make_collection_dir(tmp_path, monkeypatch)
    tasks.clean_up_request_files(request_id)
    assert (collection_dir / "keep.csv").exists()
    assert (resource_dir / "other-request" / "keep.csv").exists()
    assert (resource_dir / VALID_ID / "file.csv").exists()


def test_clean_up_request_files_removes_request_dir(tmp_path, monkeypatch):
    collection_dir, resource_dir = _make_collection_dir(tmp_path, monkeypatch)
    tasks.clean_up_request_files(VALID_ID)
    assert not (resource_dir / VALID_ID).exists()
    assert (resource_dir / "other-request" / "keep.csv").exists()


@pytest.mark.parametrize("request_id", ["..", "../..", "/"])
def test_workflow_clean_up_refuses_parent_directories(tmp_path, request_id):
    base = tmp_path / "resource"
    (base / "sub").mkdir(parents=True)
    (base / "sub" / "keep.csv").write_text("x")
    (tmp_path / "keep.csv").write_text("x")
    workflow.clean_up(request_id, str(base), str(base / "sub"))
    assert (tmp_path / "keep.csv").exists()
    assert (base / "sub" / "keep.csv").exists()


def test_workflow_clean_up_removes_request_dir(tmp_path):
    base = tmp_path / "resource"
    (base / VALID_ID).mkdir(parents=True)
    (base / VALID_ID / "file.csv").write_text("x")
    (base / "keep.csv").write_text("x")
    workflow.clean_up(VALID_ID, str(base))
    assert not (base / VALID_ID).exists()
    assert (base / "keep.csv").exists()


@pytest.mark.parametrize("status", ["COMPLETE", "PROCESSING", None])
def test_rejects_status_other_than_new(stored_request, status):
    message = _message()
    message["status"] = status
    with pytest.raises(tasks.UntrustedTaskMessage, match="status"):
        tasks._validate_broker_message([message], {}, "check_file")


def test_rejects_request_type_sent_to_wrong_task(stored_request):
    with pytest.raises(tasks.UntrustedTaskMessage, match="check_url task"):
        tasks._validate_broker_message([_message()], {}, "check_url")


def test_malformed_params_are_untrusted(stored_request):
    params = dict(VALID_PARAMS, unexpected_field="x")
    with pytest.raises(tasks.UntrustedTaskMessage, match="Malformed"):
        tasks._validate_broker_message([_message(params=params)], {}, "check_file")


@pytest.mark.parametrize(
    "resource", ["../../etc/passwd", "/etc/passwd", "resource-hash", "A" * 64]
)
def test_rejects_invalid_resource(monkeypatch, resource):
    monkeypatch.setattr(
        tasks.utils,
        "load_specification",
        lambda _dirs: SimpleNamespace(dataset={"article-4-direction-area": {}}),
    )
    params = SimpleNamespace(
        resource=resource,
        collection="article-4-direction",
        dataset="article-4-direction-area",
    )
    with pytest.raises(tasks.UntrustedTaskMessage, match="resource"):
        tasks._validate_params(params)


def test_accepts_sha256_resource(monkeypatch):
    monkeypatch.setattr(
        tasks.utils,
        "load_specification",
        lambda _dirs: SimpleNamespace(dataset={"article-4-direction-area": {}}),
    )
    params = SimpleNamespace(
        resource="a" * 64,
        collection="article-4-direction",
        dataset="article-4-direction-area",
    )
    tasks._validate_params(params)


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


def test_clean_up_request_files_refuses_symlinked_request_dir(tmp_path, monkeypatch):
    collection_dir, resource_dir = _make_collection_dir(tmp_path, monkeypatch)
    link_id = "B" * 22
    (resource_dir / link_id).symlink_to(resource_dir / "other-request")
    tasks.clean_up_request_files(link_id)
    assert (resource_dir / "other-request" / "keep.csv").exists()


def test_clean_up_request_files_does_not_follow_nested_symlinks(tmp_path, monkeypatch):
    collection_dir, resource_dir = _make_collection_dir(tmp_path, monkeypatch)
    (resource_dir / VALID_ID / "link").symlink_to(resource_dir / "other-request")
    tasks.clean_up_request_files(VALID_ID)
    assert not (resource_dir / VALID_ID).exists()
    assert (resource_dir / "other-request" / "keep.csv").exists()


def test_workflow_clean_up_refuses_symlinked_request_dir(tmp_path):
    base = tmp_path / "resource"
    (base / "other").mkdir(parents=True)
    (base / "other" / "keep.csv").write_text("x")
    (base / VALID_ID).symlink_to(base / "other")
    workflow.clean_up(VALID_ID, str(base))
    assert (base / "other" / "keep.csv").exists()


def test_workflow_clean_up_does_not_follow_nested_symlinks(tmp_path):
    base = tmp_path / "resource"
    outside = tmp_path / "outside"
    (outside / VALID_ID).mkdir(parents=True)
    (outside / VALID_ID / "keep.csv").write_text("x")
    (base / VALID_ID).mkdir(parents=True)
    (base / VALID_ID / "link").symlink_to(outside)
    workflow.clean_up(VALID_ID, str(base))
    assert (outside / VALID_ID / "keep.csv").exists()
    assert not (base / VALID_ID).exists()


@pytest.mark.parametrize("resource", ["", None])
def test_accepts_empty_resource_on_url_requests(monkeypatch, resource):
    # add_data_task only uses resource when it is non-empty; otherwise the URL
    # path is taken.
    monkeypatch.setattr(
        tasks.utils,
        "load_specification",
        lambda _dirs: SimpleNamespace(dataset={"article-4-direction-area": {}}),
    )
    params = SimpleNamespace(
        resource=resource,
        url="https://example.com/data.csv",
        collection="article-4-direction",
        dataset="article-4-direction-area",
    )
    tasks._validate_params(params)
