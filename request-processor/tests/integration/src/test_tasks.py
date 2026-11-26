import datetime
import time
import pytest
import json
import shutil
import os
import database
from sqlalchemy import inspect
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session
from src import tasks
from request_model import models, schemas
from src.tasks import check_datafile, check_dataurl
from digital_land.collect import FetchStatus
from unittest import mock
from application.configurations.config import Directories


@pytest.fixture(scope="module")
def test_data_dir(test_dir):
    return os.path.realpath(f"{test_dir}/../../data")


@pytest.mark.parametrize(
    "filename, uploaded_filename, expected_status",
    [
        (
            "article-direction-area.csv",
            "492f15d8-45e4-427e-bde0-f60d69889f40",
            "COMPLETE",
        ),
        # A well-formed upload key that is not in the bucket.
        ("invalid.csv", "00000000-0000-4000-8000-000000000000", "FAILED"),
    ],
)
def test_check_datafile(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
    test_data_dir,
    filename,
    uploaded_filename,
    expected_status,
):
    """
    This function tests the check_datafile task for file validation.

    Args:
        mocker: Mocking framework for Python.
        celery_app: Celery application.
        celery_worker: Celery worker.
        s3_bucket: S3 bucket.
        db: Database.
        mock_directories: Mocked directories.
        mock_fetch_pipeline_csvs: Mock function for fetching pipeline CSVs.
        test_data_dir: Directory containing test data.
        filename: Name of the file to validate.
        uploaded_filename: Uploaded filename.
        expected_status: Expected status after validation.
    """
    params = {
        "collection": "article-4-direction",
        "dataset": "article-4-direction-area",
        "original_filename": filename,
        "uploaded_filename": uploaded_filename,
    }
    request = _create_request(
        schemas.CheckFileParams(**params), schemas.RequestTypeEnum.check_file
    )
    _handle_pipeline_config_csvs(
        test_data_dir, mock_directories, mocker, mock_fetch_pipeline_csvs, request
    )

    # Convert mock directory paths to strings
    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

    _register_and_check_request(
        mock_directories_str, celery_app, request, expected_status
    )


@pytest.mark.parametrize(
    "test_name, url, get_request_return_value, expected_status, mock_response, expected_plugin_call",
    [
        (
            "valid_url_no_plugin",
            "https://example.com/exampleurl.csv",
            (
                None,
                '{"type":"FeatureCollection","properties":{"exceededTransferLimit":true}, "features":[{"type":"Feature","id":1,"geometry":{"type":"Point", "coordinates":[-1.59153574212325,54.9392094142866]}, "properties": {"reference": "CA01","name": "Ashleworth Conservation Area"}}]}'.encode(  # noqa
                    "utf-8"
                ),
            ),
            "COMPLETE",
            True,
            None,
        ),
        (
            "valid_url_with_arcgis_plugin",
            "https://example.com/arcgis/rest/services/MapServer",
            (
                None,
                '{"type":"FeatureCollection","features":[{"type":"Feature","properties":{"name":"Test Feature"}}]}'.encode(  # noqa: E501
                    "utf-8"
                ),
            ),
            "COMPLETE",
            True,
            "arcgis",
        ),
        (
            "valid_url_with_wfs_plugin",
            "https://example.com/wfs?service=WFS",
            (
                None,
                '<?xml version="1.0"?><wfs:FeatureCollection></wfs:FeatureCollection>'.encode(
                    "utf-8"
                ),
            ),
            "COMPLETE",
            True,
            "wfs",
        ),
        (
            "invalid_url",
            "https://example.com/exampleurl.csv",
            ('{"status": "404", "message": "Unable to process"}', None),
            "COMPLETE",
            False,
            None,
        ),
    ],
)
def test_check_datafile_url(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
    test_data_dir,
    test_name,
    url,
    get_request_return_value,
    expected_status,
    mock_response,
    expected_plugin_call,
):
    """
    This function tests the check_datafile task for URL validation with plugin support.

    Args:
        mocker: Mocking framework for Python.
        celery_app: Celery application.
        celery_worker: Celery worker.
        s3_bucket: S3 bucket.
        db: Database.
        mock_directories: Mocked directories.
        mock_fetch_pipeline_csvs: Mock function for fetching pipeline CSVs.
        test_data_dir: Directory containing test data.
        test_name: Name of the test case.
        url: The URL to validate.
        get_request_return_value: The return value of the mocked get_request function.
        expected_status: The expected status of the request.
        mock_response: determine if mock fetch_pipeline_csvs should be called.
        expected_plugin_call: The expected plugin parameter passed to collector.fetch().
    """

    params = {
        "collection": "article-4-direction",
        "dataset": "article-4-direction-area",
        "url": url,
    }

    request = _create_request(
        schemas.CheckUrlParams(**params), schemas.RequestTypeEnum.check_url
    )

    _handle_pipeline_config_csvs(
        test_data_dir, mock_directories, mocker, mock_fetch_pipeline_csvs, request
    )

    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

    # Track calls to collector.fetch to verify plugin parameter
    fetch_calls = []

    def mock_collector_fetch(self, url, plugin=None, **kwargs):
        fetch_calls.append({"url": url, "plugin": plugin})

        if test_name == "invalid_url":
            return FetchStatus.FAILED, {"status": "404"}

        if plugin == expected_plugin_call:
            resource_dir = self.resource_dir
            resource_dir.mkdir(parents=True, exist_ok=True)
            mock_file = resource_dir / "mock_resource_hash"
            mock_file.write_text("mock csv data")
            return FetchStatus.OK, {"plugin": plugin}
        else:
            return FetchStatus.FAILED, {}

    mocker.patch("digital_land.collect.Collector.fetch", mock_collector_fetch)

    _register_and_check_request(
        mock_directories_str, celery_app, request, expected_status
    )

    # Verify the plugin parameter was passed correctly
    assert (
        len(fetch_calls) > 0
    ), f"Expected at least one fetch call, got {len(fetch_calls)}"

    # Find the call with the expected plugin
    matching_calls = [
        call for call in fetch_calls if call["plugin"] == expected_plugin_call
    ]
    assert (
        len(matching_calls) > 0
    ), f"Expected call with plugin {expected_plugin_call} not found in {fetch_calls}"
    assert (
        matching_calls[0]["url"] == url
    ), f"Expected URL {url}, got {matching_calls[0]['url']}"


def _wait_for_request_status(
    request_id, expected_status, timeout_seconds=10, interval_seconds=1
):
    seconds_waited = 0
    actual_status = "UNKNOWN"
    while seconds_waited <= timeout_seconds:
        db_session = database.session_maker()
        with db_session() as session:
            result = (
                session.query(models.Request)
                .filter(models.Request.id == request_id)
                .first()
            )
            actual_status = result.status
            if actual_status == expected_status:
                return
            else:
                time.sleep(interval_seconds)
                seconds_waited += interval_seconds
    pytest.fail(
        f"Expected status of {expected_status} for request {request_id} but actual status was {actual_status}"
    )


def _create_request(params, type):
    request_model = models.Request(
        type=type,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=params.model_dump(),
    )
    db_session = database.session_maker()
    with db_session() as session:
        session.add(request_model)
        session.commit()
        session.refresh(request_model)
    request = schemas.Request(
        id=request_model.id,
        type=request_model.type,
        status=request_model.status,
        created=request_model.created,
        modified=request_model.modified,
        params=request_model.params,
        response=None,
    )
    return request


def _handle_pipeline_config_csvs(
    test_data_dir, mock_directories, mocker, mock_fetch_pipeline_csvs, request
):
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    shutil.copy(source_organisation_csv, destination_organisation_csv)
    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs("article-4-direction-area", request.id),
    )


def _register_and_check_request(
    mock_directories_str, celery_app, request, expected_status
):
    if request.type == schemas.RequestTypeEnum.check_file:
        task = celery_app.register_task(check_datafile)
    elif request.type == schemas.RequestTypeEnum.check_url:
        task = celery_app.register_task(check_dataurl)
    else:
        raise ValueError(f"Unknown request type: {request.type}")
    # The worker rejects broker messages carrying a directories override, so
    # point the worker's default Directories at the test dirs instead.
    with mock.patch.multiple(Directories, **mock_directories_str):
        task.delay(request.model_dump())
        _wait_for_request_status(request.id, expected_status)


@pytest.fixture
def result_bucket(monkeypatch, s3_client):
    monkeypatch.setenv("REQUEST_DETAILS_BUCKET_NAME", "request-results-test")
    s3_client.create_bucket(
        Bucket="request-results-test",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    yield s3_client
    for obj in s3_client.list_objects_v2(Bucket="request-results-test").get(
        "Contents", []
    ):
        s3_client.delete_object(Bucket="request-results-test", Key=obj["Key"])
    s3_client.delete_bucket(Bucket="request-results-test")


def create_request(saved=False, resource=False):
    params = schemas.AddDataParams(
        collection="conservation-area",
        dataset="conservation-area",
        url=None if resource else "https://example.test/data.csv",
        resource="a" * 64 if resource else None,
    )
    request = _create_request(params, schemas.RequestTypeEnum.add_data)
    if saved:
        with database.session_maker()() as session:
            session.add(models.Response(request_id=request.id, data=summaries()))
            session.commit()
    return request.model_dump(mode="json")


def summaries():
    return {
        "pipeline-summary": {"new-entities": [{"entity": 123}]},
        "endpoint-summary": {},
        "source-summary": {},
    }


@pytest.mark.parametrize("resource", [False, True])
def test_task_publishes_before_complete(db, result_bucket, monkeypatch, resource):
    request = create_request(resource=resource)
    monkeypatch.setattr(tasks, "_fetch_resource", lambda *a, **kw: ("data.csv", {}))
    monkeypatch.setattr(tasks, "_download_resource", lambda *a, **kw: ("data.csv", {}))
    monkeypatch.setattr(
        tasks,
        "_get_resource_metadata",
        lambda *a: {
            "organisation": "local-authority:ABC",
            "endpoints": ["endpoint"],
            "start_date": None,
            "url": "https://example.test/data.csv",
            "documentation_url": None,
            "licence": None,
        },
    )
    monkeypatch.setattr(
        tasks.workflow, "add_data_workflow", lambda *a, **kw: summaries()
    )
    original_update = tasks._update_request_status

    def update(request_id, status):
        if status == "COMPLETE":
            obj = result_bucket.get_object(
                Bucket="request-results-test", Key=f"{request_id}.json"
            )
            assert json.loads(obj["Body"].read())["id"] == request_id
            assert tasks._get_request(request_id).published_at is not None
        original_update(request_id, status)

    monkeypatch.setattr(tasks, "_update_request_status", update)
    result = tasks.add_data_task.apply(args=[request], throw=False)
    assert result.successful(), str(result.result)
    assert tasks._get_request(request["id"]).status == "COMPLETE"


def test_publication_failure_is_failed_and_retry_uses_saved_result(
    db, result_bucket, monkeypatch
):
    request = create_request(saved=True)

    def unexpected_workflow(*args, **kwargs):
        raise AssertionError("A publication retry must not rerun entity allocation")

    monkeypatch.setattr(tasks.workflow, "add_data_workflow", unexpected_workflow)
    monkeypatch.setattr(tasks, "_fetch_resource", unexpected_workflow)
    result_bucket.delete_bucket(Bucket="request-results-test")
    first = tasks.add_data_task.apply(args=[request], throw=False)
    assert first.failed()
    assert tasks._get_request(request["id"]).status == "FAILED"
    assert tasks._get_request(request["id"]).published_at is None
    assert tasks._get_response(request["id"]).data == summaries()

    result_bucket.create_bucket(
        Bucket="request-results-test",
        CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
    )
    # The broker guard rejects messages for FAILED requests as replays, so the
    # retry arrives as an SQS redelivery, while the request is PROCESSING.
    tasks._update_request_status(request["id"], "PROCESSING")
    second = tasks.add_data_task.apply(args=[request], throw=False)
    assert second.successful(), str(second.result)
    assert tasks._get_request(request["id"]).status == "COMPLETE"
    assert tasks._get_request(request["id"]).published_at is not None
    obj = result_bucket.get_object(
        Bucket="request-results-test", Key=f"{request['id']}.json"
    )
    assert json.loads(obj["Body"].read())["response"]["data"] == summaries()


@pytest.mark.parametrize("failure", ["returned_error", "download_error", "exception"])
def test_add_data_errors_are_published(db, result_bucket, monkeypatch, failure):
    request = create_request()
    if failure == "exception":

        def fetch(*args, **kwargs):
            raise RuntimeError("Unexpected processing failure")

        monkeypatch.setattr(tasks, "_fetch_resource", fetch)
    else:
        monkeypatch.setattr(
            tasks,
            "_fetch_resource",
            lambda *a, **kw: (
                None if failure == "download_error" else "data.csv",
                {"message": "Download failed", "status": "403"},
            ),
        )
        monkeypatch.setattr(
            tasks.workflow,
            "add_data_workflow",
            lambda *a, **kw: {"message": "Processing failed", "status": "500"},
        )
    result = tasks.add_data_task.apply(args=[request], throw=False)
    saved = tasks._get_request(request["id"])
    expected_status = "COMPLETE" if failure == "returned_error" else "FAILED"
    assert saved.status == expected_status
    assert saved.published_at is not None
    assert result.successful() == (failure == "returned_error")
    obj = result_bucket.get_object(
        Bucket="request-results-test", Key=f"{request['id']}.json"
    )
    payload = json.loads(obj["Body"].read())
    assert payload["status"] == expected_status
    if failure == "exception":
        assert payload["response"] is None
    else:
        assert payload["response"]["error"] == saved.response.error
        assert payload["response"]["error"]


@pytest.fixture
def task_logs(caplog):
    # Celery can route task logs to stderr without propagating to pytest's root handler.
    tasks.logger.addHandler(caplog.handler)
    try:
        yield caplog
    finally:
        tasks.logger.removeHandler(caplog.handler)


def test_failed_upload_preserves_original_error(
    db, result_bucket, monkeypatch, task_logs
):
    request = create_request()

    def fetch(*args, **kwargs):
        raise RuntimeError("Original processing failure")

    monkeypatch.setattr(tasks, "_fetch_resource", fetch)
    # Leave the fixture's bucket intact but configure an unavailable destination.
    monkeypatch.setenv("REQUEST_DETAILS_BUCKET_NAME", "unavailable-results-bucket")
    result = tasks.add_data_task.apply(args=[request], throw=False)
    assert isinstance(result.result, RuntimeError)
    assert str(result.result) == "Original processing failure"
    assert tasks._get_request(request["id"]).status == "FAILED"
    assert tasks._get_request(request["id"]).published_at is None
    assert "Failed to publish failed add-data request" in task_logs.text


def test_republication_records_latest_success(db, result_bucket, monkeypatch):
    request = create_request(saved=True)
    request_id = request["id"]
    earlier = datetime.datetime(2020, 1, 1, tzinfo=datetime.timezone.utc)
    with database.session_maker()() as session:
        saved = session.get(models.Request, request_id)
        saved.published_at = earlier
        session.commit()

    tasks.add_data_task.apply(args=[request], throw=True)
    published_at = tasks._get_request(request_id).published_at
    assert published_at > earlier
    assert published_at.utcoffset() == datetime.timedelta(0)

    # A later failed upload must not erase the last confirmed publication.
    monkeypatch.setenv("REQUEST_DETAILS_BUCKET_NAME", "unavailable-results-bucket")
    result = tasks.add_data_task.apply(args=[request], throw=False)
    assert result.failed()
    assert tasks._get_request(request_id).published_at == published_at


@pytest.mark.parametrize("processing_failed", [False, True])
def test_timestamp_failure_preserves_published_result(
    db, result_bucket, monkeypatch, task_logs, processing_failed
):
    request = create_request(saved=not processing_failed)
    original_commit = Session.commit

    def commit(session):
        if any(
            isinstance(row, models.Request)
            and inspect(row).attrs.published_at.history.has_changes()
            for row in session.dirty
        ):
            raise SQLAlchemyError("Publication timestamp write failed")
        original_commit(session)

    def fetch(*args, **kwargs):
        raise RuntimeError("Original processing failure")

    monkeypatch.setattr(Session, "commit", commit)
    monkeypatch.setattr(tasks, "_fetch_resource", fetch)
    result = tasks.add_data_task.apply(args=[request], throw=False)

    expected_status = "FAILED" if processing_failed else "COMPLETE"
    assert result.successful() is not processing_failed
    if processing_failed:
        assert isinstance(result.result, RuntimeError)
        assert str(result.result) == "Original processing failure"
    saved = tasks._get_request(request["id"])
    assert saved.status == expected_status
    assert saved.published_at is None
    obj = result_bucket.get_object(
        Bucket="request-results-test", Key=f"{request['id']}.json"
    )
    payload = json.loads(obj["Body"].read())
    assert payload["status"] == expected_status
    if not processing_failed:
        assert payload["response"]["data"] == summaries()
    assert "Failed to record publication timestamp" in task_logs.text


@pytest.mark.parametrize("pause_at", ["status_update", "s3_upload"])
def test_stale_failure_cannot_replace_newer_success(
    db, result_bucket, monkeypatch, pause_at
):
    request = create_request(saved=True)
    request_id = request["id"]
    successful_publications = []

    def complete_newer_delivery():
        # The newer delivery passed the broker guard while the request was
        # still PROCESSING, before the older delivery marked it FAILED.
        with mock.patch.object(
            tasks.GuardedAddDataTask, "validate_message", return_value="PROCESSING"
        ):
            result = tasks.add_data_task.apply(args=[request], throw=False)
        assert result.successful(), str(result.result)
        successful_publications.append(tasks._get_request(request_id).published_at)

    original_update = tasks._update_request_status
    original_publish = tasks.publish_request_result

    def update(request_id, status):
        if status == "FAILED" and pause_at == "status_update":
            complete_newer_delivery()
        original_update(request_id, status)

    def publish(request, status="COMPLETE"):
        if status == "FAILED" and pause_at == "s3_upload":
            complete_newer_delivery()
        return original_publish(request, status=status)

    monkeypatch.setattr(tasks, "_update_request_status", update)
    monkeypatch.setattr(tasks, "publish_request_result", publish)
    # Resume an older handler after another delivery has finished the same request.
    tasks.after_task_failure(
        task_id="older-delivery",
        exception=RuntimeError("Older delivery failed"),
        traceback=None,
        einfo=None,
        args=[request],
        sender=tasks.add_data_task,
    )

    saved = tasks._get_request(request_id)
    assert saved.status == "COMPLETE"
    assert successful_publications and successful_publications[0] is not None
    assert saved.published_at == successful_publications[0]
    obj = result_bucket.get_object(
        Bucket="request-results-test", Key=f"{request_id}.json"
    )
    payload = json.loads(obj["Body"].read())
    assert payload["status"] == "COMPLETE"
    assert payload["response"]["data"] == summaries()

    # A further redelivery must not reopen the completed database request.
    tasks.before_task(
        task_id="later-delivery", task=tasks.add_data_task, args=[request]
    )
    assert tasks._get_request(request_id).status == "COMPLETE"
