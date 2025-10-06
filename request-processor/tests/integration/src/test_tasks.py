import datetime
import time
import pytest
import json
import shutil
import os
import database
from request_model import models, schemas
from src.tasks import check_datafile
from digital_land.collect import FetchStatus


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
        ("invalid.csv", "invalid", "FAILED"),
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
    "test_name, url, get_request_return_value, expected_status, mock_response",
    [
        (
            "valid_url",
            "exampleurl.csv",
            (
                None,
                '{"type":"FeatureCollection","properties":{"exceededTransferLimit":true}, "features":[{"type":"Feature","id":1,"geometry":{"type":"Point", "coordinates":[-1.59153574212325,54.9392094142866]}, "properties": {"reference": "CA01","name": "Ashleworth Conservation Area"}}]}'.encode(  # noqa
                    "utf-8"
                ),
            ),
            "COMPLETE",
            True,
        ),
        (
            "invalid_url",
            "exampleurl.csv",
            ('{"status": "404", "message": "Unable to process"}', None),
            "FAILED",
            False,
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
):
    """
    This function tests the check_datafile task for URL validation.

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

    def mock_collector_fetch(self, url, plugin=None):
        if expected_status == "COMPLETE":
            resource_dir = self.resource_dir
            resource_dir.mkdir(parents=True, exist_ok=True)
            mock_file = resource_dir / "mock_resource_hash"
            mock_file.write_text("mock csv data")
            return FetchStatus.OK
        else:
            return FetchStatus.FAILED

    mocker.patch("digital_land.collect.Collector.fetch", mock_collector_fetch)

    _register_and_check_request(
        mock_directories_str, celery_app, request, expected_status
    )


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
    mock_directories_json = json.dumps(mock_directories_str)
    check_datafile_task = celery_app.register_task(check_datafile)
    check_datafile_task.delay(request.model_dump(), directories=mock_directories_json)
    _wait_for_request_status(request.id, expected_status)
