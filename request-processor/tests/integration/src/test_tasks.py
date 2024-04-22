import datetime
import time
import pytest
import json
import shutil
import os
import database
from request_model import models, schemas
from src.tasks import check_datafile


@pytest.fixture(scope="module")
def test_data_dir(test_dir):
    return os.path.realpath(f"{test_dir}/../../data")


def test_check_datafile(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
    test_data_dir,
):
    """
    This function tests the check_datafile task for an valid file validation.

    Args:
        mocker: Mocking framework for Python.
        celery_app: Celery application.
        celery_worker: Celery worker.
        s3_bucket: S3 bucket.
        db: Database.
        mock_directories: Mocked directories.
        mock_fetch_pipeline_csvs: Mock function for fetching pipeline CSVs.
        test_data_dir: Directory containing test data.
    """
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_file,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=schemas.CheckFileParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            original_filename="article-direction-area.csv",
            uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
        ).model_dump(),
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
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    print("destination_organisation_csv=" + destination_organisation_csv)
    shutil.copy(source_organisation_csv, destination_organisation_csv)
    print(
        "destination_organisation_csv exists? "
        + str(os.path.exists(destination_organisation_csv))
    )
    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs("article-4-direction-area", request.id),
    )
    # Convert mock directory paths to strings
    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

    mock_directories_json = json.dumps(mock_directories_str)
    check_datafile_task = celery_app.register_task(check_datafile)
    check_datafile_task.delay(request.model_dump(), directories=mock_directories_json)
    _wait_for_request_status(request.id, "COMPLETE")


def test_check_datafile_invalid(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
    test_data_dir,
):
    """
    This function tests the check_datafile task for an invalid file validation.

    Args:
        mocker: Mocking framework for Python.
        celery_app: Celery application.
        celery_worker: Celery worker.
        s3_bucket: S3 bucket.
        db: Database.
        mock_directories: Mocked directories.
        mock_fetch_pipeline_csvs: Mock function for fetching pipeline CSVs.
        test_data_dir: Directory containing test data.
    """
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_file,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=schemas.CheckFileParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            original_filename="invalid.csv",
            uploaded_filename="invalid",
        ).model_dump(),
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
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    shutil.copy(source_organisation_csv, destination_organisation_csv)

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs("article-4-direction-area", request.id),
    )
    # Convert mock directory paths to strings
    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

    mock_directories_json = json.dumps(mock_directories_str)
    check_datafile_task = celery_app.register_task(check_datafile)
    check_datafile_task.delay(request.model_dump(), directories=mock_directories_json)
    _wait_for_request_status(request.id, "FAILED")


def test_check_datafile_url(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
    test_data_dir,
):
    """
    This function tests the check_datafile task for a valid URL validation.

    Args:
        mocker: Mocking framework for Python.
        celery_app: Celery application.
        celery_worker: Celery worker.
        s3_bucket: S3 bucket.
        db: Database.
        mock_directories: Mocked directories.
        mock_fetch_pipeline_csvs: Mock function for fetching pipeline CSVs.
        test_data_dir: Directory containing test data.
    """
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_url,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=schemas.CheckUrlParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            url="exampleurl.csv",
        ).model_dump(),
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
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    print("destination_organisation_csv=" + destination_organisation_csv)
    shutil.copy(source_organisation_csv, destination_organisation_csv)
    print(
        "destination_organisation_csv exists? "
        + str(os.path.exists(destination_organisation_csv))
    )

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs("article-4-direction-area", request.id),
    )
    # Convert mock directory paths to strings
    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

    content = '{"type":"FeatureCollection","properties":{"exceededTransferLimit":true}, \
    "features":[{"type":"Feature","id":1,"geometry":{"type":"Point", \
    "coordinates":[-1.59153574212325,54.9392094142866]}, \
    "properties": {"reference": "CA01","name": "Ashleworth Conservation Area"}}]}'

    # Encode the JSON string to bytes using UTF-8 encoding
    content_bytes = content.encode("utf-8")
    # Mock the result of utils.get_request
    mocker.patch(
        "application.core.utils.get_request", return_value=(None, content_bytes)
    )

    mock_directories_json = json.dumps(mock_directories_str)
    check_datafile_task = celery_app.register_task(check_datafile)
    check_datafile_task.delay(request.model_dump(), directories=mock_directories_json)
    _wait_for_request_status(request.id, "COMPLETE")


def test_check_datafile_url_invalid(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
    test_data_dir,
):
    """
    This function tests the check_datafile task for an invalid URL validation.

    Args:
        mocker: Mocking framework for Python.
        celery_app: Celery application.
        celery_worker: Celery worker.
        s3_bucket: S3 bucket.
        db: Database.
        mock_directories: Mocked directories.
        mock_fetch_pipeline_csvs: Mock function for fetching pipeline CSVs.
        test_data_dir: Directory containing test data.
    """
    request_model = models.Request(
        type=schemas.RequestTypeEnum.check_url,
        created=datetime.datetime.now(),
        modified=datetime.datetime.now(),
        status="NEW",
        params=schemas.CheckUrlParams(
            collection="article-4-direction",
            dataset="article-4-direction-area",
            url="exampleurl.csv",
        ).model_dump(),
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
    # Convert mock directory paths to strings
    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

    log = '{"status": "404", "message": "Unable to process"}'

    # Mock the result of utils.get_request
    mocker.patch("application.core.utils.get_request", return_value=(log, None))

    mock_directories_json = json.dumps(mock_directories_str)
    check_datafile_task = celery_app.register_task(check_datafile)
    check_datafile_task.delay(request.model_dump(), directories=mock_directories_json)
    _wait_for_request_status(request.id, "FAILED")


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
                print(
                    f"Waiting {interval_seconds} second(s) for expected status of "
                    f"{expected_status} on request {request_id}"
                )

    pytest.fail(
        f"Expected status of {expected_status} for request {request_id} but actual status was {actual_status}"
    )
