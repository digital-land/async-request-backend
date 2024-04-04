import datetime
import time
import pytest
import json
import shutil
import os
import database
from request_model import models, schemas
from tasks import check_datafile

# TODO: Happy path for check_url

# TODO: Unhappy path for unknown request type


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
        response=None
    )
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    print("destination_organisation_csv="+destination_organisation_csv)
    shutil.copy(source_organisation_csv, destination_organisation_csv)
    print("destination_organisation_csv exists? "+str(os.path.exists(destination_organisation_csv)))

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs,
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
        response=None
    )
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    shutil.copy(source_organisation_csv, destination_organisation_csv)

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs,
    )
    # Convert mock directory paths to strings
    mock_directories_str = {
        key: str(path) for key, path in mock_directories._asdict().items()
    }

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
