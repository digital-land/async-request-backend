import datetime
import time
from collections import namedtuple
import pytest
import csv
import json
import shutil
import os
import database
from request_model import models, schemas
from tasks import check_datafile


@pytest.fixture
def mock_directories(tmpdir):
    Directories = namedtuple(
        "Directories",
        [
            "COLLECTION_DIR",
            "CONVERTED_DIR",
            "ISSUE_DIR",
            "COLUMN_FIELD_DIR",
            "TRANSFORMED_DIR",
            "FLATTENED_DIR",
            "DATASET_DIR",
            "DATASET_RESOURCE_DIR",
            "PIPELINE_DIR",
            "SPECIFICATION_DIR",
            "CACHE_DIR",
        ],
    )
    VAR_DIR = tmpdir.mkdir("var")
    return Directories(
        COLLECTION_DIR=tmpdir.mkdir("collection"),
        CONVERTED_DIR=tmpdir.mkdir("converted"),
        ISSUE_DIR=tmpdir.mkdir("issue"),
        COLUMN_FIELD_DIR=VAR_DIR.mkdir("column-field"),
        TRANSFORMED_DIR=tmpdir.mkdir("transformed"),
        FLATTENED_DIR=tmpdir.mkdir("flattened"),
        DATASET_DIR=tmpdir.mkdir("dataset"),
        DATASET_RESOURCE_DIR=VAR_DIR.mkdir("dataset-resource"),
        PIPELINE_DIR=tmpdir.mkdir("pipeline"),
        SPECIFICATION_DIR="specification",
        CACHE_DIR=VAR_DIR.mkdir("cache"),
    )


@pytest.fixture
def mock_fetch_pipeline_csvs(tmpdir, mock_directories):
    # create a mock column.csv in the pipeline folder
    mock_column_csv = os.path.join(tmpdir, mock_directories.PIPELINE_DIR, "column.csv")
    row = {
        "dataset": "article-4-direction-area",
        "resource": "",
        "column": "wkt",
        "field": "geometry",
    }
    fieldnames = row.keys()
    with open(mock_column_csv, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)


def test_check_datafile(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
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
    )
    source_organisation_csv = "tests/data/csvs/organisation.csv"
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
    _wait_for_request_status(request.id, "COMPLETE")


def test_check_datafile_invalid(
    mocker,
    celery_app,
    celery_worker,
    s3_bucket,
    db,
    mock_directories,
    mock_fetch_pipeline_csvs,
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
    )
    source_organisation_csv = "tests/data/csvs/organisation.csv"
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
