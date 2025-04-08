import os

import boto3
import pytest
from moto import mock_aws
from testcontainers.postgres import PostgresContainer
import csv
import database
from request_model import models
from collections import namedtuple

os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["REQUEST_FILES_BUCKET_NAME"] = "dluhc-data-platform-request-files-local"
os.environ["CELERY_BROKER_URL"] = "memory://"

postgres_container = PostgresContainer("postgres:16.2-alpine")


@pytest.fixture(scope="module")
def postgres(request):
    postgres_container.start()

    def remove_postgres_container():
        postgres_container.stop()

    request.addfinalizer(remove_postgres_container)
    os.environ["DATABASE_URL"] = postgres_container.get_connection_url()


@pytest.fixture(scope="module")
def db(postgres):
    models.Base.metadata.create_all(bind=database.engine())


@pytest.fixture(scope="module")
def s3_client():
    with mock_aws():
        yield boto3.client("s3")


@pytest.fixture(scope="module")
def test_dir(request):
    return os.path.dirname(request.module.__file__)


@pytest.fixture(scope="module")
def project_dir(test_dir):
    print("PROJ DIR:: ", os.path.realpath(f"{test_dir}"))
    return os.path.realpath(f"{test_dir}/../../")


@pytest.fixture(scope="module")
def s3_bucket(test_data_dir, s3_client):
    s3_client.create_bucket(
        Bucket=os.environ["REQUEST_FILES_BUCKET_NAME"],
        CreateBucketConfiguration={
            "LocationConstraint": os.environ["AWS_DEFAULT_REGION"]
        },
    )
    s3_client.put_object(
        Bucket=os.environ["REQUEST_FILES_BUCKET_NAME"],
        Key="492f15d8-45e4-427e-bde0-f60d69889f40",
        Body=open(f"{test_data_dir}/files/article-direction-area.csv", "rb"),
    )


@pytest.fixture
def mock_directories(tmpdir, project_dir):
    Directories = namedtuple(
        "Directories",
        [
            "COLLECTION_DIR",
            "CONVERTED_DIR",
            "ISSUE_DIR",
            "COLUMN_FIELD_DIR",
            "TRANSFORMED_DIR",
            "DATASET_RESOURCE_DIR",
            "PIPELINE_DIR",
            "SPECIFICATION_DIR",
            "CACHE_DIR",
        ],
    )
    var_dir = tmpdir.mkdir("var")
    return Directories(
        COLLECTION_DIR=tmpdir.mkdir("collection"),
        CONVERTED_DIR=tmpdir.mkdir("converted"),
        ISSUE_DIR=tmpdir.mkdir("issue"),
        COLUMN_FIELD_DIR=var_dir.mkdir("column-field"),
        TRANSFORMED_DIR=tmpdir.mkdir("transformed"),
        DATASET_RESOURCE_DIR=var_dir.mkdir("dataset-resource"),
        PIPELINE_DIR=tmpdir.mkdir("pipeline"),
        SPECIFICATION_DIR="tests/data/specification",
        CACHE_DIR=var_dir.mkdir("cache"),
    )


@pytest.fixture
def mock_fetch_pipeline_csvs(mock_directories):
    def _mock_fetch_pipeline_csvs(dataset_name, request_id):
        # create a mock column.csv in the pipeline folder
        pipeline_dir = os.path.join(
            mock_directories.PIPELINE_DIR, dataset_name, request_id
        )

        os.makedirs(pipeline_dir, exist_ok=True)
        mock_column_csv = os.path.join(pipeline_dir, "column.csv")
        row = {
            "dataset": dataset_name,
            "": "",
            "resource": "",
            "column": "id",
            "field": "reference",
        }
        fieldnames = row.keys()
        with open(mock_column_csv, "w") as f:
            dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
            dictwriter.writeheader()
            dictwriter.writerow(row)

        mock_transform_csv = os.path.join(pipeline_dir, "transform.csv")
        row = {
            "dataset": dataset_name,
            "replacement-field": "name",
            "field": "SiteReference",
        }
        fieldnames = row.keys()
        with open(mock_transform_csv, "w") as f:
            dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
            dictwriter.writeheader()
            dictwriter.writerow(row)

    return _mock_fetch_pipeline_csvs


@pytest.fixture
def mock_extract_dataset_field_rows(mock_directories):
    def _mock_extract_dataset_field_rows(dataset_name):
        os.makedirs(mock_directories.SPECIFICATION_DIR, exist_ok=True)
        mock_field_csv = os.path.join(
            mock_directories.SPECIFICATION_DIR, "dataset-field.csv"
        )
        rows = [
            {
                "dataset": dataset_name,
                "field": "entry-date",
                "field-dataset": "",
                "guidance": "",
                "hint": "",
            },
            {
                "dataset": dataset_name,
                "field": "geometry",
                "field-dataset": "",
                "guidance": "",
                "hint": "",
            },
            {
                "dataset": dataset_name,
                "field": "reference",
                "field-dataset": "",
                "guidance": "",
                "hint": "",
            },
        ]
        fieldnames = rows[0].keys()
        with open(mock_field_csv, "w") as f:
            dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
            dictwriter.writeheader()
            dictwriter.writerows(rows)

    return _mock_extract_dataset_field_rows
