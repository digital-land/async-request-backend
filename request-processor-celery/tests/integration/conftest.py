import os

import boto3
import pytest
from moto import mock_aws

os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
os.environ["DATABASE_URL"] = "sqlite://"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["REQUEST_FILES_BUCKET_NAME"] = "dluhc-data-platform-request-files-local"
os.environ["CELERY_BROKER_URL"] = "memory://"


@pytest.fixture(scope="module")
def s3_client():
    with mock_aws():
        yield boto3.client('s3')


@pytest.fixture(scope="module")
def s3_bucket(request, s3_client):
    s3_client.create_bucket(
        Bucket=os.environ["REQUEST_FILES_BUCKET_NAME"],
        CreateBucketConfiguration={
            'LocationConstraint': os.environ["AWS_DEFAULT_REGION"]
        }
    )
    test_dir = os.path.dirname(request.module.__file__)
    s3_client.put_object(
        Bucket=os.environ["REQUEST_FILES_BUCKET_NAME"],
        Key="B1E16917-449C-4FC5-96D1-EE4255A79FB1.jpg",
        Body=open(f"{test_dir}/bogdan-farca-CEx86maLUSc-unsplash.jpg", "rb")
    )
