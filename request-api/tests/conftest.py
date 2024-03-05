
import os

import boto3
import pytest
from moto import mock_aws

os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
os.environ["DATABASE_URL"] = "sqlite://"
os.environ["SQS_QUEUE_NAME"] = "request-queue"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"


@pytest.fixture(scope="module")
def sqs_client():
    with mock_aws():
        yield boto3.resource('sqs')


@pytest.fixture(scope="module")
def sqs_queue(sqs_client):
    sqs_client.create_queue(QueueName=os.environ["SQS_QUEUE_NAME"])
