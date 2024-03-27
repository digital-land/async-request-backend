
import os

import alembic
import boto3
import pytest
from alembic.config import Config
from moto import mock_aws
from sqlalchemy_utils import database_exists, create_database, drop_database
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.postgres import PostgresContainer

os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
os.environ["SQS_QUEUE_NAME"] = "request-queue"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["CELERY_BROKER_URL"] = "memory://"

postgres_container = PostgresContainer("postgres:16.2-alpine")


@pytest.fixture(scope="module")
def postgres(request):
    postgres_container.start()

    def remove_postgres_container():
        postgres_container.stop()

    request.addfinalizer(remove_postgres_container)
    os.environ["DATABASE_URL"] = postgres_container.get_connection_url()
    wait_container_is_ready(postgres_container)


@pytest.fixture(scope="module")
def db(postgres):
    if not database_exists(os.environ["DATABASE_URL"]):
        print("Database does not exist, creating...")
        create_database(os.environ["DATABASE_URL"])
    # Apply migrations in postgres DB
    config = Config("alembic.ini")
    alembic.command.upgrade(config, "head")

    yield
    drop_database(os.environ["DATABASE_URL"])


@pytest.fixture(scope="module")
def sqs_client():
    with mock_aws():
        yield boto3.resource('sqs')


@pytest.fixture(scope="module")
def sqs_queue(sqs_client):
    sqs_client.create_queue(QueueName=os.environ["SQS_QUEUE_NAME"])
