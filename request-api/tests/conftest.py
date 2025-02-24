import os

import alembic
import boto3
import pytest
from alembic.config import Config
from moto import mock_aws
from sqlalchemy_utils import database_exists, create_database, drop_database
from testcontainers.core.waiting_utils import wait_container_is_ready
from testcontainers.postgres import PostgresContainer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from request_model import schemas

os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"
os.environ["SQS_QUEUE_NAME"] = "request-queue"
os.environ["AWS_ACCESS_KEY_ID"] = "testing"
os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
os.environ["AWS_SECURITY_TOKEN"] = "testing"
os.environ["AWS_SESSION_TOKEN"] = "testing"
os.environ["CELERY_BROKER_URL"] = "memory://"
os.environ["DATABASE_URL"] = "sqlite://"

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
def db(postgres, test_dir):
    if not database_exists(os.environ["DATABASE_URL"]):
        print("Database does not exist, creating...")
        create_database(os.environ["DATABASE_URL"])
    # Apply migrations in postgres DB
    config = Config(os.path.realpath(f"{test_dir}/../../alembic.ini"))
    config.set_main_option(
        "script_location", os.path.realpath(f"{test_dir}/../../migrations")
    )
    alembic.command.upgrade(config, "head")

    engine = create_engine(os.environ["DATABASE_URL"])
    Session = sessionmaker(bind=engine)
    session = Session()

    yield session 
    
    session.close()
    drop_database(os.environ["DATABASE_URL"])


@pytest.fixture(scope="module")
def sqs_client():
    with mock_aws():
        yield boto3.resource("sqs")


@pytest.fixture(scope="module")
def sqs_queue(sqs_client):
    sqs_client.create_queue(QueueName=os.environ["SQS_QUEUE_NAME"])


@pytest.fixture(scope="module")
def test_dir(request):
    return os.path.dirname(request.module.__file__)


class Helpers:
    @staticmethod
    def build_request_create():
        return schemas.RequestCreate(
            params=schemas.CheckFileParams(
                collection="tree-preservation-order",
                dataset="tree",
                original_filename="something.csv",
                uploaded_filename="generated.csv",
            )
        )

    @staticmethod
    def request_create_dict(request: schemas.RequestCreate = None):
        if request is None:
            request = Helpers.build_request_create()
        return request.model_dump()


@pytest.fixture
def helpers():
    return Helpers
