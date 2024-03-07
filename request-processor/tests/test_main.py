import json
import os

import pytest
from sqlalchemy import create_engine, StaticPool
from sqlalchemy.orm import sessionmaker

import main
from request_model import models

from main import handle_messages

engine = create_engine(
    os.environ.get('DATABASE_URL'),
    connect_args={"check_same_thread": False},
    poolclass=StaticPool,
    echo=True
)

models.Base.metadata.create_all(bind=engine)

main.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def test_handle_messages(sqs_queue, s3_bucket):
    db_request = _setup_request(sqs_queue)
    handle_messages()

    messages = sqs_queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=1)
    assert len(messages) == 0

    with main.SessionLocal() as session:
        request = (session.query(models.Request)
                   .filter(models.Request.id == db_request.id)
                   .first())
        assert request.status == 'COMPLETE'


def test_handle_messages_when_no_messages_available(sqs_queue):
    try:
        handle_messages()
    except Exception:
        pytest.fail("Unexpected Exception when no messages available on SQS queue..")


def _setup_request(sqs_queue):
    db_request = models.Request(
        user_email="chris.cundill@tpximpact.com",
        status='NEW',
        data=models.RequestData(
            uploaded_file=models.UploadedFile(
                original_filename="bogdan-farca-CEx86maLUSc-unsplash.jpg",
                uploaded_filename="B1E16917-449C-4FC5-96D1-EE4255A79FB1.jpg"
            ),
            response=None
        ).model_dump()
    )
    with main.SessionLocal() as session:
        session.add(db_request)
        session.commit()
        session.refresh(db_request)
    sqs_queue.send_message(
        MessageBody=json.dumps({
            "user_email": "chris.cundill@tpximpact.com",
            "id": db_request.id,
            "status": "NEW",
            "created": "2024-03-06T16:29:46.182052Z",
            "modified": "2024-03-06T16:29:46.182052Z",
            "data": {
                "uploaded_file": {
                    "original_filename": "bogdan-farca-CEx86maLUSc-unsplash.jpg",
                    "uploaded_filename": "B1E16917-449C-4FC5-96D1-EE4255A79FB1.jpg"
                },
                "response": None
            }
        }),
        MessageAttributes={}
    )
    return db_request

