import os
from typing import Callable, Any

import boto3

import logging

from signal_handler import SignalHandler

from sqlalchemy.orm import Session

from database import SessionLocal, engine

import crud

from request_model import schemas

logger = logging.getLogger(__name__)
sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])
wait_seconds = int(os.getenv("SQS_RECEIVE_WAIT_SECONDS", default=10))


def with_session(f: Callable[[Session], Any]):
    db = SessionLocal()
    try:
        return f(db)

    finally:
        db.close()


if __name__ == "__main__":
    signal_handler = SignalHandler()
    while not signal_handler.received_signal:
        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=wait_seconds)
        for message in messages:
            try:
                request = schemas.Request.model_validate_json(message.body)
                
                # TODO: Grab file from S3
                # Future: call pipeline and write error summary to database

                with SessionLocal() as session:
                    print(f"Updating DB for request: {request.id}", flush=True)
                    model = crud.get_request(session, request.id)
                    model.status = 'COMPLETE'
                    session.commit()
                    session.flush()

                # TODO: notify user by email (GOV UK Notify mock)

                print(f"Received message: {message.body}", flush=True)
                logger.info(f"Received message: {message.body}")

            except Exception as e:
                print(f"exception while processing message: {repr(e)}", flush=True)
                logger.error(f"exception while processing message: {repr(e)}")
                continue

            message.delete()
