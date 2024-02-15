import os

import boto3

import logging

from signal_handler import SignalHandler

from database import SessionLocal

import crud

from request_model import schemas

from notifications_python_client.notifications import NotificationsAPIClient

logger = logging.getLogger(__name__)

sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])
wait_seconds = int(os.getenv("SQS_RECEIVE_WAIT_SECONDS", default=10))

notifications_client = NotificationsAPIClient(
    base_url=os.environ["NOTIFY_BASE_URL"],
    api_key=os.environ["NOTIFY_API_KEY"]
)
template_id = os.environ["NOTIFY_TEMPLATE_ID"]


def handle_messages():
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=wait_seconds)
    for message in messages:
        try:
            request = schemas.Request.model_validate_json(message.body)

            # TODO: Download file from S3
            # Future: call pipeline and write error summary to database

            with SessionLocal() as session:
                model = crud.get_request(session, request.id)
                model.status = 'COMPLETE'
                session.commit()
                session.flush()

            # TODO: notify user by email (GOV UK Notify mock)
            notifications_client.send_email_notification(
                email_address=request.user_email,  # required string
                template_id=template_id,  # required UUID string
                reference=request.id
            )

            print(f"Received message: {message.body}", flush=True)
            logger.info(f"Received message: {message.body}")

        except Exception as e:
            print(f"exception while processing message: {repr(e)}", flush=True)
            logger.error(f"exception while processing message: {repr(e)}")
            continue

        message.delete()


if __name__ == "__main__":
    signal_handler = SignalHandler()
    while not signal_handler.received_signal:
        handle_messages()

