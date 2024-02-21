import os

import time

import boto3

import logging

from signal_handler import SignalHandler

from database import SessionLocal

import crud

from request_model import schemas, models

from notifications_python_client.notifications import NotificationsAPIClient

import s3_transfer_manager

logger = logging.getLogger(__name__)

sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])
wait_seconds = int(os.getenv("SQS_RECEIVE_WAIT_SECONDS", default=10))

notifications_client = NotificationsAPIClient(
    base_url=os.environ["NOTIFY_BASE_URL"],
    api_key=os.environ["NOTIFY_API_KEY"]
)
template_id = os.environ["NOTIFY_TEMPLATE_ID"]

# Threshold for s3_transfer_manager to automatically use multipart download
max_file_size_mb = 30


def handle_messages():
    messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=wait_seconds)
    for message in messages:
        try:
            print(message.body, flush=True)
            request = schemas.Request.model_validate_json(message.body)
            request_data = models.RequestData.model_validate(request.data)

            # Set status to PROCESSING
            update_request_status(request.id, 'PROCESSING')

            s3_transfer_manager.download_with_default_configuration(
                os.environ["REQUEST_FILES_BUCKET_NAME"],
                request_data.uploaded_file.uploaded_filename,
                f"/tmp/{request_data.uploaded_file.uploaded_filename}",
                max_file_size_mb
            )

            # Future: call pipeline here and write error summary to database
            # Simulate processing delay
            time.sleep(20)

            # Set status to COMPLETE when processing successful
            update_request_status(request.id, 'COMPLETE')

            # TODO: Consider how we will handle failures to send email notification
            notifications_client.send_email_notification(
                email_address=request.user_email,  # required string
                template_id=template_id,  # required UUID string
                reference=request.id
            )

            # Remove downloaded file
            os.remove(f"/tmp/{request_data.uploaded_file.uploaded_filename}")

            print(f"Received message: {message.body}", flush=True)
            logger.info(f"Received message: {message.body}")

        except Exception as e:
            print(f"exception while processing message: {repr(e)}", flush=True)
            logger.error(f"exception while processing message: {repr(e)}")
            # Set status to NEW since processing needs to be re-tried
            update_request_status(request.id, 'NEW')
            continue

        message.delete()


def update_request_status(requestId, status):
    with SessionLocal() as session:
        model = crud.get_request(session, requestId)
        model.status = status
        session.commit()
        session.flush()

if __name__ == "__main__":
    signal_handler = SignalHandler()
    while not signal_handler.received_signal:
        handle_messages()

