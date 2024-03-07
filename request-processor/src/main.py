import os

from functools import cache

import boto3

import logging

from signal_handler import SignalHandler

from database import SessionLocal

import crud

from request_model import schemas, models

import s3_transfer_manager

logger = logging.getLogger(__name__)

wait_seconds = int(os.getenv("SQS_RECEIVE_WAIT_SECONDS", default=10))

# Threshold for s3_transfer_manager to automatically use multipart download
max_file_size_mb = 30


@cache
def queue():
    sqs = boto3.resource("sqs")
    return sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])


def handle_messages():
    messages = queue().receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=wait_seconds)
    for message in messages:
        try:
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
            # time.sleep(20)

            # Set status to COMPLETE when processing successful
            update_request_status(request.id, 'COMPLETE')

            # Remove downloaded file
            os.remove(f"/tmp/{request_data.uploaded_file.uploaded_filename}")

            print(f"Processed message: {message.body}", flush=True)

        except Exception as e:
            print(f"exception while processing message: {repr(e)}", flush=True)
            logger.error(f"exception while processing message: {repr(e)}")
            # Set status to NEW since processing needs to be re-tried
            update_request_status(request.id, 'NEW')
            continue

        message.delete()


def update_request_status(request_id, status):
    with SessionLocal() as session:
        model = crud.get_request(session, request_id)
        model.status = status
        session.commit()
        session.flush()

# def db_session():
#     db = SessionLocal()
#     try:
#         yield db
#     finally:
#         db.close()


def main():
    signal_handler = SignalHandler()
    while not signal_handler.received_exit_signal:
        handle_messages()


if __name__ == "__main__":
    main()

