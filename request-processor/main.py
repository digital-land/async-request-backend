import os

import boto3

import logging

from signal_handler import SignalHandler

logger = logging.getLogger(__name__)
sqs = boto3.resource("sqs")
queue = sqs.get_queue_by_name(QueueName=os.environ["SQS_QUEUE_NAME"])
wait_seconds = int(os.getenv("SQS_RECEIVE_WAIT_SECONDS", default=10))

if __name__ == "__main__":
    signal_handler = SignalHandler()
    while not signal_handler.received_signal:
        messages = queue.receive_messages(MaxNumberOfMessages=1, WaitTimeSeconds=wait_seconds)
        for message in messages:
            try:
                print(f"Received message: {message.body}", flush=True)
                logger.info(f"Received message: {message.body}")

            except Exception as e:
                print(f"exception while processing message: {repr(e)}", flush=True)
                logger.error(f"exception while processing message: {repr(e)}")
                continue

            message.delete()
