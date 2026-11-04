"""Shared database and queue dependencies for API routes."""

import logging
import os
from datetime import datetime

import boto3
from botocore.exceptions import ClientError, BotoCoreError
from slack_sdk import WebClient
from sqlalchemy.exc import SQLAlchemyError

from database import session_maker


def send_slack_alert(message):
    slack_token = os.environ.get("SLACK_BOT_TOKEN", "")
    slack_channel = os.environ.get("SLACK_CHANNEL", "")
    if not slack_token or not slack_channel:
        logging.warning("Slack token or channel is missing.")
        return
    client = WebClient(token=slack_token)
    bot_name = "SQS and DB"
    client.chat_postMessage(channel=slack_channel, text=message, username=bot_name)


def is_connection_restored(last_attempt_time, max_retry_duration=60):
    current_time = datetime.now().timestamp()
    last_attempt_timestamp = last_attempt_time.timestamp()
    return (current_time - last_attempt_timestamp) > max_retry_duration


def _get_db():
    retries = 5
    for attempt in range(retries):
        try:
            db = session_maker()()
            try:
                yield db
            finally:
                db.close()
            return
        except SQLAlchemyError as e:
            logging.exception(f"Database connection failed (Attempt {attempt+1}): {e}")
            if is_connection_restored(datetime.now()):
                break
    send_slack_alert("DB connection issue detected in async-request-backend..")


def _get_sqs_client():
    retries = 5
    for attempt in range(retries):
        try:
            logging.info(f"SQS client successfully connected on attempt {attempt}.")
            return boto3.client("sqs")
        except (ClientError, BotoCoreError) as e:
            logging.exception(
                f"SQS connection failed on attempt {attempt}. Retrying...Error: {e}"
            )
            if is_connection_restored(datetime.now()):
                break
    send_slack_alert("SQS connection issue detected in async-request-backend..")
