import os

broker_transport_options = {
    "region": os.environ["CELERY_BROKER_REGION"],
    "is_secure": os.environ.get("CELERY_BROKER_IS_SECURE", "false").lower() == "true"
}

broker_connection_retry_on_startup = True
