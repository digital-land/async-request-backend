"""Publish the add-data contract consumed by the Config repository."""

import os

import boto3
from botocore.exceptions import ClientError
from request_model.schemas import Request


def publish_request_result(request, status="COMPLETE"):
    """Publish API JSON; return False when an existing object prevents failure export."""
    bucket = os.environ.get("REQUEST_DETAILS_BUCKET_NAME", "").strip()
    if not bucket:
        raise ValueError("REQUEST_DETAILS_BUCKET_NAME must be configured")
    if request.type != "add_data":
        raise ValueError("Only add-data results can be published")

    payload = Request.model_validate(request).model_copy(update={"status": status})
    # A deterministic key makes re-publication of the saved result safe.
    # An older failure handler must never overwrite a newer successful delivery.
    conditions = {"IfNoneMatch": "*"} if status == "FAILED" else {}
    try:
        boto3.client("s3").put_object(
            Bucket=bucket,
            Key=f"{request.id}.json",
            Body=payload.model_dump_json().encode("utf-8"),
            ContentType="application/json",
            **conditions,
        )
    except ClientError as exc:
        if status == "FAILED" and exc.response["Error"]["Code"] == "PreconditionFailed":
            return False
        raise
    return True
