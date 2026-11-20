import json
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import boto3
from botocore.exceptions import ClientError
from moto import mock_aws

from src import request_results


@pytest.fixture
def result_request():
    return SimpleNamespace(
        id="request-123",
        type="add_data",
        status="PROCESSING",
        created=datetime(2026, 11, 19, tzinfo=timezone.utc),
        modified=datetime(2026, 11, 19, 1, tzinfo=timezone.utc),
        add_data_submission_id="submission-123",
        contact_name="Not part of the request API",
        contact_email="private@example.com",
        params={
            "type": "add_data",
            "collection": "conservation-area",
            "dataset": "conservation-area",
            "organisation": "local-authority:ABC",
            "column_mapping": {"title": "name"},
            "authoritative": False,
            "authoritative_organisation": "local-authority:XYZ",
            "endpoint_parameters": {"token": "secret"},
        },
        response=SimpleNamespace(
            error=None,
            data={
                "pipeline-summary": {"new-entities": [{"entity": 123}]},
                "endpoint-summary": {"existing_endpoint_entry": {"endpoint": "abc"}},
                "source-summary": {},
                "additional-result": {"value": "preserved"},
            },
        ),
    )


@pytest.fixture
def results_bucket(monkeypatch):
    monkeypatch.setenv("REQUEST_DETAILS_BUCKET_NAME", "request-results-test")
    with mock_aws():
        client = boto3.client("s3")
        client.create_bucket(
            Bucket="request-results-test",
            CreateBucketConfiguration={"LocationConstraint": "eu-west-2"},
        )
        yield client


def test_publishes_full_request_json_and_can_repeat(result_request, results_bucket):
    for _ in range(2):
        request_results.publish_request_result(result_request)
    obj = results_bucket.get_object(
        Bucket="request-results-test", Key="request-123.json"
    )
    assert obj["ContentType"] == "application/json"
    payload = json.loads(obj["Body"].read())
    assert payload["id"] == "request-123"
    assert payload["type"] == "add_data"
    assert payload["status"] == "COMPLETE"
    assert payload["created"] == "2026-11-19T00:00:00Z"
    assert payload["modified"] == "2026-11-19T01:00:00Z"
    assert payload["add_data_submission_id"] == "submission-123"
    assert payload["params"]["endpoint_parameters"] == {"token": "secret"}
    assert payload["params"]["column_mapping"] == {"title": "name"}
    assert payload["response"]["data"] == result_request.response.data
    assert "contact_name" not in payload
    assert "contact_email" not in payload
    assert result_request.status == "PROCESSING"


def test_missing_bucket_fails(result_request, monkeypatch):
    monkeypatch.delenv("REQUEST_DETAILS_BUCKET_NAME", raising=False)
    with pytest.raises(ValueError, match="REQUEST_DETAILS_BUCKET_NAME"):
        request_results.publish_request_result(result_request)


@pytest.mark.parametrize("status", ["COMPLETE", "FAILED"])
def test_upload_failure_propagates(result_request, results_bucket, status):
    results_bucket.delete_bucket(Bucket="request-results-test")
    with pytest.raises(ClientError):
        request_results.publish_request_result(result_request, status=status)


def test_check_results_not_published(result_request, results_bucket):
    result_request.type = "check_url"
    with pytest.raises(ValueError):
        request_results.publish_request_result(result_request)
    assert (
        results_bucket.list_objects_v2(Bucket="request-results-test")["KeyCount"] == 0
    )


@pytest.mark.parametrize("status", ["COMPLETE", "FAILED"])
def test_publishes_error_response(result_request, results_bucket, status):
    result_request.response.data = None
    result_request.response.error = {"errMsg": "Could not process resource"}
    request_results.publish_request_result(result_request, status=status)
    obj = results_bucket.get_object(
        Bucket="request-results-test", Key="request-123.json"
    )
    payload = json.loads(obj["Body"].read())
    assert payload["status"] == status
    assert payload["response"] == {
        "data": None,
        "error": {"errMsg": "Could not process resource"},
    }


def test_success_replaces_failure_but_late_failure_cannot_replace_success(
    result_request, results_bucket
):
    request_results.publish_request_result(result_request, status="FAILED")
    request_results.publish_request_result(result_request, status="COMPLETE")
    request_results.publish_request_result(result_request, status="FAILED")
    obj = results_bucket.get_object(
        Bucket="request-results-test", Key="request-123.json"
    )
    assert json.loads(obj["Body"].read())["status"] == "COMPLETE"
