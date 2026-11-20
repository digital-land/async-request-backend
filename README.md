# async-request-backend

Backend providing asynchronous request processing for frontends handling intensive workloads.

Completed and failed `add_data` results are published to `<request-id>.json` in
the private bucket configured by `REQUEST_DETAILS_BUCKET_NAME` on the worker.
Grant its runtime role `s3:PutObject` on `arn:aws:s3:::<bucket>/*`; Config's GitHub OIDC role
needs `s3:GetObject` (and corresponding KMS permissions if applicable).
Local Compose creates `dluhc-data-platform-request-details-local` in LocalStack.

`request.published_at` records the latest successful S3 upload time in UTC, for both
success and error results. It is NULL until publication is recorded; a failed later
upload preserves the previous timestamp. If recording the timestamp fails after
upload, the error is logged without failing the task or replacing the S3 result.
A NULL timestamp therefore does not prove the object is absent from S3.
It is database-only publication metadata,
not part of the request API/S3 JSON. No automatic publication retry is scheduled.
Apply migration `f3b152a01c01` before deploying the updated API or workers. Existing
rows remain NULL because their publication history is unknown.

Publication must succeed before the request becomes COMPLETE, including when the
workflow returns an error response. Raised exceptions are marked FAILED first,
then the failure handler attempts to publish that saved request. If this upload
also fails, it is logged without replacing the original error. Unexpected failures
may have no saved response; the JSON then contains `response: null`.
Overlapping deliveries are possible. Failure uploads use S3 `If-None-Match: *`
so they cannot replace an existing object; a successful upload can replace a
failure result. Skipped failure uploads do not update `published_at`. Completed
add-data requests cannot be downgraded by a late failure or reopened by redelivery.
Re-delivering a task with a saved response republishes it without reallocating
entities. Check requests are not exported. The JSON contains the
full `GET /requests/{id}` response using the shared `Request` schema: identity,
timestamps, submission ID, all parameters and response data. Contact fields belong
to the separate Add Data Submission record and are not in this schema. Detailed
rows from `/response-details` are also separate. Returned errors retain the API's
COMPLETE-with-error convention; raised exceptions export FAILED. Config rejects
both non-COMPLETE requests and requests containing `response.error`.

Deploy the bucket, permissions and worker configuration before Config's S3 reader.
Publish saved results for older completed requests before confirming them in
Manage, and retain objects long enough for delayed confirmation and reruns.

This repository implements open design proposal 001:

https://digital-land.github.io/technical-documentation/architecture/design/proposals/001-publish-async/index.html

# Getting Started

## Prerequisites
Ensure you have Docker installed.

## Local running

Clone the Repository
```shell
    git clone https://github.com/digital-land/async-request-backend.git
    cd async-request-backend
```
Create and Activate Virtual Environment
```shell
    python -m venv venv
    source venv/bin/activate
```
Install Dependencies

```shell
    pip install -r requirements.txt
```
Initialize the project
```shell
    make init
```

A docker compose setup has been configured to run the async request backend.  This setup runs a Python/FastAPI for receiving requests, a Postgres database to store requests, an SQS queue using the excellent [Localstack](https://www.localstack.cloud/) container to trigger processing and a basic Python app to process the requests.

You can run the docker compose stack by executing the following command:

```shell
docker-compose up -d --no-deps --build
```
To view service logs, use:
```shell
docker compose logs -f <service_name>
# Example
docker compose logs -f request-processor
```
To inspect the database tables and records via CLI, execute:
```shell
docker-compose exec request-db psql -U postgres -d request_database
```
### Local authentication

Auth is on by default. Local Compose sets `AUTHENTICATION_ON=false`;

Run `docker compose up -d --build` locally. To enable auth, configure the JWT keys
and run `AUTHENTICATION_ON=true docker compose up -d --build`.

### Request API

To create a new request, you can post via curl:

```shell
curl --location 'http://localhost:8000/requests' \
--header 'Content-Type: application/json' \
--data-raw '{
    "user_email": "someone@email.com"
}'
```

Alternatively, a good way to test local service (with Postman) is go to https://provide.planning.data.gov.uk/ select a dataset and use it's Enpoint URL. Fill in the check_url_request.json template in request-api folder with this endpoint and create a POST to http://localhost:8000/requests with this json in the body.

You can then go to GET http://localhost:8000/requests/{request.id} to see the results. http://localhost:8000/requests/{request.id}/response-details provides the breakdown that the provide front end builds of.

If changes are made, you can use the production async-backend at (http://production-pub-async-api-lb-636110663.eu-west-2.elb.amazonaws.com) with a POST to /requests. Then compare the results of production to what your localhost machine is producing.

### SQS

To create an SQS queue, you can use the AWS CLI:

```shell
aws --endpoint-url=http://localhost:4566 sqs create-queue --queue-name async-request-queue --region eu-west-2 --output table | cat
```

You can place a test message on the queue like so:

```shell
aws --endpoint-url=http://localhost:4566 sqs send-message --queue-url http://sqs.eu-west-2.localhost.localstack.cloud:4566/000000000000/async-request-queue --message-body "Hello World"
```

You can read the test message from the queue like so:

```shell
aws --endpoint-url=http://localhost:4566 sqs receive-message --queue-url http://sqs.eu-west-2.localhost.localstack.cloud:4566/000000000000/async-request-queue
```

To delete the message, you'll need to run the following command making use of the `receipt-handle` parameter associated with the message, e.g.

```shell
aws --endpoint-url=http://localhost:4566 sqs delete-message --queue-url http://sqs.eu-west-2.localhost.localstack.cloud:4566/000000000000/async-request-queue \
 --receipt-handle "MzczYmIzODAtNmM2YS00ZDAyLThkOWYtMTgyYjcyYzZlOTA0IGFybjphd3M6c3FzOmV1LXdlc3QtMjowMDAwMDAwMDAwMDA6YXN5bmMtcmVxdWVzdC1xdWV1ZSBhMjk1ZGVhNi1jNGI2LTQ5ZDQtODEyNC0yNjMwMjFhOWZlOTMgMTcwNzgzNzc1My43NzMzOTk4"
```

## Managing Dependencies

To include new dependencies, update the requirements.in file with the desired packages. Afterward, run the following command to generate an updated requirements.txt file, which includes both direct and dependent dependencies:
    ```
    pip-compile -r requirements/requirements.in
    ```

This ensures that your project accurately reflects its dependencies, including any transitive dependencies required by the newly added packages.

## Debug

Because this is a monorepo, debugging can be challenging. The primary approach is to review the logs for each sub‑repository through Docker. However, if you need to perform breakpoint debugging, a debug configuration is available under the .vscode/ folder, currently only set up for the request-processor service.
