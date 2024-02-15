# async-request-backend

Backend providing asynchronous request processing for frontends handling intensive workloads.

This repository currently hosts proof of concept code as part of open design proposal 001:

https://digital-land.github.io/technical-documentation/architecture/design/proposals/001-publish-async/index.html

## Local running

A docker compose setup has been configured to run the async request backend.  This setup runs a Python/FastAPI for 
receiving requests, a Postgres database to store requests, an SQS queue using the excellent [Localstack](https://www.localstack.cloud/) container
to trigger processing and a basic Python app to process the requests.

You can run the docker compose stack by executing the following command:

```shell
docker compose up -d
```

### Request API

To create a new request, you can post via curl:

```shell
curl --location 'http://localhost:8000/requests' \
--header 'Content-Type: application/json' \
--data-raw '{
    "user_email": "someone@email.com"
}'
```

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