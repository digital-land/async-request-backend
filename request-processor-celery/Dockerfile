FROM python:3.12-alpine

RUN apk add --no-cache libcurl curl-dev build-base

COPY request-processor-celery/src/. .
COPY request-processor-celery/requirements.txt requirements.txt

COPY ../request_model ./request_model
COPY ../request_tasks ./request_tasks

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["celery", "-A", "request_tasks.task", "worker", "-l", "INFO", "-P", "eventlet", "--concurrency", "1"]