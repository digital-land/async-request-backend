FROM python:3.12-alpine

RUN apk add --no-cache musl-dev curl
RUN pip3 install cython
RUN apk add --no-cache libcurl curl-dev build-base git proj proj-dev proj-util geos-dev

COPY request-processor-celery/src/. .
COPY request-processor-celery/requirements.txt requirements.txt
COPY request-processor-celery/makefile makefile

COPY ../request_model ./request_model
COPY ../task_interface ./task_interface

RUN make init

ENTRYPOINT ["celery", "--config", "celeryconfig", "-A", "tasks", "worker", "-l", "INFO", "-P", "eventlet", "--concurrency", "1"]
