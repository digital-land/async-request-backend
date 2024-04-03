FROM python:3.12-alpine

RUN apk add --no-cache musl-dev curl
RUN pip3 install cython
RUN apk add --no-cache libcurl curl-dev build-base git proj proj-dev proj-util geos-dev

COPY request-processor-celery/src/. .
COPY request-processor-celery/requirements.txt requirements.txt
COPY request-processor-celery/makefile makefile
COPY request-processor-celery/docker-entrypoint.sh docker-entrypoint.sh

COPY ../request_model ./request_model
COPY ../task_interface ./task_interface

RUN make init

ENTRYPOINT ["./docker-entrypoint.sh"]