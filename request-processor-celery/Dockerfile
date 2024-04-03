FROM python:3.12-alpine

RUN apk add --no-cache musl-dev curl
RUN pip3 install cython
RUN apk add --no-cache libcurl curl-dev build-base git proj proj-dev proj-util geos-dev

# Install GDAL dependencies
RUN apk update && \
    apk add --no-cache gdal-dev gdal gdal-tools && \
    rm -rf /var/cache/apk/*

COPY request-processor-celery/src/. .
COPY request-processor-celery/requirements.txt requirements.txt
COPY request-processor-celery/makefile makefile

COPY ../request_model ./request_model
COPY ../task_interface ./task_interface

RUN make init

ENTRYPOINT ["celery", "-A", "tasks", "worker", "-l", "INFO", "-P", "eventlet", "--concurrency", "1"]
