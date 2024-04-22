FROM python:3.12-alpine

RUN apk add --no-cache musl-dev curl
RUN pip3 install cython
RUN apk add --no-cache libcurl curl-dev build-base git proj proj-dev proj-util geos-dev

# Install GDAL dependencies
RUN apk update && \
    apk add --no-cache gdal-dev gdal gdal-tools && \
    rm -rf /var/cache/apk/*

COPY request-processor/src/. .
COPY request-processor/requirements/requirements.txt requirements/requirements.txt
COPY request-processor/makefile makefile
COPY request-processor/makerules/makerules.mk makerules/makerules.mk
COPY request-processor/makerules/python.mk makerules/python.mk
COPY request-processor/docker-entrypoint.sh docker-entrypoint.sh

COPY ../request_model ./request_model
COPY ../task_interface ./task_interface

RUN make init

ENTRYPOINT ["./docker-entrypoint.sh"]
