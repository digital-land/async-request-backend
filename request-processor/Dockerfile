FROM python:3.12-alpine3.19

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

RUN apk add --no-cache musl-dev curl
RUN pip3 install cython
RUN apk add --no-cache libcurl curl-dev build-base git proj proj-dev proj-util geos-dev

# Install GDAL dependencies
RUN apk update && \
    apk add --no-cache gdal-dev gdal gdal-tools aws-cli postgresql-client jq && \
    rm -rf /var/cache/apk/*

COPY request-processor/requirements/requirements.txt requirements/requirements.txt
COPY request-processor/makefile makefile
COPY request-processor/makerules/makerules.mk makerules/makerules.mk
COPY request-processor/makerules/python.mk makerules/python.mk
RUN make init


COPY .git/ .
COPY request-processor/src/. .

COPY ../request_model ./request_model
COPY ../task_interface ./task_interface

COPY request-processor/docker-healthcheck.sh docker-healthcheck.sh

COPY request-processor/healthcheck-output-template.json healthcheck-output-template.json

HEALTHCHECK CMD "./docker-healthcheck.sh"

COPY request-processor/docker-entrypoint.sh docker-entrypoint.sh

ENTRYPOINT ["./docker-entrypoint.sh"]
