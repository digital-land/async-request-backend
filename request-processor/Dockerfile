FROM python:3.12.7-slim-bookworm

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

RUN pip3 install cython

RUN apt update && \
    apt install gcc -y && \
    apt install g++ -y && \
    apt install make && \
    apt install git -y && \
    apt install libc-dev curl proj-bin -y libproj-dev -y \
    gdal-bin \
    gdal-data \
    awscli \
    postgresql-client \
    jq


RUN apt clean

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
