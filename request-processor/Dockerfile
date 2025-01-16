FROM python:3.9-slim-bookworm

ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

RUN pip install --no-cache-dir cython

RUN apt update && apt install -y --no-install-recommends \
    gcc g++ make git libc-dev curl proj-bin libproj-dev procps\
    gdal-bin gdal-data awscli postgresql-client jq && \
    rm -rf /var/lib/apt/lists/*


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
