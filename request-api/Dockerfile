FROM python:3.10-slim


ARG GIT_COMMIT
ENV GIT_COMMIT=$GIT_COMMIT

RUN apt update && apt install -y --no-install-recommends \
libc-dev curl binutils
# RUN apk add --no-cache libcurl curl-dev build-base linux-headers libffi-dev

COPY request-api/requirements/requirements.txt requirements/requirements.txt

RUN python -m pip install -r requirements/requirements.txt

COPY request-api/src/. .

COPY ../request_model ./request_model

COPY ../task_interface ./task_interface

COPY request-api/alembic.ini alembic.ini

COPY request-api/docker-entrypoint.sh docker-entrypoint.sh

COPY request-api/migrations migrations

COPY request-api/makefile makefile

ENTRYPOINT ["./docker-entrypoint.sh"]
