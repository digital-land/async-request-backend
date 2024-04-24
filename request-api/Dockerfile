FROM python:3.12-alpine

RUN apk add --no-cache libcurl curl-dev build-base

COPY request-api/requirements/requirements.txt requirements/requirements.txt

COPY request-api/src/. .

RUN python -m pip install -r requirements/requirements.txt

COPY ../request_model ./request_model

COPY ../task_interface ./task_interface

COPY request-api/alembic.ini alembic.ini

COPY request-api/docker-entrypoint.sh docker-entrypoint.sh

COPY request-api/migrations migrations

COPY request-api/makefile makefile

ENTRYPOINT ["./docker-entrypoint.sh"]
