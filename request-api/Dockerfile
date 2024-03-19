FROM python:3.12-alpine

RUN apk add --no-cache libcurl curl-dev build-base

COPY request-api/requirements.txt requirements.txt

COPY request-api/src/. .

COPY ../request_model ./request_model

COPY ../task_interface ./task_interface

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0"]