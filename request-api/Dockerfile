FROM python:3.12-alpine

COPY request-api/requirements.txt requirements.txt

COPY request-api/src/. .

COPY ../request_model ./request_model

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0"]