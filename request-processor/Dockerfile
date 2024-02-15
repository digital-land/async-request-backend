FROM python:3.12-alpine

COPY request-processor/src/. .
COPY request-processor/requirements.txt requirements.txt

COPY ../request_model ./request_model

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]