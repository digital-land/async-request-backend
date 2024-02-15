FROM python:3.12-alpine

COPY requirements.txt requirements.txt

COPY src/. .

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["uvicorn", "main:app", "--host", "0.0.0.0"]