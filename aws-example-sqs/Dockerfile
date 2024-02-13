FROM python:3.12-alpine

WORKDIR /app

COPY requirements.txt requirements.txt
COPY queue_wrapper.py queue_wrapper.py
COPY message_wrapper.py message_wrapper.py

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["python", "message_wrapper.py"]