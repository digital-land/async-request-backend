FROM python:3.12-alpine

COPY signal_handler.py signal_handler.py
COPY main.py main.py
COPY requirements.txt requirements.txt

RUN python -m pip install -r requirements.txt

ENTRYPOINT ["python", "main.py"]