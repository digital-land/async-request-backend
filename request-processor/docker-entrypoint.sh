#!/bin/sh
set -e
echo "${DATABASE_URL}"
celery --config celeryconfig -A tasks worker -l  INFO  -P eventlet --concurrency ${CELERY_WORKER_CONCURRENCY:-1} &

# Run the healthcheck script in the background
./docker-healthcheck.sh &

# Wait for Celery worker to finish
wait
