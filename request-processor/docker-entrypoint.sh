#!/bin/sh
set -e
echo "${DATABASE_URL}"
celery --config celeryconfig -A tasks worker -l  INFO  -P prefork --concurrency "${CELERY_WORKER_CONCURRENCY:-1}"
