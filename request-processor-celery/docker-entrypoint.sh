#!/bin/sh
set -e
echo "${DATABASE_URL}"
celery --config celeryconfig -A tasks worker -l INFO -P eventlet --concurrency 1

