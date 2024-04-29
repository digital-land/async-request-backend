# Checks health by checking worker process is running along with connectivity checks of SQS and Postgres
set -e
pgrep celery
aws sqs get-queue-url --queue-name celery
pg_isready -d "$DATABASE_URL"