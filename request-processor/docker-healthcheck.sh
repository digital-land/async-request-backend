#!/bin/sh

# Health-check output is captured by Docker/ECS. Also send failures to the main
# container's stderr so they appear in the worker's CloudWatch log stream.
log_failure() {
  message="$(date -u '+%Y-%m-%dT%H:%M:%SZ') Worker health check failed: $*"
  printf '%s\n' "$message" >&2
  if [ -w /proc/1/fd/2 ]; then
    printf '%s\n' "$message" > /proc/1/fd/2
  fi
}

exit_code=0
queue_name="${CELERY_TASK_DEFAULT_QUEUE:-celery}"

if ! pgrep celery > /dev/null 2>&1; then
  log_failure "Celery process not found"
  exit_code=1
fi

# Keep individual probes bounded and avoid SDK retries consuming the entire
# health-check window. Never log DATABASE_URL, which contains credentials.
if AWS_MAX_ATTEMPTS=1 aws sqs get-queue-url --queue-name "$queue_name" \
    --cli-connect-timeout 1 --cli-read-timeout 1 > /dev/null 2>&1; then
  sqs_status="HEALTHY"
else
  log_failure "SQS queue lookup failed or timed out (queue=$queue_name)"
  sqs_status="UNHEALTHY"
  exit_code=1
fi

if pg_isready -d "$DATABASE_URL" -t 1 > /dev/null 2>&1; then
  pg_status="HEALTHY"
else
  log_failure "PostgreSQL readiness check failed or timed out"
  pg_status="UNHEALTHY"
  exit_code=1
fi

if ! jq --arg version "$GIT_COMMIT" --arg pg_status "$pg_status" \
    --arg sqs_status "$sqs_status" \
    '.version=$version | (.dependencies[] | select(.name=="request-db")).status=$pg_status | (.dependencies[] | select(.name=="sqs")).status=$sqs_status' \
    healthcheck-output-template.json; then
  log_failure "Could not generate health-check JSON"
  exit_code=1
fi

exit "$exit_code"
