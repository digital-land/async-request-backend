# Checks health by checking worker process is running along with connectivity checks of SQS and Postgres

# Check Celery process is running
pgrep celery > /dev/null 2> /dev/null
exit_code=$?

# Check SQS queue exists
if aws sqs get-queue-url --queue-name celery > /dev/null 2> /dev/null
then
  sqs_status="HEALTHY"
else
  sqs_status="UNHEALTHY";
  exit_code=1;
fi

# Check Postgres DB is ready
if pg_isready -d "$DATABASE_URL" > /dev/null 2> /dev/null
then
  pg_status="HEALTHY"
else
  pg_status="UNHEALTHY";
  exit_code=1;
fi

# Provide JSON output
jq ".version=\"$GIT_COMMIT\" | (.dependencies[] | select(.name==\"request-db\")).status=\"$pg_status\" | (.dependencies[] | select(.name==\"sqs\")).status=\"$sqs_status\"" healthcheck-output-template.json

exit $exit_code

# Note use of  > /dev/null 2> /dev/null which suppresses stout and stderr
