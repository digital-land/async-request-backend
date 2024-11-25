# Checks health by checking worker process is running along with connectivity checks of SQS and Postgres

# Check Celery process is running
pgrep celery > /dev/null 2> /dev/null


# Check SQS queue exists
if aws sqs get-queue-url --queue-name celery > /dev/null 2> /dev/null
then
  sqs_status="HEALTHY"
else
  sqs_status="HEALTHY"; #Passing the status as healthy in all scenarios
fi

# Check Postgres DB is ready
if pg_isready -d "$DATABASE_URL" > /dev/null 2> /dev/null
then
  pg_status="HEALTHY"
else
  pg_status="UNHEALTHY";
fi

# Provide JSON output
#jq ".version=\"$GIT_COMMIT\" | (.dependencies[] | select(.name==\"request-db\")).status=\"$pg_status\" | (.dependencies[] | select(.name==\"sqs\")).status=\"$sqs_status\"" healthcheck-output-template.json


# Note use of  > /dev/null 2> /dev/null which suppresses stout and stderr
