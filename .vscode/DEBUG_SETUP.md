# Debug Setup Guide for Request Processor Checking of URL

### Prerequisites

You need these services running:
- PostgreSQL (or use Docker version with port 54320 exposed)
- Redis (for Celery broker) - or use Docker version with port 6379 exposed
- LocalStack (for S3/SQS) - or use Docker version with port 4566 exposed
- **Do Not** have the request-processor running.

Keep your Docker Compose stack running with:
```bash
docker compose up -d localstack request-db redis
```

- Make sure to have .venv installed locally for request-processor, such that the launch.json is correctly pointing to it

### Start Debugging of CheckURL

Click **"Run"** in VS Code Debug

The script will:
1. Invoke the `check_dataurl` task directly (no Celery broker needed)
2. Pass your POST request body payload <-- make sure to set the request_body variable to data you want to test in debug_trigger.py
3. Pause at breakpoints you've set


## Environment Variables

The launch configuration sets these automatically:

```
DATABASE_URL=postgresql://postgres:password@localhost:54320/request_database
CELERY_BROKER_URL=redis://localhost:6379/0
AWS_ENDPOINT_URL=http://localhost:4566
AWS_DEFAULT_REGION=eu-west-2
SENTRY_ENABLED=false
```

If your local setup differs, edit `.vscode/launch.json` and update the `env` section within debugging request processor.

---

## Notes

- The `debug_trigger.py` script calls `check_dataurl()` **synchronously**, without Celery. This is intentional—it makes debugging simpler.
- The request database transaction will be created when the task runs, so you can inspect the DB afterward.
- The `docker_volume` folder should now contain the downloaded resources under `/opt/collection/resource/<request-id>/` after the task completes.

