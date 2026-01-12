# Debug Setup Guide for Request Processor

### Prerequisites

You need these services running locally (outside Docker):
- PostgreSQL (or use Docker version with port 5432 exposed)
- Redis (for Celery broker) - or use Docker version with port 6379 exposed
- LocalStack (for S3/SQS) - or use Docker version with port 4566 exposed

### Keep Docker Services Running (Recommended)

Keep your Docker Compose stack running:
```bash
docker compose up -d localstack request-db redis
```

Then you can use VS Code's debugger with the local Python environment.

### Start Debugging

Click **"Run"** in VS Code Debug (the play icon) or press `F5`.

The script will:
1. Invoke the `check_dataurl` task directly (no Celery broker needed)
2. Pass your POST request body payload
3. Pause at breakpoints you've set
4. Display results when complete

## What Services Are Needed for What

| Service | Purpose | Docker Port | Local Port |
|---------|---------|-------------|------------|
| PostgreSQL | Store request/response data | 54320 | 5432 |
| Redis | Celery message broker | 6379 | 6379 |
| LocalStack | Mock AWS S3/SQS | 4566 | 4566 |

---

## Using the Debug Script Standalone

You can also run the debug script without VS Code's debugger:

```bash
cd /Users/matthewpoole1/DigitalLand/async-request-backend
python debug_trigger.py
```

This will:
1. Invoke the task synchronously
2. Print the result or any errors
3. NOT pause at breakpoints (unless you attach a debugger)

---

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

