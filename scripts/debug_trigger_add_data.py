#!/usr/bin/env python
"""
Manual debug trigger for add_data_task.

Invokes the add_data task directly without Celery so breakpoints hit reliably in VS Code.
"""

import os
import sys
import json
import datetime
from pathlib import Path

# Set up paths
workspace_root = Path(__file__).parent.parent
request_processor_src = workspace_root / "request-processor" / "src"
request_processor_root = workspace_root / "request-processor"
request_model = workspace_root / "request_model"
task_interface = workspace_root / "task_interface"

sys.path.insert(0, str(request_processor_src))
sys.path.insert(0, str(request_model))
sys.path.insert(0, str(task_interface))

import crud  # noqa: E402
import database  # noqa: E402
import request_model.models as request_models  # noqa: E402
from tasks import add_data_task  # noqa: E402

# Change to request-processor so relative paths (e.g. specification/, var/) resolve correctly
os.chdir(request_processor_root)


def ensure_request_exists(request_payload: dict) -> None:
    """Upsert a row into the request table so the processor can write responses."""

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Start the stack (or export DATABASE_URL) before running debug_trigger_add_data."
        )

    request_id = request_payload["id"]
    db_session = database.session_maker()
    with db_session() as session:
        existing = crud.get_request(session, request_id)
        if existing:
            existing.status = request_payload.get("status", existing.status)
            existing.type = request_payload.get("type", existing.type)
            existing.params = request_payload.get("params", existing.params)
        else:
            session.add(
                request_models.Request(
                    id=request_id,
                    status=request_payload.get("status", "PENDING"),
                    type=request_payload.get("type"),
                    params=request_payload.get("params"),
                )
            )
        session.commit()


# Override directories to local workspace paths (avoids relying on /opt/* when not in Docker)
workspace_volume = workspace_root / "request-processor" / "docker_volume"
directories_override = {
    "COLLECTION_DIR": str(workspace_volume / "collection"),
    "ISSUE_DIR": str(workspace_volume / "issue"),
    "COLUMN_FIELD_DIR": str(workspace_volume / "column-field"),
    "TRANSFORMED_DIR": str(workspace_volume / "transformed"),
    "CONVERTED_DIR": str(workspace_volume / "converted"),
    "PIPELINE_DIR": str(workspace_volume / "pipeline"),
    # Leave relative defaults for var/ and specification/
}

# Ensure base directories exist
for d in directories_override.values():
    Path(d).mkdir(parents=True, exist_ok=True)

# Request payload - edit the params block if you need to test different datasets or URLs
now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
request_payload = {
    "id": "debug-add-data-001",
    "type": "add_data",
    "status": "PENDING",
    "created": now,
    "modified": now,
    "response": None,
    "params": {
        "type": "add_data",
        "organisationName": "Stockport Metropolitan Borough Council",
        "organisation": "local-authority:SKP",
        "dataset": "article-4-direction",
        "collection": "article-4-direction",
        "column_mapping": None,
        "geom_type": None,
        "url": "https://smbc-opendata.s3.eu-west-1.amazonaws.com/Article4/Article4_Dataset_Stockport.csv",
        "documentation_url": "https://example.com/article-4-direction/documentation",
        "licence": "ogl3",
        "start_date": "2020-01-01",
        "plugin": None,
    },
}

print("=" * 80)
print("DEBUG TRIGGER: Invoking add_data_task directly")
print("=" * 80)
print(f"\nRequest ID: {request_payload['id']}")
print(f"Dataset: {request_payload['params']['dataset']}")
print(f"Collection: {request_payload['params']['collection']}")
print(f"URL: {request_payload['params']['url'][:80]}...")
print("\n" + "=" * 80)
print("Set a breakpoint inside add_data_workflow if you want to step through it.")
print("=" * 80 + "\n")

try:
    ensure_request_exists(request_payload)
    result = add_data_task(request_payload, directories=json.dumps(directories_override))
    print("\n" + "=" * 80)
    print("TASK COMPLETED SUCCESSFULLY")
    print("=" * 80)
    print(f"\nResult: {json.dumps(result, indent=2, default=str)}")
except Exception as e:
    print("\n" + "=" * 80)
    print("TASK FAILED WITH EXCEPTION")
    print("=" * 80)
    print(f"\nException Type: {type(e).__name__}")
    print(f"Exception Message: {str(e)}")
    import traceback

    traceback.print_exc()
    sys.exit(1)
