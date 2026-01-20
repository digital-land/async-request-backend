#!/usr/bin/env python
"""
Manual debug trigger for check_dataurl task.

This script allows you to directly invoke the check_dataurl task without going through
the Celery broker, making it easier to debug with breakpoints in VS Code.
    
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

# Import request-processor DB helpers now that paths/cwd are set
import crud
import database
import request_model.models as request_models

# Change to request-processor so relative paths (e.g. specification/, var/) resolve correctly
os.chdir(request_processor_root)

def ensure_request_exists(request_payload: dict) -> None:
    """Upsert a row into the request table so the processor can write responses.

    When calling tasks directly (not via Celery), the normal request-api flow that
    creates the Request row does not run.
    """

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise RuntimeError(
            "DATABASE_URL is not set. Start the stack (or export DATABASE_URL) before running debug_trigger."
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


# Now import the task (request-processor/src is on sys.path)
from tasks import check_dataurl

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

# Request payload - POST body from your requirement (all required fields for schemas.Request)
now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
request_payload = {
    "id": "debug-request-001",
    "type": "check_url",
    "status": "PENDING",
    "created": now,
    "modified": now,
    "response": None,
    "params": {
        "type": "check_url",
        "organisationName": "London Borough of Hackney",
        "dataset": "article-4-direction",
        "collection": "article-4-direction",
        "column_mapping": None,
        "geom_type": None,
        "url": "https://map2.hackney.gov.uk/geoserver/ows?service=WFS&version=2.0&request=GetFeature&outputFormat=application/json&SrsName=EPSG:4326&typeName=planning:lldc_hackney_conservation_area"
    }
}

print("=" * 80)
print("DEBUG TRIGGER: Invoking check_dataurl task directly")
print("=" * 80)
print(f"\nRequest ID: {request_payload['id']}")
print(f"Dataset: {request_payload['params']['dataset']}")
print(f"Collection: {request_payload['params']['collection']}")
print(f"URL: {request_payload['params']['url'][:80]}...")
print("\n" + "=" * 80)
print("Set breakpoint at line 99 in pipeline.py and resume execution.")
print("=" * 80 + "\n")

try:
    ensure_request_exists(request_payload)
    # Call the task synchronously without Celery
    result = check_dataurl(request_payload, directories=json.dumps(directories_override))
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
