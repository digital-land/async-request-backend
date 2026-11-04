# Add Data Submission workflow

HTTP endpoints live in `request-api/src/routers/add_data_submissions.py`, registered
by `main.py`. They use shared dependencies from `src/dependencies.py`. Workflow
and persistence logic remains in `src/add_data_submissions.py`, API schemas in
`src/add_data_submission_schemas.py`, and shared database models in `request_model/`.

Submission workflow is separate from request processing. A request's `COMPLETE`
means processing finished. A submission's `ADDED` means adding the data was
dispatched; it does not certify that the downstream workflow succeeded. `ADDED`
and `CLOSED` are terminal. This API does not dispatch additions or send email.

All endpoints below require a Manage JWT with the `staff` or `admin` role.
Transitions additionally require a nonblank signed `username` claim (the GitHub
login); the API derives the actor from it and rejects a body-supplied actor.
Authentication bypass without a verified identity cannot perform transitions.

## Transitions

Read `/add-data-submissions/{id}` to obtain its current `version`, then send:

```http
POST /add-data-submissions/{id}/transitions
Content-Type: application/json

{
  "to": "IN_PROGRESS",
  "assignee": "github-login",
  "expected_version": 1,
  "note": "Picking this up"
}
```

The response is the updated submission, including its incremented `version`
and events. A stale version or disallowed transition returns `409`; reload and
review the current state before retrying. A row lock protects the version check,
state change and event insertion in one transaction. This also prevents a stale
claim from becoming a reassignment, and detects simultaneous reassignments.

| From | Allowed destinations |
| --- | --- |
| `TO_BE_ADDED` | `IN_PROGRESS`, `QUERIED`, `CLOSED` |
| `IN_PROGRESS` | `TO_BE_ADDED`, `IN_PROGRESS`, `QUERIED`, `ADDED`, `CLOSED` |
| `QUERIED` | `TO_BE_ADDED`, `IN_PROGRESS`, `CLOSED` |
| `ADDED`, `CLOSED` | None |

- `IN_PROGRESS` requires `assignee`, a GitHub login. This also handles reassignment.
- `TO_BE_ADDED` clears the assignee. Other destinations preserve it. Supply the
  `assignee` field only for `IN_PROGRESS`.
- `QUERIED` requires a nonblank `note` and an existing contact email. Querying a
  waiting submission leaves it unassigned. Manage-origin submissions cannot be queried.
- `CLOSED` requires `closed_reason`: `duplicate`, `withdrawn`, `no_response`,
  `not_suitable` or `resubmitted`. Only `duplicate` and `resubmitted` may include
  `related_add_data_submission_id`; it must identify a different existing submission.
- Invalid input returns `422`; missing submissions return `404`.

`GET /add-data-submissions/{id}/events` returns events in ID order. Transition
events include from/to status, actor, resulting assignee, note, timestamp and
closure details. Creation events retain their existing shape with nullable
actor/from-status and closure fields. The API provides no event update or delete
operation. Workflow changes do not update processing requests or their `modified`
timestamps.

## List

`GET /add-data-submissions` supports exact filters `status`, `assignee`,
`organisation`, `dataset` and `source`, combined with AND. `reference` performs a
case-insensitive literal substring search (`%` and `_` are not wildcards).

`sort=submitted_at` is the default, oldest first; `sort=-submitted_at` reverses
date ordering. ID breaks timestamp ties. `limit` defaults to 50 (1–500), and
`offset` defaults to 0. The existing pagination envelope is used:

```json
{
  "params": {"offset": 0, "limit": 50},
  "total_results_available": 0,
  "data": []
}
```

Each entry uses the submission read schema, including contact details and events.

## Edit details

`PATCH /add-data-submissions/{id}` requires `expected_version` from the last read,
plus `documentation_url` (an HTTP/HTTPS URL) and/or `add_data_details` (a JSON object
or null). Omitted fields are unchanged;
the JSON object is replaced rather than merged, and null clears it. Empty patches
and unknown fields return `422`. The version is checked under the same row lock
used by transitions, and incremented atomically with a successful edit. Stale saves
return `409`; reload and review the latest details before retrying. Detail edits
and transitions share this counter, so either makes an older version stale.
This endpoint does not change workflow status,
assignee, status timestamp or events. It may edit details on
terminal submissions without reopening them.

## Migration and verification

Apply Alembic revision `f3b151a01c01` after #150's `f3b150a01c01`. It defaults
existing submissions to version 1 and adds nullable closure details to events.

From `request-api`, using its Python 3.10 test environment and Docker:

```sh
python -m pytest tests/integration/test_add_data_submissions.py
python -m pytest tests/unit/test_submission_route_guards.py
```

Run API unit, integration and acceptance suites as separate processes, since their
shared cached database engine can otherwise retain a stopped container address.
