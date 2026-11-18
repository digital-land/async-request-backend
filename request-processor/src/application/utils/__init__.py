"""
Safety checks for the request-processor worker.

* task_guard: rejects task messages that request-api could not have sent.
* safe_fetch: only lets the worker connect to public addresses.
* resource_safety: rejects fetched or uploaded files that would make GDAL read
  other locations.
* request_cleanup: deletes a request's files without leaving its directory.
"""
