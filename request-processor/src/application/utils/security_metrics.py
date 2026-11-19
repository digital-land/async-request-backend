"""
Counts inputs the worker refuses for safety reasons.

Every refusal is counted as the same Sentry metric so they can be graphed and
alerted on together. Attributes are kept low-cardinality: no URLs, hosts,
filenames or request ids.
"""

import sentry_sdk

SECURITY_REJECTED_METRIC = "async.security.rejected"


def count_security_rejection(layer, source):
    """
    Count one refused input.

    layer: the check that refused it: "url" (the URL text), "connection" (the
        address a host resolves to, including after a redirect) or "content"
        (a file that points GDAL at another data source).
    source: where the input came from: "fetch" (a user-supplied URL) or
        "upload" (a file uploaded through Submit).
    """
    sentry_sdk.metrics.count(
        SECURITY_REJECTED_METRIC,
        1,
        attributes={"service": "request-processor", "layer": layer, "source": source},
    )
