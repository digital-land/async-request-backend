"""
Stop user-supplied URL fetches from reaching internal addresses.

request_model.url_safety.check_fetch_url rejects bad URLs up front, but a public
hostname can still resolve to an internal address, a public URL can redirect to
an internal one, and DNS answers can change between a check and the connection.

untrusted_fetches() closes those gaps for everything that goes through urllib3
(requests, and so digital-land's Collector and its arcgis plugin). While it is
active on the current thread, every new connection resolves the host, refuses
non-public addresses, and connects to the address it checked. Connections made
outside the context manager, such as boto3 to S3 and SQS, are unaffected.

While it is active, every response read through requests is also limited to
MAX_RESPONSE_BYTES, so a URL cannot exhaust the worker's memory with a huge or
endless body. This matches Submit's limit for URLs.

The paged WFS plugin fetches with ogr2ogr (GDAL) in a subprocess, which this
cannot see, so check_host_resolves_publicly is also called before fetching.
"""

import contextlib
import ipaddress
import socket
import threading
from urllib.parse import urlsplit

import requests
import urllib3.util.connection as urllib3_connection
from requests.adapters import HTTPAdapter

from application.logging.monitoring import count_security_rejection
from request_model.url_safety import UnsafeUrlError, is_public_ip

# Matches Submit's validations.maxFileSize for URLs.
MAX_RESPONSE_BYTES = 100_000_000
_READ_CHUNK_SIZE = 64 * 1024

_state = threading.local()
_original_create_connection = urllib3_connection.create_connection
_original_adapter_send = HTTPAdapter.send


class BlockedAddressError(OSError):
    """A fetch tried to connect to a host that is not allowed."""


class NonPublicAddressError(BlockedAddressError):
    """A host resolved to an internal address."""


class ResponseTooLargeError(requests.exceptions.RequestException):
    """A fetched response was larger than MAX_RESPONSE_BYTES."""


def _public_addresses(host, port):
    """Resolve host and return its addresses, refusing any non-public one."""
    addresses = socket.getaddrinfo(host, port, 0, socket.SOCK_STREAM)
    if not addresses:
        raise BlockedAddressError(f"{host} did not resolve")
    for *_, sockaddr in addresses:
        ip = ipaddress.ip_address(sockaddr[0].split("%", 1)[0])
        if not is_public_ip(ip):
            # Counted here so both the connection guard and the paged WFS
            # up-front check report it, once.
            count_security_rejection("connection", "fetch")
            raise NonPublicAddressError(f"{host} resolves to a non-public address")
    return [sockaddr[0] for *_, sockaddr in addresses]


def _guarded_create_connection(address, *args, **kwargs):
    if not getattr(_state, "active", False):
        return _original_create_connection(address, *args, **kwargs)

    host, port = address
    host = host.strip("[]")
    last_error = None
    # Connect to the address that was checked, so a second DNS lookup can't
    # return something different. TLS still verifies against the hostname,
    # which urllib3 passes separately when wrapping the socket.
    for ip in dict.fromkeys(_public_addresses(host, port)):
        try:
            return _original_create_connection((ip, port), *args, **kwargs)
        except OSError as e:
            last_error = e
    raise last_error


def _too_large(response):
    response.close()
    return ResponseTooLargeError(
        f"Response is larger than {MAX_RESPONSE_BYTES // 1_000_000}MB",
        response=response,
    )


def _size_limited_send(
    self, request, stream=False, timeout=None, verify=True, cert=None, proxies=None
):
    options = dict(timeout=timeout, verify=verify, cert=cert, proxies=proxies)
    if not getattr(_state, "active", False):
        return _original_adapter_send(self, request, stream=stream, **options)

    # Read the body here, stopping at the limit, rather than letting requests
    # read all of it. The limit applies after Content-Encoding is decoded, so a
    # small gzip body cannot expand past it either. Callers that asked to
    # stream get the body already read, which requests supports.
    response = _original_adapter_send(self, request, stream=True, **options)

    declared = response.headers.get("Content-Length", "")
    if declared.isdigit() and int(declared) > MAX_RESPONSE_BYTES:
        raise _too_large(response)

    body = bytearray()
    try:
        for chunk in response.iter_content(_READ_CHUNK_SIZE):
            body += chunk
            if len(body) > MAX_RESPONSE_BYTES:
                raise _too_large(response)
    except BaseException:
        response.close()
        raise
    response._content = bytes(body)
    return response


def _install():
    if urllib3_connection.create_connection is not _guarded_create_connection:
        urllib3_connection.create_connection = _guarded_create_connection
    if HTTPAdapter.send is not _size_limited_send:
        HTTPAdapter.send = _size_limited_send


_install()


@contextlib.contextmanager
def untrusted_fetches():
    """
    Refuse connections to non-public addresses, and responses larger than
    MAX_RESPONSE_BYTES, on this thread while active.
    """
    previous = getattr(_state, "active", False)
    _state.active = True
    try:
        yield
    finally:
        _state.active = previous


def check_host_resolves_publicly(url):
    """Raise UnsafeUrlError unless every address the URL's host resolves to is public."""
    parts = urlsplit(url)
    try:
        _public_addresses(parts.hostname, parts.port or 443)
    except (BlockedAddressError, OSError) as e:
        raise UnsafeUrlError(f"URL host is not allowed: {e}") from e
