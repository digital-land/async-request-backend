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
endless body. This matches Submit's limit for URLs. Each response must also
arrive within MAX_RESPONSE_SECONDS in total.

The paged WFS plugin fetches with ogr2ogr (GDAL) in a subprocess, which this
cannot see, so check_host_resolves_publicly is also called before fetching.
"""

import contextlib
import ipaddress
import socket
import threading
import weakref
from urllib.parse import urlsplit

import requests
import urllib3.connection
import urllib3.util.connection as urllib3_connection
from requests.adapters import HTTPAdapter

from application.logging.monitoring import count_security_rejection
from request_model.url_safety import UnsafeUrlError, is_public_ip

# Matches Submit's validations.maxFileSize for URLs.
MAX_RESPONSE_BYTES = 100_000_000
MAX_RESPONSE_SECONDS = 15 * 60
_READ_CHUNK_SIZE = 64 * 1024

_state = threading.local()
_original_create_connection = urllib3_connection.create_connection
_original_adapter_send = HTTPAdapter.send
_CONNECTION_CLASSES = (
    urllib3.connection.HTTPConnection,
    urllib3.connection.HTTPSConnection,
)
# Unwrap in case another import of this module already installed its guard.
_original_connects = {
    cls: getattr(
        cls.__dict__["connect"], "_safe_fetch_original", cls.__dict__["connect"]
    )
    for cls in _CONNECTION_CLASSES
}


class BlockedAddressError(OSError):
    """A fetch tried to connect to a host that is not allowed."""


class NonPublicAddressError(BlockedAddressError):
    """A host resolved to an internal address."""


class ResponseTooLargeError(requests.exceptions.RequestException):
    """A fetched response was larger than MAX_RESPONSE_BYTES."""


class ResponseTooSlowError(requests.exceptions.Timeout):
    """A fetched response took longer than MAX_RESPONSE_SECONDS in total."""


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


def _guarded_connect(original):
    def connect(self):
        original(self)
        sockets = getattr(_state, "sockets", None)
        if sockets is not None and self.sock is not None:
            sockets.add(self.sock)

    connect.__qualname__ = f"_guarded_connect.{original.__qualname__}"
    connect._safe_fetch_original = original
    return connect


_guarded_connects = {
    cls: _guarded_connect(original) for cls, original in _original_connects.items()
}


def _shut_down(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass


class _SocketRegistry:
    """
    The sockets opened on one thread inside untrusted_fetches().

    The fetch thread adds sockets while a _Deadline timer thread may be shutting
    them down, so both go through one lock. Once the deadline has expired, any
    socket opened afterwards is shut down straight away, so it cannot start a
    new blocking read.
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._sockets = weakref.WeakSet()
        self._expired = False

    def add(self, sock):
        with self._lock:
            self._sockets.add(sock)
            expired = self._expired
        if expired:
            _shut_down(sock)

    def expire(self):
        with self._lock:
            self._expired = True
            sockets = list(self._sockets)
        for sock in sockets:
            _shut_down(sock)

    def reset(self):
        with self._lock:
            self._expired = False


class _Deadline:
    """Shuts down this thread's sockets from a timer, ending any blocked read."""

    def __init__(self, seconds, sockets):
        self._lock = threading.Lock()
        self._sockets = sockets
        self._expired = False
        self._finished = False
        self._timer = threading.Timer(seconds, self._expire)
        self._timer.daemon = True
        self._timer.start()

    def _expire(self):
        # Hold this lock while expiring the registry, so finish() cannot
        # reset it before expire() runs and leave it marked as expired.
        with self._lock:
            if self._finished:
                return
            self._expired = True
            self._sockets.expire()

    def finish(self):
        """Stop the timer. Returns True if it already fired."""
        self._timer.cancel()
        with self._lock:
            self._finished = True
            if self._expired:
                # Later requests in the same untrusted_fetches() get new
                # sockets that must not be shut down.
                self._sockets.reset()
            return self._expired


def _too_slow(response=None):
    if response is not None:
        response.close()
    return ResponseTooSlowError(
        f"Response took longer than {MAX_RESPONSE_SECONDS} seconds",
        response=response,
    )


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
    deadline = _Deadline(MAX_RESPONSE_SECONDS, _state.sockets)
    response = None
    try:
        response = _original_adapter_send(self, request, stream=True, **options)

        declared = response.headers.get("Content-Length", "")
        if declared.isdigit() and int(declared) > MAX_RESPONSE_BYTES:
            raise _too_large(response)

        body = bytearray()
        for chunk in response.iter_content(_READ_CHUNK_SIZE):
            body += chunk
            if len(body) > MAX_RESPONSE_BYTES:
                raise _too_large(response)
    except BaseException as error:
        if response is not None:
            response.close()
        if deadline.finish() and not isinstance(error, ResponseTooLargeError):
            raise _too_slow() from error
        raise
    # A body with no length can look complete after being cut off.
    if deadline.finish():
        raise _too_slow(response)
    response._content = bytes(body)
    return response


def _install():
    if urllib3_connection.create_connection is not _guarded_create_connection:
        urllib3_connection.create_connection = _guarded_create_connection
    if HTTPAdapter.send is not _size_limited_send:
        HTTPAdapter.send = _size_limited_send
    for cls, guarded in _guarded_connects.items():
        if cls.__dict__["connect"] is not guarded:
            cls.connect = guarded


_install()


@contextlib.contextmanager
def untrusted_fetches():
    """
    Refuse connections to non-public addresses, and responses larger than
    MAX_RESPONSE_BYTES or slower than MAX_RESPONSE_SECONDS, on this thread
    while active.
    """
    previous = getattr(_state, "active", False), getattr(_state, "sockets", None)
    _state.active = True
    _state.sockets = _SocketRegistry()
    try:
        yield
    finally:
        _state.active, _state.sockets = previous


def check_host_resolves_publicly(url):
    """Raise UnsafeUrlError unless every address the URL's host resolves to is public."""
    parts = urlsplit(url)
    try:
        _public_addresses(parts.hostname, parts.port or 443)
    except (BlockedAddressError, OSError) as e:
        raise UnsafeUrlError(f"URL host is not allowed: {e}") from e
