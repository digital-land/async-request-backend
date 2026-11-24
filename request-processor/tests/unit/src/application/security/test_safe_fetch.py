import http.server
import socket
import threading

import pytest
import requests
import urllib3.util.connection as urllib3_connection

from application.security import safe_fetch
from request_model.url_safety import UnsafeUrlError


def _fake_getaddrinfo(ip):
    def getaddrinfo(host, port, *args, **kwargs):
        family = socket.AF_INET6 if ":" in ip else socket.AF_INET
        return [(family, socket.SOCK_STREAM, 6, "", (ip, port))]

    return getaddrinfo


@pytest.fixture
def local_server():
    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"secret")

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/"
    server.shutdown()


def test_guard_is_installed():
    # Compare by name: tests can import this module under two package paths
    # (application.security and src.application.security), each installing its guard.
    installed = urllib3_connection.create_connection
    assert installed.__qualname__ == safe_fetch._guarded_create_connection.__qualname__


def test_local_server_reachable_outside_untrusted_fetches(local_server):
    assert requests.get(local_server, timeout=5).content == b"secret"


def test_local_server_blocked_inside_untrusted_fetches(local_server):
    with safe_fetch.untrusted_fetches():
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.get(local_server, timeout=5)


@pytest.mark.parametrize(
    "ip",
    [
        "169.254.170.2",
        "169.254.169.254",
        "10.0.0.5",
        "127.0.0.1",
        "::1",
        # A AAAA record pointing at NAT64 for an internal IPv4 address.
        "64:ff9b::a00:5",
    ],
)
def test_hostname_resolving_to_internal_address_is_blocked(monkeypatch, ip):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo(ip))
    with safe_fetch.untrusted_fetches():
        with pytest.raises(safe_fetch.BlockedAddressError):
            urllib3_connection.create_connection(("public-looking.example", 80))


def test_dns_name_for_an_internal_address_is_blocked(monkeypatch):
    # Passes the URL text check, but resolves to the metadata service.
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("169.254.169.254"))
    connected = []
    monkeypatch.setattr(
        safe_fetch,
        "_original_create_connection",
        lambda address, *a, **kw: connected.append(address) or "sock",
    )
    with safe_fetch.untrusted_fetches():
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.get("http://169.254.169.254.nip.io/latest/meta-data/", timeout=5)
    assert connected == []


@pytest.fixture
def redirecting_server():
    """A server that redirects every request to an internal address."""
    hits = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            hits.append(self.path)
            self.send_response(302)
            self.send_header("Location", "http://169.254.170.2/v2/credentials/x")
            self.end_headers()

        def log_message(self, *args):
            pass

    server = http.server.HTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/data.csv", hits
    server.shutdown()


def test_redirect_to_internal_address_is_blocked(monkeypatch, redirecting_server):
    url, hits = redirecting_server
    # Treat the local test server as the public site; everything else keeps
    # the normal rule, so the redirect target is still internal.
    real_is_public_ip = safe_fetch.is_public_ip
    monkeypatch.setattr(
        safe_fetch,
        "is_public_ip",
        lambda ip: str(ip) == "127.0.0.1" or real_is_public_ip(ip),
    )
    connected = []
    real_connect = safe_fetch._original_create_connection

    def record_connect(address, *args, **kwargs):
        connected.append(address[0])
        return real_connect(address, *args, **kwargs)

    monkeypatch.setattr(safe_fetch, "_original_create_connection", record_connect)

    with safe_fetch.untrusted_fetches():
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.get(url, timeout=5)

    assert hits == ["/data.csv"]  # the first hop was fetched
    assert "169.254.170.2" not in connected  # the redirect never connected


def test_connects_to_the_checked_address(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    connected = []
    monkeypatch.setattr(
        safe_fetch,
        "_original_create_connection",
        lambda address, *a, **kw: connected.append(address) or "sock",
    )
    with safe_fetch.untrusted_fetches():
        assert urllib3_connection.create_connection(("example.org", 443)) == "sock"
    assert connected == [("93.184.216.34", 443)]


def test_untrusted_fetches_is_restored_after_exit():
    with safe_fetch.untrusted_fetches():
        pass
    assert not getattr(safe_fetch._state, "active", False)


def test_check_host_resolves_publicly_rejects_internal(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("169.254.170.2"))
    with pytest.raises(UnsafeUrlError):
        safe_fetch.check_host_resolves_publicly("https://wfs.example.org/wfs")


@pytest.fixture
def rejections(monkeypatch):
    """Security rejections counted, as (layer, source) pairs."""
    counted = []
    monkeypatch.setattr(
        safe_fetch,
        "count_security_rejection",
        lambda layer, source: counted.append((layer, source)),
    )
    return counted


def test_each_internal_address_rejection_is_counted_once(monkeypatch, rejections):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("169.254.170.2"))
    with safe_fetch.untrusted_fetches():
        with pytest.raises(safe_fetch.NonPublicAddressError):
            urllib3_connection.create_connection(("public-looking.example", 80))
    with pytest.raises(UnsafeUrlError):
        safe_fetch.check_host_resolves_publicly("https://wfs.example.org/wfs")
    assert rejections == [("connection", "fetch"), ("connection", "fetch")]


def test_host_that_does_not_resolve_is_not_counted(monkeypatch, rejections):
    monkeypatch.setattr(socket, "getaddrinfo", lambda *args, **kwargs: [])
    with safe_fetch.untrusted_fetches():
        with pytest.raises(safe_fetch.BlockedAddressError):
            urllib3_connection.create_connection(("missing.example", 80))
    assert rejections == []


def test_public_address_is_not_counted(monkeypatch, rejections):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    safe_fetch.check_host_resolves_publicly("https://wfs.example.org/wfs")
    assert rejections == []


def test_check_host_resolves_publicly_accepts_public(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    safe_fetch.check_host_resolves_publicly("https://wfs.example.org/wfs")


@pytest.fixture
def body_server():
    """A local server whose response body is set per test."""
    responses = {}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def do_GET(self):
            body, headers = responses["body"], responses.get("headers", {})
            self.send_response(200)
            for name, value in headers.items():
                self.send_header(name, value)
            if "Content-Length" not in headers:
                # Chunked, so the size is not known up front.
                self.send_header("Transfer-Encoding", "chunked")
                self.end_headers()
                for start in range(0, len(body), 4096):
                    end = start + 4096
                    part = body[start:end]
                    self.wfile.write(b"%x\r\n%s\r\n" % (len(part), part))
                self.wfile.write(b"0\r\n\r\n")
            else:
                self.end_headers()
                self.wfile.write(body)

        def log_message(self, *args):
            pass

        def handle_one_request(self):
            try:
                super().handle_one_request()
            except ConnectionError:
                pass  # the client stopped reading at the limit

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/data.csv", responses
    server.shutdown()


@pytest.fixture
def local_server_is_public(monkeypatch):
    real_is_public_ip = safe_fetch.is_public_ip
    monkeypatch.setattr(
        safe_fetch,
        "is_public_ip",
        lambda ip: str(ip) == "127.0.0.1" or real_is_public_ip(ip),
    )


@pytest.fixture
def small_limit(monkeypatch):
    monkeypatch.setattr(safe_fetch, "MAX_RESPONSE_BYTES", 10_000)


@pytest.mark.parametrize("declare_length", [True, False])
def test_response_over_the_limit_is_refused(
    body_server, local_server_is_public, small_limit, declare_length
):
    url, responses = body_server
    responses["body"] = b"x" * 20_000
    if declare_length:
        responses["headers"] = {"Content-Length": "20000"}
    with safe_fetch.untrusted_fetches():
        with pytest.raises(safe_fetch.ResponseTooLargeError):
            requests.get(url, timeout=5)


def test_response_with_false_small_length_is_still_limited(
    body_server, local_server_is_public, small_limit
):
    url, responses = body_server
    responses["body"] = b"x" * 20_000
    # The header is ignored for reading: urllib3 reads to the declared length,
    # so a lying header gives a short body, never a longer one.
    responses["headers"] = {"Content-Length": "5"}
    with safe_fetch.untrusted_fetches():
        assert len(requests.get(url, timeout=5).content) == 5


def test_compressed_response_is_limited_after_decoding(
    body_server, local_server_is_public, small_limit
):
    import gzip

    url, responses = body_server
    responses["body"] = gzip.compress(b"x" * 1_000_000)
    responses["headers"] = {"Content-Encoding": "gzip"}
    assert len(responses["body"]) < 10_000
    with safe_fetch.untrusted_fetches():
        with pytest.raises(safe_fetch.ResponseTooLargeError):
            requests.get(url, timeout=5)


@pytest.mark.parametrize("stream", [False, True])
def test_response_under_the_limit_is_returned(
    body_server, local_server_is_public, small_limit, stream
):
    url, responses = body_server
    responses["body"] = b"reference\n" + b"1" * 5_000
    with safe_fetch.untrusted_fetches():
        response = requests.get(url, timeout=5, stream=stream)
        assert response.content == responses["body"]
        assert b"".join(response.iter_content(100)) == responses["body"]


def test_response_size_is_not_limited_outside_untrusted_fetches(
    body_server, small_limit
):
    url, responses = body_server
    responses["body"] = b"x" * 20_000
    assert len(requests.get(url, timeout=5).content) == 20_000


def test_too_large_error_is_a_requests_exception():
    # Collector catches requests.RequestException and logs its name, which
    # tasks._fetch_with_plugins turns into a "too large" error for the user.
    assert issubclass(safe_fetch.ResponseTooLargeError, requests.RequestException)


def test_too_slow_error_is_a_requests_exception():
    assert issubclass(safe_fetch.ResponseTooSlowError, requests.RequestException)


@pytest.fixture
def slow_server():
    """A local server that sends the part named in settings["slow"] a byte at a time."""
    import time

    settings = {}

    def drip(wfile, data):
        for byte in data:
            wfile.write(bytes([byte]))
            wfile.flush()
            time.sleep(0.1)

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            slow = settings["slow"]
            body = b"x" * 50
            if slow == "headers":
                self.wfile.write(b"HTTP/1.1 200 OK\r\n")
                drip(self.wfile, b"X-Padding: " + b"a" * 50 + b"\r\n")
                self.wfile.write(b"Content-Length: 50\r\n\r\n" + body)
                return
            self.send_response(200)
            if slow == "body":
                self.send_header("Content-Length", str(len(body)))
            self.send_header("Connection", "close")
            self.end_headers()
            drip(self.wfile, body)

        def log_message(self, *args):
            pass

        def handle_one_request(self):
            try:
                super().handle_one_request()
            except (ConnectionError, OSError):
                pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}/data.csv", settings
    server.shutdown()


@pytest.fixture
def short_deadline(monkeypatch):
    monkeypatch.setattr(safe_fetch, "MAX_RESPONSE_SECONDS", 0.5)


@pytest.mark.parametrize("slow", ["headers", "body", "body-no-length"])
def test_response_over_the_time_limit_is_refused(
    slow_server, local_server_is_public, short_deadline, slow
):
    import time

    url, settings = slow_server
    settings["slow"] = slow
    started = time.monotonic()
    with safe_fetch.untrusted_fetches():
        with pytest.raises(safe_fetch.ResponseTooSlowError):
            requests.get(url, timeout=5)
    assert time.monotonic() - started < 2


def test_response_within_the_time_limit_is_returned_and_not_cut_later(
    body_server, local_server_is_public, short_deadline
):
    import time

    url, responses = body_server
    responses["body"] = b"reference\n1\n"
    with safe_fetch.untrusted_fetches():
        with requests.Session() as session:
            assert session.get(url, timeout=5).content == responses["body"]
            time.sleep(1)
            assert session.get(url, timeout=5).content == responses["body"]


def test_response_time_is_not_limited_outside_untrusted_fetches(
    slow_server, short_deadline
):
    url, settings = slow_server
    settings["slow"] = "body"
    assert requests.get(url, timeout=5).content == b"x" * 50


class _FakeSocket:
    def __init__(self):
        self.shut_down = False

    def shutdown(self, how):
        self.shut_down = True


def test_socket_opened_after_the_deadline_expired_is_shut_down():
    registry = safe_fetch._SocketRegistry()
    deadline = safe_fetch._Deadline(0, registry)
    deadline._timer.join()
    late = _FakeSocket()
    registry.add(late)
    assert late.shut_down
    assert deadline.finish()


def test_registry_is_usable_again_after_an_expired_deadline_finishes():
    registry = safe_fetch._SocketRegistry()
    deadline = safe_fetch._Deadline(0, registry)
    deadline._timer.join()
    deadline.finish()
    sock = _FakeSocket()
    registry.add(sock)
    assert not sock.shut_down


def test_deadline_shuts_down_sockets_added_while_it_expires():
    registry = safe_fetch._SocketRegistry()
    sockets = [_FakeSocket() for _ in range(20_000)]
    deadline = safe_fetch._Deadline(0.01, registry)
    for sock in sockets:
        registry.add(sock)
    deadline._timer.join()
    assert deadline.finish()
    assert all(sock.shut_down for sock in sockets)
