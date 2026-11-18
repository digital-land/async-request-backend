import http.server
import socket
import threading

import pytest
import requests
import urllib3.util.connection as urllib3_connection

from application.utils import safe_fetch
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
    # (application.utils and src.application.utils), each installing its guard.
    installed = urllib3_connection.create_connection
    assert installed.__qualname__ == safe_fetch._guarded_create_connection.__qualname__


def test_local_server_reachable_outside_untrusted_fetches(local_server):
    assert requests.get(local_server, timeout=5).content == b"secret"


def test_local_server_blocked_inside_untrusted_fetches(local_server):
    with safe_fetch.untrusted_fetches():
        with pytest.raises(requests.exceptions.ConnectionError):
            requests.get(local_server, timeout=5)


@pytest.mark.parametrize(
    "ip", ["169.254.170.2", "169.254.169.254", "10.0.0.5", "127.0.0.1", "::1"]
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


def test_check_host_resolves_publicly_accepts_public(monkeypatch):
    monkeypatch.setattr(socket, "getaddrinfo", _fake_getaddrinfo("93.184.216.34"))
    safe_fetch.check_host_resolves_publicly("https://wfs.example.org/wfs")
