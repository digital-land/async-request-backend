"""
Checks for URLs that the request-processor fetches on a user's behalf.

check_url and add_data requests carry a URL that the worker downloads. Without
checks, that lets a caller make the worker read local files (file://) or reach
internal services such as the ECS credentials endpoint (169.254.170.2) or the
EC2 metadata service (169.254.169.254).

check_fetch_url is a static check: it needs no network access, so request-api
can use it to reject bad URLs before a request is stored. It does not cover a
hostname that resolves to an internal address, or a redirect to one.
"""

import ipaddress
import re
import socket
from urllib.parse import urlsplit

ALLOWED_SCHEMES = {"http", "https"}
MAX_URL_LENGTH = 2048
_BLOCKED_HOSTNAMES = {"localhost"}
# Characters that URL parsers disagree on, which can make the host we check
# differ from the host that is actually contacted.
_DISALLOWED_CHARACTERS = set("\\") | {chr(c) for c in range(0x20)} | {"\x7f"}
# The same control characters percent-encoded (e.g. %00, %0a). No real data URL
# needs them, and they can change how a path is read once decoded.
_ENCODED_CONTROL_CHARACTER = re.compile(r"%(?:[01][0-9a-f]|7f)", re.IGNORECASE)
# GDAL virtual file system paths such as /vsicurl/ or /vsis3/. These let GDAL
# read from other locations, so they have no place in a URL we fetch.
_GDAL_VIRTUAL_PATH = re.compile(r"/vsi[a-z0-9_]*/", re.IGNORECASE)


class UnsafeUrlError(ValueError):
    """The URL must not be fetched by the worker."""


def parse_ip(host):
    """
    Return the address if host is an IP literal, else None.

    Also accepts the legacy IPv4 forms that resolvers understand, such as
    "2852039166", "0xa9fea9fe" or "169.254.43518", which all mean
    169.254.169.254.
    """
    try:
        return ipaddress.ip_address(host)
    except ValueError:
        pass
    try:
        return ipaddress.IPv4Address(socket.inet_aton(host))
    except OSError:
        return None


def is_public_ip(ip):
    """True if ip is a publicly routable address."""
    if ip.version == 6 and ip.ipv4_mapped is not None:
        ip = ip.ipv4_mapped
    return ip.is_global


def check_fetch_url(url):
    """
    Raise UnsafeUrlError unless url is an http(s) URL whose host is not an
    internal address. Returns the url unchanged.
    """
    if not isinstance(url, str) or not url.strip():
        raise UnsafeUrlError("A URL is required")
    if len(url) > MAX_URL_LENGTH:
        raise UnsafeUrlError(f"URL must be at most {MAX_URL_LENGTH} characters")
    if any(character in _DISALLOWED_CHARACTERS for character in url):
        raise UnsafeUrlError("URL contains characters that are not allowed")
    if _ENCODED_CONTROL_CHARACTER.search(url):
        raise UnsafeUrlError("URL contains encoded characters that are not allowed")
    if _GDAL_VIRTUAL_PATH.search(url):
        raise UnsafeUrlError("URL contains a path that is not allowed")

    try:
        parts = urlsplit(url)
        host = parts.hostname
        parts.port  # raises ValueError for an invalid port
    except ValueError as e:
        raise UnsafeUrlError("URL could not be parsed") from e

    if parts.scheme.lower() not in ALLOWED_SCHEMES:
        raise UnsafeUrlError("Only http and https URLs are allowed")
    if not host:
        raise UnsafeUrlError("URL must include a host")
    # user:password@host is a common way to disguise the real host.
    if parts.username is not None or parts.password is not None:
        raise UnsafeUrlError("URL must not include a username or password")

    host = host.rstrip(".").lower()
    if host in _BLOCKED_HOSTNAMES or host.endswith(".localhost"):
        raise UnsafeUrlError("URL must not point at this machine")

    ip = parse_ip(host)
    if ip is not None and not is_public_ip(ip):
        raise UnsafeUrlError("URL must not point at an internal address")

    return url
