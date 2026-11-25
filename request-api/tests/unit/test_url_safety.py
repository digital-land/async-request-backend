import ipaddress

import pytest

from request_model.url_safety import is_public_ip


@pytest.mark.parametrize(
    "address",
    [
        "127.0.0.1",
        "10.0.0.1",
        "169.254.169.254",
        "169.254.170.2",
        "100.64.0.1",
        "::1",
        "::",
        "fd00:ec2::254",
        "fe80::1",
        "::ffff:169.254.169.254",
        "2002:a9fe:a9fe::",
        # NAT64 addresses whose IPv4 part is internal.
        "64:ff9b::a9fe:a9fe",
        "64:ff9b::a00:1",
        "64:ff9b::7f00:1",
        # Local-use NAT64, IPv4-compatible and site-local addresses.
        "64:ff9b:1::808:808",
        "::a9fe:a9fe",
        "::808:808",
        "fec0::1",
    ],
)
def test_internal_addresses_are_not_public(address):
    assert not is_public_ip(ipaddress.ip_address(address))


@pytest.mark.parametrize(
    "address",
    [
        "8.8.8.8",
        "93.184.216.34",
        "2001:4860:4860::8888",
        "::ffff:8.8.8.8",
        "64:ff9b::808:808",
    ],
)
def test_public_addresses_are_public(address):
    assert is_public_ip(ipaddress.ip_address(address))
