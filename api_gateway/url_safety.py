"""SSRF guard for user-supplied webhook URLs."""

import asyncio
import ipaddress
import socket
from urllib.parse import urlsplit


class UnsafeURLError(ValueError):
    pass


def _check_ip(addr: str) -> None:
    ip = ipaddress.ip_address(addr)
    if (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    ):
        raise UnsafeURLError("URL resolves to a non-public address")


async def assert_public_url(url: str) -> None:
    """Raise UnsafeURLError unless url is http(s) and resolves only to public IPs."""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise UnsafeURLError("Only http(s) URLs with a host are allowed")
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(parts.hostname, parts.port or 443)
    except socket.gaierror as exc:
        raise UnsafeURLError("Host does not resolve") from exc
    for info in infos:
        _check_ip(info[4][0])
