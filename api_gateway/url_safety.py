"""SSRF guard for user-supplied webhook URLs."""

import asyncio
import ipaddress
import os
import socket
from urllib.parse import urlsplit


class UnsafeURLError(ValueError):
    pass


def _env_flag(name: str) -> bool:
    return os.environ.get(name, "").strip().lower() in ("1", "true", "yes")


def _check_ip(addr: str, allow_private: bool) -> None:
    ip = ipaddress.ip_address(addr)
    # Link-local covers cloud metadata (169.254.169.254); never allowed.
    if ip.is_link_local or ip.is_multicast or ip.is_reserved or ip.is_unspecified:
        raise UnsafeURLError("URL resolves to a disallowed address")
    if (ip.is_private or ip.is_loopback) and not allow_private:
        raise UnsafeURLError(
            "URL resolves to a private address "
            "(operator can allow with WEBHOOK_ALLOW_PRIVATE_TARGETS=true)"
        )


async def assert_public_url(url: str) -> None:
    """Validate a webhook target; raise UnsafeURLError if it is not allowed.

    Policy (operator-controlled via env, default is strict):
    - Public hosts must use https (WEBHOOK_ALLOW_HTTP=true permits plain http).
    - Private/loopback hosts are rejected unless WEBHOOK_ALLOW_PRIVATE_TARGETS=true
      (for self-hosted setups notifying a local server); plain http is then fine
      since traffic stays on the operator's own network.
    - Link-local/metadata addresses are always rejected.
    """
    allow_private = _env_flag("WEBHOOK_ALLOW_PRIVATE_TARGETS")
    allow_http = _env_flag("WEBHOOK_ALLOW_HTTP")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname:
        raise UnsafeURLError("Only http(s) URLs with a host are allowed")
    loop = asyncio.get_running_loop()
    try:
        infos = await loop.getaddrinfo(parts.hostname, parts.port or 443)
    except socket.gaierror as exc:
        raise UnsafeURLError("Host does not resolve") from exc
    addrs = [info[4][0] for info in infos]
    for addr in addrs:
        _check_ip(addr, allow_private)
    all_private = all(
        ipaddress.ip_address(a).is_private or ipaddress.ip_address(a).is_loopback
        for a in addrs
    )
    if parts.scheme == "http" and not (allow_http or all_private):
        raise UnsafeURLError("Public webhook targets must use https")
