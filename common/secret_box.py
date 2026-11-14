"""Symmetric encryption for secrets stored at rest (e.g. webhook signing secrets).

The key comes from WEBHOOK_ENCRYPTION_KEY and must be identical in every service
that reads or writes these secrets (api_gateway, heartbeat_service).
"""

import base64
import hashlib
import logging
import os

from cryptography.fernet import Fernet, InvalidToken

from common.constants import (
    ENCRYPTED_PREFIX as PREFIX,
    ENV_WEBHOOK_ENCRYPTION_KEY,
    INSECURE_DEFAULT_SECRET,
)

if os.environ.get(ENV_WEBHOOK_ENCRYPTION_KEY, INSECURE_DEFAULT_SECRET) == (
    INSECURE_DEFAULT_SECRET
):
    logging.getLogger(__name__).warning(
        "WEBHOOK_ENCRYPTION_KEY is unset/default; set a real value in production."
    )


def _fernet() -> Fernet:
    raw = os.environ.get(ENV_WEBHOOK_ENCRYPTION_KEY, INSECURE_DEFAULT_SECRET).encode()
    return Fernet(base64.urlsafe_b64encode(hashlib.sha256(raw).digest()))


def encrypt(plaintext: str) -> str:
    if not plaintext:
        return ""
    return PREFIX + _fernet().encrypt(plaintext.encode()).decode()


def decrypt(stored: str) -> str:
    """Return plaintext. Values without the prefix are legacy plaintext."""
    if not stored or not stored.startswith(PREFIX):
        return stored or ""
    try:
        return _fernet().decrypt(stored[len(PREFIX) :].encode()).decode()
    except InvalidToken:
        return ""  # wrong key: refuse to sign rather than leak/crash
