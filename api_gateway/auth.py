import hashlib
import hmac
import logging
import os
import secrets

from elasticsearch import NotFoundError
from fastapi import Header, HTTPException, status

from api_gateway.elastic_client import get_es

SALT = os.environ.get("API_KEY_SALT", "change-me-in-production")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "change-me-in-production")
KEY_INDEX = "api_keys"

if "change-me-in-production" in (SALT, ADMIN_SECRET):
    logging.getLogger(__name__).warning(
        "API_KEY_SALT/ADMIN_SECRET use the insecure default; set real values."
    )


def _hash_key(plaintext: str) -> str:
    return hashlib.sha256(f"{SALT}{plaintext}".encode()).hexdigest()


async def verify_api_key(x_api_key: str = Header(...)) -> dict:
    """FastAPI dependency: validates X-API-Key header, returns key metadata."""
    key_hash = _hash_key(x_api_key)
    es = get_es()
    try:
        resp = await es.search(
            index=KEY_INDEX,
            body={"query": {"term": {"key_hash": key_hash}}, "size": 1},
        )
        hits = resp["hits"]["hits"]
        if not hits:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
            )
        return hits[0]["_source"]
    except HTTPException:
        raise
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Authentication backend unavailable",
        )


def verify_admin(x_admin_secret: str = Header(...)) -> None:
    if not hmac.compare_digest(x_admin_secret.encode(), ADMIN_SECRET.encode()):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin secret"
        )


async def create_api_key(
    owner_email: str, node_ids: list[str] | None = None, can_restart: bool = False
) -> tuple[str, str]:
    """Generate a new API key. Returns (plaintext_key, key_id)."""
    plaintext = f"dls_{secrets.token_urlsafe(24)}"
    key_id = secrets.token_hex(16)
    key_hash = _hash_key(plaintext)
    es = get_es()
    await es.index(
        index=KEY_INDEX,
        id=key_id,
        body={
            "key_id": key_id,
            "key_hash": key_hash,
            "owner_email": owner_email,
            "node_ids": node_ids or [],
            "can_restart": can_restart,
        },
        refresh="wait_for",
    )
    return plaintext, key_id


async def delete_api_key(key_id: str) -> bool:
    es = get_es()
    try:
        await es.delete(index=KEY_INDEX, id=key_id)
        return True
    except NotFoundError:
        return False


def node_allowed(key_meta: dict, node_id: str) -> bool:
    """A key with no node_ids sees every node; otherwise only the listed ones."""
    node_ids = key_meta.get("node_ids") or []
    return not node_ids or node_id in node_ids
