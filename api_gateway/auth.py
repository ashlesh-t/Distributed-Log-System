import hashlib
import os
import secrets

from fastapi import Header, HTTPException, status

from api_gateway.elastic_client import get_es

SALT = os.environ.get("API_KEY_SALT", "change-me-in-production")
ADMIN_SECRET = os.environ.get("ADMIN_SECRET", "change-me-in-production")
KEY_INDEX = "api_keys"


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
            status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid API key"
        )


def verify_admin(x_admin_secret: str = Header(...)) -> None:
    if x_admin_secret != ADMIN_SECRET:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Invalid admin secret"
        )


async def create_api_key(owner_email: str) -> tuple[str, str]:
    """Generate a new API key. Returns (plaintext_key, key_id)."""
    plaintext = f"dls_{secrets.token_urlsafe(24)}"
    key_id = secrets.token_hex(16)
    key_hash = _hash_key(plaintext)
    es = get_es()
    await es.index(
        index=KEY_INDEX,
        id=key_id,
        body={"key_id": key_id, "key_hash": key_hash, "owner_email": owner_email},
    )
    return plaintext, key_id


async def delete_api_key(key_id: str) -> bool:
    es = get_es()
    try:
        await es.delete(index=KEY_INDEX, id=key_id)
        return True
    except Exception:
        return False
