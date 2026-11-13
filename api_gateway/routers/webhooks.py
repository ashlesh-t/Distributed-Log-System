import secrets
from fastapi import APIRouter, Depends, HTTPException
import httpx
from elasticsearch import NotFoundError

from api_gateway.auth import verify_api_key
from api_gateway.elastic_client import get_es
from api_gateway.url_safety import UnsafeURLError, assert_public_url
from api_gateway.schemas import WebhookCreate, WebhookInfo, WebhooksResponse

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])
WEBHOOK_INDEX = "webhooks"


async def _get_owned(webhook_id: str, key_meta: dict) -> dict:
    """Return the webhook doc if owned by the caller; 404 otherwise (no existence leak)."""
    try:
        doc = await get_es().get(index=WEBHOOK_INDEX, id=webhook_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Webhook not found")
    src = doc["_source"]
    if src.get("owner_key_id") != key_meta.get("key_id", ""):
        raise HTTPException(status_code=404, detail="Webhook not found")
    return src


@router.post("", response_model=WebhookInfo, status_code=201)
async def create_webhook(body: WebhookCreate, key_meta: dict = Depends(verify_api_key)):
    try:
        await assert_public_url(body.url)
    except UnsafeURLError as exc:
        raise HTTPException(status_code=422, detail=str(exc))
    es = get_es()
    webhook_id = secrets.token_hex(16)
    doc = {
        "webhook_id": webhook_id,
        "url": body.url,
        "events": body.events,
        "secret": body.secret,
        "owner_key_id": key_meta.get("key_id", ""),
    }
    await es.index(index=WEBHOOK_INDEX, id=webhook_id, body=doc)
    return WebhookInfo(webhook_id=webhook_id, url=body.url, events=body.events)


@router.get("", response_model=WebhooksResponse)
async def list_webhooks(key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    key_id = key_meta.get("key_id", "")
    resp = await es.search(
        index=WEBHOOK_INDEX,
        body={"query": {"term": {"owner_key_id": key_id}}, "size": 100},
    )
    webhooks = [WebhookInfo(**h["_source"]) for h in resp["hits"]["hits"]]
    return WebhooksResponse(webhooks=webhooks)


@router.delete("/{webhook_id}", status_code=204)
async def delete_webhook(webhook_id: str, key_meta: dict = Depends(verify_api_key)):
    await _get_owned(webhook_id, key_meta)
    await get_es().delete(index=WEBHOOK_INDEX, id=webhook_id)


@router.post("/{webhook_id}/test")
async def test_webhook(webhook_id: str, key_meta: dict = Depends(verify_api_key)):
    url = (await _get_owned(webhook_id, key_meta))["url"]
    try:
        await assert_public_url(url)
    except UnsafeURLError as exc:
        raise HTTPException(status_code=422, detail=str(exc))

    payload = {
        "event": "test",
        "message": "This is a test webhook from the Distributed Log System PaaS",
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.post(url, json=payload, follow_redirects=False)
        return {"sent": True, "status_code": r.status_code}
    except Exception as exc:
        return {"sent": False, "error": str(exc)}
