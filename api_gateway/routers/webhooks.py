import secrets
from fastapi import APIRouter, Depends, HTTPException
import httpx

from api_gateway.auth import verify_api_key
from api_gateway.elastic_client import get_es
from api_gateway.schemas import WebhookCreate, WebhookInfo, WebhooksResponse

router = APIRouter(prefix="/v1/webhooks", tags=["webhooks"])
WEBHOOK_INDEX = "webhooks"


@router.post("", response_model=WebhookInfo, status_code=201)
async def create_webhook(body: WebhookCreate, key_meta: dict = Depends(verify_api_key)):
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
    es = get_es()
    try:
        await es.delete(index=WEBHOOK_INDEX, id=webhook_id)
    except Exception:
        raise HTTPException(status_code=404, detail="Webhook not found")


@router.post("/{webhook_id}/test")
async def test_webhook(webhook_id: str, key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    try:
        doc = await es.get(index=WEBHOOK_INDEX, id=webhook_id)
        url = doc["_source"]["url"]
    except Exception:
        raise HTTPException(status_code=404, detail="Webhook not found")

    payload = {
        "event": "test",
        "message": "This is a test webhook from the Distributed Log System PaaS",
    }
    try:
        async with httpx.AsyncClient(timeout=5.0) as client:
            r = await client.post(url, json=payload)
        return {"sent": True, "status_code": r.status_code}
    except Exception as exc:
        return {"sent": False, "error": str(exc)}
