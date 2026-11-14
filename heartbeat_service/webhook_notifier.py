import hashlib
import hmac
import json
import os
from datetime import datetime, timezone

import httpx

from common import secret_box
from common.constants import DEFAULT_ES_HOST, ENV_ES_HOST, WEBHOOK_INDEX
from common.url_safety import UnsafeURLError, assert_public_url


class WebhookNotifier:
    def __init__(self):
        self._global_url = os.environ.get("WEBHOOK_URL", "")

    async def notify(
        self, node_id: str, service_name: str, event: str, extra: dict | None = None
    ) -> None:
        payload = {
            "event": event,
            "node_id": node_id,
            "service_name": service_name,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            **(extra or {}),
        }
        # Fire global webhook if configured
        if self._global_url:
            await self._post(
                self._global_url, payload, secret=os.environ.get("WEBHOOK_SECRET", "")
            )

        # Also notify per-tenant webhooks stored in ES (loaded by api_gateway)
        await self._notify_es_webhooks(node_id, event, payload)

    async def _notify_es_webhooks(
        self, node_id: str, event: str, payload: dict
    ) -> None:
        try:
            from elasticsearch import AsyncElasticsearch

            host = os.environ.get(ENV_ES_HOST, DEFAULT_ES_HOST)
            es = AsyncElasticsearch(host)
            resp = await es.search(
                index=WEBHOOK_INDEX,
                body={
                    "query": {"bool": {"filter": [{"term": {"events": event}}]}},
                    "size": 100,
                },
            )
            await es.close()
            for hit in resp["hits"]["hits"]:
                wh = hit["_source"]
                # Tenant scoping: a webhook only sees events for nodes its owner
                # key may access (empty list = unrestricted key).
                allowed = wh.get("owner_node_ids") or []
                if allowed and node_id not in allowed:
                    continue
                try:
                    await assert_public_url(wh["url"])
                except UnsafeURLError as exc:
                    print(f"[webhook] skipping unsafe target {wh['url']}: {exc}")
                    continue
                await self._post(
                    wh["url"], payload, secret=secret_box.decrypt(wh.get("secret", ""))
                )
        except Exception as exc:
            print(f"[webhook] ES webhook lookup error: {exc}")

    async def _post(self, url: str, payload: dict, secret: str = "") -> None:
        headers = {"Content-Type": "application/json"}
        if secret:
            sig = hmac.new(
                secret.encode(),
                json.dumps(payload, sort_keys=True).encode(),
                hashlib.sha256,
            ).hexdigest()
            headers["X-Signature-256"] = f"sha256={sig}"
        for attempt in range(2):
            try:
                async with httpx.AsyncClient(timeout=5.0) as client:
                    r = await client.post(url, json=payload, headers=headers)
                    print(f"[webhook] {url} → {r.status_code}")
                    return
            except Exception as exc:
                if attempt == 0:
                    print(f"[webhook] retry after error: {exc}")
                else:
                    print(f"[webhook] failed after retry: {exc}")
