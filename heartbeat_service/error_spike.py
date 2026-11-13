"""Emits the ``error_spike`` webhook event when a node logs many errors."""

import asyncio
import os
import time

from elasticsearch import AsyncElasticsearch

from heartbeat_service.webhook_notifier import WebhookNotifier

WINDOW_SECONDS = int(os.environ.get("ERROR_SPIKE_WINDOW_SECONDS", "60"))
THRESHOLD = int(os.environ.get("ERROR_SPIKE_THRESHOLD", "20"))
COOLDOWN_SECONDS = int(os.environ.get("ERROR_SPIKE_COOLDOWN_SECONDS", "300"))


class ErrorSpikeDetector:
    def __init__(self, es=None, notifier=None, clock=time.monotonic):
        host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.es = es or AsyncElasticsearch(host)
        self.notifier = notifier or WebhookNotifier()
        self._clock = clock
        self._last_alert: dict[str, float] = {}

    async def check_once(self) -> list[str]:
        resp = await self.es.search(
            index="distributed_logs-*",
            ignore_unavailable=True,
            body={
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"log_level": "ERROR"}},
                            {"range": {"timestamp": {"gte": f"now-{WINDOW_SECONDS}s"}}},
                        ]
                    }
                },
                "aggs": {
                    "by_node": {
                        "terms": {"field": "node_id", "min_doc_count": THRESHOLD},
                        "aggs": {
                            "svc": {"terms": {"field": "service_name", "size": 1}}
                        },
                    }
                },
            },
        )
        alerted = []
        now = self._clock()
        for bucket in (
            resp.get("aggregations", {}).get("by_node", {}).get("buckets", [])
        ):
            node_id = bucket["key"]
            if (
                now - self._last_alert.get(node_id, -COOLDOWN_SECONDS)
                < COOLDOWN_SECONDS
            ):
                continue
            self._last_alert[node_id] = now
            svc = bucket["svc"]["buckets"]
            service_name = svc[0]["key"] if svc else "unknown"
            await self.notifier.notify(
                node_id,
                service_name,
                "error_spike",
                extra={
                    "error_count": bucket["doc_count"],
                    "window_seconds": WINDOW_SECONDS,
                },
            )
            alerted.append(node_id)
        return alerted

    async def run(self) -> None:
        while True:
            await asyncio.sleep(WINDOW_SECONDS)
            try:
                await self.check_once()
            except Exception as exc:
                print(f"[error_spike] error: {exc}")
