import asyncio
import os
from datetime import datetime, timedelta, timezone

from elasticsearch import AsyncElasticsearch

from heartbeat_service.webhook_notifier import WebhookNotifier

THRESHOLD = float(os.environ.get("ERROR_SPIKE_THRESHOLD", "0.50"))
MIN_SAMPLES = int(os.environ.get("ERROR_SPIKE_MIN_SAMPLES", "10"))
WINDOW_SECONDS = int(os.environ.get("ERROR_SPIKE_WINDOW_SECONDS", "60"))
CHECK_INTERVAL = 30.0


class ErrorSpikeDetector:
    def __init__(self):
        self._es_host = os.environ.get(
            "ELASTICSEARCH_HOST", "http://elasticsearch:9200"
        )
        self._webhooks = WebhookNotifier()
        self._last_alert: dict[str, datetime] = {}  # node_id -> datetime

    async def detect_loop(self):
        while True:
            await asyncio.sleep(CHECK_INTERVAL)
            try:
                await self._check()
            except asyncio.CancelledError:
                print("[error_spike] Spike detector shutting down")
                raise
            except Exception as e:
                print(f"[error_spike] check failed: {e}")

    async def _check(self):
        es = AsyncElasticsearch(self._es_host)
        now = datetime.now(timezone.utc)
        start_time = now - timedelta(seconds=WINDOW_SECONDS)

        body = {
            "query": {"range": {"timestamp": {"gte": start_time.isoformat()}}},
            "size": 0,
            "aggs": {
                "by_node": {
                    "terms": {"field": "node_id.keyword", "size": 1000},
                    "aggs": {
                        "service_name": {
                            "terms": {"field": "service_name.keyword", "size": 1}
                        },
                        "errors": {"filter": {"term": {"log_level.keyword": "ERROR"}}},
                    },
                }
            },
        }

        try:
            resp = await es.search(index="distributed_logs-*", body=body)
        except Exception:
            # Index might not exist yet
            await es.close()
            return

        await es.close()

        buckets = resp.get("aggregations", {}).get("by_node", {}).get("buckets", [])
        for bucket in buckets:
            node_id = bucket["key"]
            total_count = bucket["doc_count"]
            error_count = bucket["errors"]["doc_count"]

            service_buckets = bucket.get("service_name", {}).get("buckets", [])
            service_name = service_buckets[0]["key"] if service_buckets else "unknown"

            if total_count >= MIN_SAMPLES:
                error_rate = error_count / total_count
                if error_rate >= THRESHOLD:
                    last = self._last_alert.get(node_id)
                    if not last or (now - last).total_seconds() > WINDOW_SECONDS:
                        self._last_alert[node_id] = now
                        await self._fire_alert(
                            node_id, service_name, error_rate, total_count
                        )

    async def _fire_alert(
        self, node_id: str, service_name: str, error_rate: float, total_count: int
    ):
        print(
            f"[error_spike] {node_id} ({service_name}) rate={error_rate:.2f} ({total_count} samples)"
        )
        await self._webhooks.notify(node_id, service_name, "error_spike")
