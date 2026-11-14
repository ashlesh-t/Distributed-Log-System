"""Emits the ``error_spike`` webhook event when a node logs many errors.

Consistency design (triaged for concurrent/multi-replica operation):

* **No phantom/shifting reads.** Elasticsearch is near-real-time: a log
  written "now" may not be searchable for ~1s (refresh interval), and a
  ``now-60s`` range is re-evaluated on every call. We therefore query a *closed*
  window ``[end - WINDOW, end)`` with concrete timestamps, where
  ``end = now - LAG`` and LAG exceeds the refresh interval. Logs inside that
  window are already searchable and no new ones can fall into it, so repeating
  the query gives the same count.
* **No gaps from timer drift.** Windows are evaluated every WINDOW/2 and overlap;
  a spike straddling a boundary is still caught. Overlap re-detects the same
  spike, which is what the cooldown below de-duplicates.
* **One alert across replicas.** The cooldown is a shared document per node in
  ES, claimed with optimistic concurrency (``if_seq_no``/``op_type=create``).
  Whichever replica wins the claim sends the webhook; the rest get a conflict.
* **Trade-off (at-most-once):** the claim happens before delivery, so if the
  webhook POST fails the alert for that cooldown period is not re-sent.
  The detector itself is a single asyncio task, so there is no in-process
  thread contention on its state.
"""

import asyncio
import os
import time
from datetime import datetime, timedelta, timezone

from elasticsearch import AsyncElasticsearch, ConflictError, NotFoundError

from common.constants import (
    DEFAULT_ES_HOST,
    ENV_ES_HOST,
    ERROR_SPIKE_STATE_INDEX,
    LOG_INDEX_PATTERN,
)
from heartbeat_service.webhook_notifier import WebhookNotifier

WINDOW_SECONDS = int(os.environ.get("ERROR_SPIKE_WINDOW_SECONDS", "60"))
THRESHOLD = int(os.environ.get("ERROR_SPIKE_THRESHOLD", "20"))
COOLDOWN_SECONDS = int(os.environ.get("ERROR_SPIKE_COOLDOWN_SECONDS", "300"))
# Must exceed the ES refresh interval (default 1s) so the window is stable.
LAG_SECONDS = int(os.environ.get("ERROR_SPIKE_LAG_SECONDS", "5"))
CHECK_INTERVAL = max(WINDOW_SECONDS // 2, 1)


class ErrorSpikeDetector:
    def __init__(self, es=None, notifier=None, clock=time.time):
        host = os.environ.get(ENV_ES_HOST, DEFAULT_ES_HOST)
        self.es = es or AsyncElasticsearch(host)
        self.notifier = notifier or WebhookNotifier()
        self._clock = clock

    async def _claim_alert(self, node_id: str) -> bool:
        """Atomically take the right to alert for node_id; False if on cooldown
        or another replica won the race."""
        now = self._clock()
        try:
            doc = await self.es.get(index=ERROR_SPIKE_STATE_INDEX, id=node_id)
        except NotFoundError:
            doc = None
        try:
            if doc is None:
                await self.es.index(
                    index=ERROR_SPIKE_STATE_INDEX,
                    id=node_id,
                    body={"last_alert_at": now},
                    op_type="create",
                )
            else:
                if now - doc["_source"]["last_alert_at"] < COOLDOWN_SECONDS:
                    return False
                await self.es.index(
                    index=ERROR_SPIKE_STATE_INDEX,
                    id=node_id,
                    body={"last_alert_at": now},
                    if_seq_no=doc["_seq_no"],
                    if_primary_term=doc["_primary_term"],
                )
        except ConflictError:
            return False
        return True

    async def check_once(self) -> list[str]:
        end = datetime.fromtimestamp(self._clock(), timezone.utc) - timedelta(
            seconds=LAG_SECONDS
        )
        start = end - timedelta(seconds=WINDOW_SECONDS)
        resp = await self.es.search(
            index=LOG_INDEX_PATTERN,
            ignore_unavailable=True,
            body={
                "size": 0,
                "query": {
                    "bool": {
                        "filter": [
                            {"term": {"log_level": "ERROR"}},
                            {
                                "range": {
                                    "timestamp": {
                                        "gte": start.isoformat(),
                                        "lt": end.isoformat(),
                                    }
                                }
                            },
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
        for bucket in (
            resp.get("aggregations", {}).get("by_node", {}).get("buckets", [])
        ):
            node_id = bucket["key"]
            if not await self._claim_alert(node_id):
                continue
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
            await asyncio.sleep(CHECK_INTERVAL)
            try:
                await self.check_once()
            except Exception as exc:
                print(f"[error_spike] error: {exc}")
