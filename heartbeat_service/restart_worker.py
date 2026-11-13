"""Executes restart requests queued by the API gateway.

The gateway has no Docker socket / K8s credentials; it only writes documents to
the ``restart_requests`` index. This worker (which owns those credentials)
claims pending requests and runs them through RestartHandler.
"""

import asyncio
import os

from elasticsearch import AsyncElasticsearch

from heartbeat_service.restart_handler import RestartHandler

INDEX = "restart_requests"
POLL_INTERVAL = 2.0


class RestartWorker:
    def __init__(self, es=None, restarter=None):
        host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.es = es or AsyncElasticsearch(host)
        self.restarter = restarter or RestartHandler()

    async def process_once(self) -> int:
        resp = await self.es.search(
            index=INDEX,
            ignore_unavailable=True,
            body={
                "query": {"term": {"status": "pending"}},
                "sort": [{"requested_at": {"order": "asc", "unmapped_type": "date"}}],
                "size": 20,
            },
        )
        hits = resp["hits"]["hits"]
        for hit in hits:
            req = hit["_source"]
            # Mark first so a crash mid-restart can't re-run the request forever.
            await self.es.update(
                index=INDEX, id=hit["_id"], body={"doc": {"status": "running"}}
            )
            ok = await self.restarter.restart(req["node_id"], req["service_name"])
            await self.es.update(
                index=INDEX,
                id=hit["_id"],
                body={"doc": {"status": "done" if ok else "failed"}},
                refresh="wait_for",
            )
        return len(hits)

    async def run(self) -> None:
        while True:
            try:
                await self.process_once()
            except Exception as exc:
                print(f"[restart_worker] error: {exc}")
            await asyncio.sleep(POLL_INTERVAL)
