"""Executes restart requests queued by the API gateway.

The gateway has no Docker socket / K8s credentials; it only writes documents to
the ``restart_requests`` index. This worker (which owns those credentials)
claims pending requests and runs them through RestartHandler.

Concurrency: several workers (replicas, or a restart overlapping a slow poll)
may see the same pending request. A request is *claimed* with an optimistic
concurrency update (``if_seq_no``/``if_primary_term``): only one worker wins,
the others get a version conflict and skip it, so a node is never restarted twice
for one request.
"""

import asyncio
import os

from elasticsearch import AsyncElasticsearch, ConflictError

from common.constants import (
    DEFAULT_ES_HOST,
    ENV_ES_HOST,
    RESTART_REQUEST_INDEX as INDEX,
    RestartStatus,
)
from heartbeat_service.restart_handler import RestartHandler

POLL_INTERVAL = 2.0


class RestartWorker:
    def __init__(self, es=None, restarter=None):
        host = os.environ.get(ENV_ES_HOST, DEFAULT_ES_HOST)
        self.es = es or AsyncElasticsearch(host)
        self.restarter = restarter or RestartHandler()

    async def _claim(self, hit: dict) -> bool:
        try:
            await self.es.update(
                index=INDEX,
                id=hit["_id"],
                body={"doc": {"status": RestartStatus.RUNNING}},
                if_seq_no=hit["_seq_no"],
                if_primary_term=hit["_primary_term"],
            )
            return True
        except ConflictError:
            return False  # another worker claimed it first

    async def process_once(self) -> int:
        """Process pending requests; returns how many this worker executed."""
        resp = await self.es.search(
            index=INDEX,
            ignore_unavailable=True,
            seq_no_primary_term=True,
            body={
                "query": {"term": {"status": RestartStatus.PENDING}},
                "sort": [{"requested_at": {"order": "asc", "unmapped_type": "date"}}],
                "size": 20,
            },
        )
        executed = 0
        for hit in resp["hits"]["hits"]:
            if not await self._claim(hit):
                continue
            req = hit["_source"]
            ok = await self.restarter.restart(req["node_id"], req["service_name"])
            await self.es.update(
                index=INDEX,
                id=hit["_id"],
                body={
                    "doc": {
                        "status": RestartStatus.DONE if ok else RestartStatus.FAILED
                    }
                },
                refresh="wait_for",
            )
            executed += 1
        return executed

    async def run(self) -> None:
        while True:
            try:
                await self.process_once()
            except Exception as exc:
                print(f"[restart_worker] error: {exc}")
            await asyncio.sleep(POLL_INTERVAL)
