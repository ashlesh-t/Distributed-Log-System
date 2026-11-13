import asyncio
import os
from datetime import datetime, timezone

from heartbeat_service.restart_handler import RestartHandler
from heartbeat_service.webhook_notifier import WebhookNotifier
from heartbeat_service.node_status import NodeStatusManager

TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "15"))
CHECK_INTERVAL = 2.0
HEARTBEAT_RECORD_INTERVAL = float(os.environ.get("HEARTBEAT_RECORD_INTERVAL", "30"))


class NodeWatcher:
    def __init__(self):
        self._last_seen: dict[str, datetime] = {}
        self._service_map: dict[str, str] = {}
        self._suspected: set[str] = set()
        self._known: set[str] = set()
        self._failed: set[str] = set()
        self._last_recorded: dict[str, datetime] = {}
        self._ns = NodeStatusManager()
        self._restarter = RestartHandler()
        self._webhooks = WebhookNotifier()

    def update(self, node_id: str, service_name: str, status: str) -> None:
        if status in ("UP", "HEARTBEAT"):
            self._last_seen[node_id] = datetime.now(timezone.utc)
            self._service_map[node_id] = service_name
            self._record_heartbeat(node_id, service_name)
            if node_id in self._failed:
                # Node was declared down (crash or graceful stop) and is back.
                self._failed.discard(node_id)
                self._suspected.discard(node_id)
                self._known.add(node_id)
                asyncio.create_task(self._mark_recovered(node_id, service_name))
            elif node_id in self._suspected:
                # Missed a check but never declared down: no node_down was sent,
                # so there is nothing to recover from.
                self._suspected.discard(node_id)
            elif node_id not in self._known:
                # First heartbeat seen from this node — record it as active so
                # GET /v1/nodes shows it without waiting for a failure/recovery cycle.
                self._known.add(node_id)
                asyncio.create_task(self._ns.upsert(node_id, service_name, "active"))
        elif status == "DOWN":
            if node_id in self._failed:
                return
            # Stop timeout tracking, otherwise the watch loop would later declare
            # this gracefully stopped node failed again and try to restart it.
            self._last_seen.pop(node_id, None)
            self._suspected.discard(node_id)
            asyncio.create_task(self._mark_failed(node_id, service_name, graceful=True))

    def _record_heartbeat(self, node_id: str, service_name: str) -> None:
        # Sample, don't store every beat: keeps the heartbeats API useful without
        # an ES write per heartbeat per node.
        now = datetime.now(timezone.utc)
        last = self._last_recorded.get(node_id)
        if last is None or (now - last).total_seconds() >= HEARTBEAT_RECORD_INTERVAL:
            self._last_recorded[node_id] = now
            asyncio.create_task(self._ns.record_heartbeat(node_id, service_name))

    async def _mark_recovered(self, node_id: str, service_name: str) -> None:
        print(f"[watcher] {node_id} ({service_name}) recovered")
        await self._ns.upsert(node_id, service_name, "active")
        await self._webhooks.notify(node_id, service_name, "node_up")

    async def _mark_failed(
        self, node_id: str, service_name: str, graceful: bool = False
    ) -> None:
        event = "node_down"
        self._failed.add(node_id)
        print(f"[watcher] {node_id} ({service_name}) DOWN (graceful={graceful})")
        await self._ns.upsert(node_id, service_name, "failed")
        if not graceful:
            await self._restarter.restart(node_id, service_name)
        await self._webhooks.notify(node_id, service_name, event)

    async def purge_heartbeats(self) -> None:
        await self._ns.purge_old_heartbeats()

    async def watch_loop(self) -> None:
        while True:
            await asyncio.sleep(CHECK_INTERVAL)
            now = datetime.now(timezone.utc)
            for node_id, last in list(self._last_seen.items()):
                elapsed = (now - last).total_seconds()
                service_name = self._service_map.get(node_id, "unknown")
                if elapsed > TIMEOUT:
                    if node_id not in self._suspected:
                        self._suspected.add(node_id)
                    else:
                        self._suspected.discard(node_id)
                        del self._last_seen[node_id]
                        await self._mark_failed(node_id, service_name)
