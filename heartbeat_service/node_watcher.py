import asyncio
import os
from datetime import datetime, timezone

from heartbeat_service.node_status import NodeStatusManager
from heartbeat_service.restart_handler import RestartHandler
from heartbeat_service.webhook_notifier import WebhookNotifier

TIMEOUT = float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "15"))
CHECK_INTERVAL = 2.0


class NodeWatcher:
    def __init__(self):
        self._last_seen: dict[str, datetime] = {}
        self._service_map: dict[str, str] = {}
        self._suspected: set[str] = set()
        self._known: set[str] = set()
        self._ns = NodeStatusManager()
        self._restarter = RestartHandler()
        self._webhooks = WebhookNotifier()

    def update(self, node_id: str, service_name: str, status: str) -> None:
        if status in ("UP", "HEARTBEAT"):
            self._last_seen[node_id] = datetime.now(timezone.utc)
            self._service_map[node_id] = service_name
            if node_id in self._suspected:
                self._suspected.discard(node_id)
                asyncio.create_task(self._mark_recovered(node_id, service_name))
            elif node_id not in self._known:
                # First heartbeat seen from this node — record it as active so
                # GET /v1/nodes shows it without waiting for a failure/recovery cycle.
                self._known.add(node_id)
                asyncio.create_task(self._ns.upsert(node_id, service_name, "active"))
        elif status == "DOWN":
            asyncio.create_task(self._mark_failed(node_id, service_name, graceful=True))

    async def _mark_recovered(self, node_id: str, service_name: str) -> None:
        print(f"[watcher] {node_id} ({service_name}) recovered")
        await self._ns.upsert(node_id, service_name, "active")
        await self._webhooks.notify(node_id, service_name, "node_up")

    async def _mark_failed(
        self, node_id: str, service_name: str, graceful: bool = False
    ) -> None:
        event = "node_down"
        print(f"[watcher] {node_id} ({service_name}) DOWN (graceful={graceful})")
        await self._ns.upsert(node_id, service_name, "failed")
        if not graceful:
            await self._restarter.restart(node_id, service_name)
        await self._webhooks.notify(node_id, service_name, event)

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
