import asyncio
from datetime import datetime, timezone
from common.logger import get_json_logger

logger = get_json_logger("heartbeat")

class HeartbeatPublisher:
    """Publishes heartbeats by writing structured JSON to stdout/logger."""

    def __init__(
        self,
        node_id: str,
        service_name: str,
        interval: float = 5.0,
    ):
        self._node_id = node_id
        self._service_name = service_name
        self._interval = interval

    async def publish(self, status: str = "UP") -> None:
        data = {
            "node_id": self._node_id,
            "service_name": self._service_name,
            "message_type": "HEARTBEAT",
            "status": status,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info("Heartbeat sent", extra=data)

    async def publish_close(self) -> None:
        data = {
            "node_id": self._node_id,
            "service_name": self._service_name,
            "message_type": "CLOSE_LOG",
            "status": "DOWN",
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        logger.info("Service shutdown", extra=data)

    async def run_loop(self) -> None:
        """Continuously publish heartbeats. Call this as an asyncio task."""
        while True:
            await self.publish()
            await asyncio.sleep(self._interval)

    async def close(self) -> None:
        await self.publish_close()
