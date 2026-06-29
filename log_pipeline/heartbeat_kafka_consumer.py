import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer

from log_pipeline.node_status import NodeStatusManager

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


class HeartbeatKafkaConsumer:
    """Stores heartbeat events to Elasticsearch (storage only; failure detection is in heartbeat_service)."""

    def __init__(self, node_status: NodeStatusManager):
        self._ns = node_status
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            "heartbeat",
            bootstrap_servers=KAFKA_SERVERS,
            group_id="heartbeat-storage-group",
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        print("[heartbeat_kafka] consumer started")

    async def consume(self) -> None:
        assert self._consumer
        try:
            async for msg in self._consumer:
                try:
                    data = json.loads(msg.value.decode())
                    node_id = data.get("node_id", "unknown")
                    service_name = data.get("service_name", "unknown")
                    status = data.get("status", "UP")
                    await self._ns.upsert(node_id, service_name, "active" if status == "UP" else "inactive")
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    print(f"[heartbeat_kafka] bad message: {exc}")
        finally:
            await self._consumer.stop()
