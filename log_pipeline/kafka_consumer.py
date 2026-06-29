import asyncio
import json
import os

from aiokafka import AIOKafkaConsumer

from log_pipeline.elastic import ElasticsearchLogStorage

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
BUFFER_SIZE = int(os.environ.get("LOG_BUFFER_SIZE", "10"))


class LogConsumer:
    def __init__(self, storage: ElasticsearchLogStorage):
        self._storage = storage
        self._buffer: list[dict] = []
        self._consumer: AIOKafkaConsumer | None = None

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            "logs",
            bootstrap_servers=KAFKA_SERVERS,
            group_id="log-consumer-group",
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        print("[kafka_consumer] logs consumer started")

    async def consume(self) -> None:
        assert self._consumer
        try:
            async for msg in self._consumer:
                try:
                    data = json.loads(msg.value.decode())
                    self._buffer.append(data)
                    if len(self._buffer) >= BUFFER_SIZE:
                        await self._flush()
                except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                    print(f"[kafka_consumer] bad message: {exc}")
        finally:
            if self._buffer:
                await self._flush()
            await self._consumer.stop()

    async def _flush(self) -> None:
        if self._buffer:
            await self._storage.store_logs(list(self._buffer))
            self._buffer.clear()
