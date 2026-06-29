import asyncio

from log_pipeline.elastic import ElasticsearchLogStorage
from log_pipeline.kafka_consumer import LogConsumer
from log_pipeline.heartbeat_kafka_consumer import HeartbeatKafkaConsumer
from log_pipeline.node_status import NodeStatusManager


async def main() -> None:
    storage = ElasticsearchLogStorage()
    ns = NodeStatusManager()

    log_consumer = LogConsumer(storage)
    hb_consumer = HeartbeatKafkaConsumer(ns)

    await asyncio.gather(
        _run(log_consumer),
        _run(hb_consumer),
    )


async def _run(consumer) -> None:
    await consumer.start()
    await consumer.consume()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[log_pipeline] shutting down")
