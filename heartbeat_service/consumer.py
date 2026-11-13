import asyncio
import json
import os
from aiokafka import AIOKafkaConsumer

from heartbeat_service.error_spike import ErrorSpikeDetector
from heartbeat_service.node_watcher import NodeWatcher
from heartbeat_service.restart_worker import RestartWorker

KAFKA_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


async def _purge_heartbeats_hourly(watcher: NodeWatcher) -> None:
    while True:
        await watcher.purge_heartbeats()
        await asyncio.sleep(3600)


async def main() -> None:
    watcher = NodeWatcher()
    asyncio.create_task(watcher.watch_loop())
    asyncio.create_task(RestartWorker().run())
    asyncio.create_task(ErrorSpikeDetector().run())
    asyncio.create_task(_purge_heartbeats_hourly(watcher))

    for attempt in range(10):
        try:
            consumer = AIOKafkaConsumer(
                "heartbeat",
                bootstrap_servers=KAFKA_SERVERS,
                group_id="heartbeat-watcher-group",
                auto_offset_reset="latest",
            )
            await consumer.start()
            break
        except Exception as exc:
            wait = min(2**attempt, 30)
            print(f"[heartbeat_service] Kafka connect failed ({exc}), retry in {wait}s")
            await asyncio.sleep(wait)
    else:
        print("[heartbeat_service] Cannot connect to Kafka — giving up")
        return

    print("[heartbeat_service] listening for heartbeats via Kafka")
    try:
        async for msg in consumer:
            try:
                data = json.loads(msg.value.decode())
                node_id = data.get("node_id", "")
                service_name = data.get("service_name", "")
                status = data.get("status", "UP")
                if node_id:
                    watcher.update(node_id, service_name, status)
            except Exception as exc:
                print(f"[heartbeat_service] message error: {exc}")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[heartbeat_service] shutting down")
