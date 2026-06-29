import asyncio
from logs_consumer import LogConsumer
from heartBeat_consumer import Heartbeat


async def main():
    log_consumer = LogConsumer(
        topic="logs",
        bootstrap_servers="localhost:9092",
        group_id="logs-consumer-group",
        offset_reset="latest",
    )

    heartbeat_consumer = Heartbeat()

    await log_consumer.start_consumer()

    await asyncio.gather(
        log_consumer.consume_logs(), heartbeat_consumer.consume_heartbeat()
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nShutting down both consumers...")
