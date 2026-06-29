from aiokafka import AIOKafkaConsumer
from datetime import datetime
import json
import asyncio
from logStr.elasticSearch import ElasticsearchLogStorage
from logStr.nodeStatusManager import (
    NodeStatusManager,
)  # Import your NodeStatusManager class

EXPECTED_INTERVAL = 5  # Expected interval between heartbeats in seconds


class Heartbeat:
    def __init__(self):
        self.es_storage = ElasticsearchLogStorage()
        self.heart_beat = {}  # Dictionary to track heartbeat statuses and timestamps
        self.consumer = None  # Kafka consumer attribute of the class
        self.manager = NodeStatusManager()  # Initialize NodeStatusManager

    async def start_consumer(self):
        """Initialize and start the Kafka consumer for heartbeat messages."""
        loop = asyncio.get_event_loop()
        self.consumer = AIOKafkaConsumer(
            "heartbeat",  # Topic name
            loop=loop,
            bootstrap_servers="kafka:9092",
            group_id="heartbeat-consumer-group",
            auto_offset_reset="earliest",  # Start reading messages from the earliest
        )
        await self.consumer.start()  # Start the Kafka consumer
        print("Heartbeat consumer started...")

    async def check_heart_beat(self, msg):
        """
        Check the incoming heartbeat message and perform actions
        based on node status, interval, and service restarts.
        """
        node_id = msg["node_id"]
        status = msg["status"]
        timestamp = msg["timestamp"]

        # If node is seen for the first time, store its status and timestamp
        if node_id not in self.heart_beat:
            self.heart_beat[node_id] = {"status": status, "timestamp": timestamp}
            print(f"First Incoming heartbeat from {node_id}, timestamp: {timestamp}")
            await self.manager.upsert_node_status(
                node_id, status
            )  # Store the initial status
            return None

        # Handle if node shuts down gracefully
        if status == "DOWN":
            print(f"ALERT!! {node_id} shut down gracefully")
            self.heart_beat[node_id]["status"] = "DOWN"
            self.heart_beat[node_id]["timestamp"] = timestamp  # Update timestamp
            try:
                logs = {
                    "node_id": node_id,
                    "message_type": "REGISTRATION",
                    "Status": "DOWN",
                    "timestamp": timestamp,
                }
                await self.es_storage._create_index_template()
                await self.es_storage.store_logs([logs])
            except Exception as e:
                print(f"Error flushing logs to Elasticsearch: {e}")
            await self.manager.upsert_node_status(
                node_id, "inactive"
            )  # Update status in Elasticsearch
            await self.manager.delete_node_status(node_id)
            return None

        # Handle service restart or status change
        if status != self.heart_beat[node_id]["status"]:
            print(f"Service with node id: {node_id} Restarted")
            self.heart_beat[node_id]["status"] = "UP"  # Update status to 'UP'
            await self.manager.upsert_node_status(
                node_id, "active"
            )  # Update status in Elasticsearch
            return None

        # Calculate the time interval between the previous and current heartbeat
        previous_time = datetime.fromisoformat(self.heart_beat[node_id]["timestamp"])
        current_time = datetime.fromisoformat(timestamp)
        actual_interval = (current_time - previous_time).total_seconds()
        actual_interval_rounded = round(actual_interval, 2)

        # Check if the interval exceeds the expected interval
        if actual_interval_rounded > EXPECTED_INTERVAL:
            print(
                f"ALERT: Delay detected for {node_id}! Interval: {actual_interval_rounded} seconds"
            )
            self.heart_beat[node_id]["timestamp"] = timestamp
            await self.manager.upsert_node_status(
                node_id, "active"
            )  # Update status in Elasticsearch
            return True
        else:
            print(f"{node_id} : UP")

        # Update the heartbeat timestamp for the node
        self.heart_beat[node_id]["timestamp"] = timestamp
        # Update status as the node is active
        return None

    async def consume_heartbeat(self):
        """Consume heartbeat messages from the Kafka topic."""
        print("Consuming heartbeat messages...")
        try:
            async for msg in self.consumer:
                msg_value = msg.value.decode("utf-8")  # Decode the Kafka message
                msg_dict = json.loads(msg_value)  # Convert message to a dictionary
                await self.check_heart_beat(msg_dict)  # Check and process the heartbeat
        except KeyboardInterrupt:
            print("\nShutting down heartbeat consumer...")
        finally:
            # Ensure Kafka consumer is stopped gracefully
            await self.consumer.stop()
            print("Heartbeat consumer stopped.")
