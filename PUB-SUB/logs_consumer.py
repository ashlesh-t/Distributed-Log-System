import asyncio
from aiokafka import AIOKafkaConsumer
import json
from termcolor import colored
import os
from logStr.elasticSearch import ElasticsearchLogStorage


class LogConsumer:
    def __init__(self, topic, bootstrap_servers, group_id, offset_reset="latest"):
        self.es_storage = ElasticsearchLogStorage()
        self.topic = topic
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.auto_offset_reset = offset_reset
        self.consumer = None
        self.log_buffer = []
        self.buffer_size = 10  # Adjust as needed
        self.buffer_flush_interval = 2  # Seconds

    async def flush_logs_to_elasticsearch(self):
        """Flush the buffered logs to Elasticsearch."""
        if self.log_buffer:
            try:
                await self.es_storage.store_logs(self.log_buffer)
                self.log_buffer.clear()
            except Exception as e:
                print(f"Error flushing logs to Elasticsearch: {e}")

    async def start_consumer(self):
        """Start the Kafka consumer"""
        await self.es_storage._create_index_template()
        self.consumer = AIOKafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers,
            group_id=self.group_id,
            auto_offset_reset=self.auto_offset_reset,
        )
        await self.consumer.start()
        print("Logs consumer started... Press Ctrl+C to stop.")

    async def stop_consumer(self):
        """Stop the Kafka consumer gracefully"""
        if self.consumer:
            await self.consumer.stop()
        print("Logs consumer stopped.")

    async def put_to_elasticSearch(self, msg):
        # Asynchronous function to put logs to Elasticsearch
        if msg["message_type"] == "REGISTRATION":
            msg["status"] = "UP"
        self.log_buffer.append(msg)
        if len(self.log_buffer) >= self.buffer_size:
            await self.flush_logs_to_elasticsearch()

    async def classify_logs(self, msg):
        """Classify and process the logs based on their type and log level"""
        await self.put_to_elasticSearch(msg)

        if msg["message_type"] == "REGISTRATION":
            message = f"Node ID: {msg['node_id']} , service_name: {msg['service_name']} REGISTERED SUCCESSFULLY"
            print(colored(message, "green", attrs=["bold"]))
            return

        if msg["message_type"] == "LOG":
            if msg["log_level"] == "INFO":
                print(
                    f"\033[96mINFO: | node_id: {msg['node_id']} | log_id: {msg['log_id']} | service_name: {msg['service_name']} | message: {msg['message']}\033[0m"
                )
            elif msg["log_level"] == "WARN":
                print(
                    f'\033[38;5;214m WARNING!! node_id: {msg["node_id"]} | log_id: {msg["log_id"]} | service_name: {msg["service_name"]} | message: {msg["message"]} \033[0m'
                )
            else:
                print(
                    f'\033[91m ERROR!! node_id: {msg["node_id"]} | log_id: {msg["log_id"]} | service_name: {msg["service_name"]} | message: {msg["message"]} | ERROR CODE: {msg["error_details"]["error_code"]} \033[0m'
                )

        elif msg["message_type"] == "CLOSE_LOG":
            terminal_width = os.get_terminal_size().columns
            message = f": |{msg['service_name']} SHUTDOWN Gracefully!! node_id: {msg['node_id']} | log_id: {msg['log_id']} | {msg['timestamp']}"
            centered_message = message.center(terminal_width)
            print(colored(centered_message, "blue", attrs=["bold"]))

    async def consume_logs(self):
        """Consume messages from the Kafka topic and classify them"""
        print("Logs consuming")
        try:
            async for msg in self.consumer:
                if msg.value:
                    msg = json.loads(msg.value.decode("utf-8"))
                    await self.classify_logs(msg)
                else:
                    print("Received empty message.")
        except KeyboardInterrupt:
            print("Stopping logs consumer...")
        except asyncio.CancelledError:
            print("Consumer task was canceled.")
        finally:
            await self.flush_logs_to_elasticsearch()
            await self.stop_consumer()
            await self.es_storage.close_connection()
