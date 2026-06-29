import asyncio
import json
import signal
import sys
from flask import Flask, render_template
from flask_socketio import SocketIO
from logs_consumer import LogConsumer
from heartBeat_consumer import Heartbeat
from datetime import datetime, timedelta, timezone
from logStr.nodeStatusManager import NodeStatusManager

# Initialize Flask app and SocketIO
app = Flask(__name__)
socketio = SocketIO(app)
manager = NodeStatusManager()  # Initialize NodeStatusManager

# Initialize consumers
log_consumer = LogConsumer(
    topic="logs",
    bootstrap_servers="kafka:9092",  # Kafka server
    group_id="logs-consumer-group",
    offset_reset="latest",
)

heartbeat_consumer = Heartbeat()

nodes = {}


@app.route("/")
def index():
    """Serve the main UI"""
    return render_template("index.html")


async def check_heartbeat_timeout():
    """
    Periodically check if any node has missed its heartbeat for more than 10 seconds.
    """
    while True:
        current_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        nodes = await manager.get_all_nodes()  # Retrieve all nodes

        for node_id, node_info in nodes.items():
            last_seen = datetime.fromisoformat(node_info.get("timestamp", ""))

            # Ensure `last_seen` is aware by adding timezone if missing
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(
                    tzinfo=timezone(timedelta(hours=5, minutes=30))
                )

            elapsed_time = (current_time - last_seen).total_seconds()
            if (
                elapsed_time > 10 and node_info["status"] == "active"
            ):  # If the node has been inactive for more than 10 seconds
                print(
                    f"Node {node_id} is failed. Last heartbeat received {elapsed_time:.2f} seconds ago."
                )
                await manager.upsert_node_status(
                    node_id, "failed"
                )  # Update status using NodeStatusManager
                socketio.emit("node_failed", {"node_id": node_id, "status": "failed"})

        # Sleep for 1 second before checking again
        await asyncio.sleep(1)


async def check_active_nodes():
    """
    Periodically check if a node is still active and print 'ACTIVE' when it is.
    """
    while True:
        current_time = datetime.now(timezone(timedelta(hours=5, minutes=30)))
        nodes = await manager.get_all_nodes()  # Retrieve all nodes

        for node_id, node_info in nodes.items():
            last_seen = datetime.fromisoformat(node_info.get("timestamp", ""))

            # Ensure `last_seen` is aware by adding timezone if missing
            if last_seen.tzinfo is None:
                last_seen = last_seen.replace(
                    tzinfo=timezone(timedelta(hours=5, minutes=30))
                )

            elapsed_time = (current_time - last_seen).total_seconds()
            if (
                elapsed_time <= 10 and node_info["status"] == "active"
            ):  # Node is active if it has been seen within the last 10 seconds
                print(
                    f"Node {node_id} is ACTIVE. Last heartbeat received {elapsed_time:.2f} seconds ago."
                )
                # await manager.upsert_node_status(node_id, 'active')  # Update status using NodeStatusManager
                socketio.emit("node_active", {"node_id": node_id, "status": "active"})

        # Sleep for 1 second before checking again
        await asyncio.sleep(1)


async def consume_logs_and_heartbeats():
    """Run log and heartbeat consumers and emit data to frontend."""

    # Start the log and heartbeat consumers
    await log_consumer.start_consumer()
    await heartbeat_consumer.start_consumer()

    # Function to consume log messages and emit events
    async def log_consumer_task():
        async for msg in log_consumer.consumer:
            if msg.value:
                msg = json.loads(msg.value.decode("utf-8"))
                await log_consumer.classify_logs(msg)
                emit_log_message(msg)

    # Function to consume heartbeat messages and emit events
    async def heartbeat_consumer_task():
        async for msg in heartbeat_consumer.consumer:
            msg_value = msg.value.decode("utf-8")
            msg_dict = json.loads(msg_value)
            res = await heartbeat_consumer.check_heart_beat(msg_dict)
            emit_heartbeat_alert(msg_dict, res)

    # Run all tasks concurrently
    await asyncio.gather(
        log_consumer_task(),
        heartbeat_consumer_task(),
        check_heartbeat_timeout(),  # Periodically check heartbeat timeout
        check_active_nodes(),  # Periodically check active nodes
    )


def emit_log_message(msg):
    """Emit log messages to frontend"""
    log_level = msg.get("log_level", "REGISTRATION")
    node_id = msg.get("node_id")
    service_name = msg.get("service_name")
    message = msg.get("message", "")
    timestamp = msg.get(
        "timestamp", datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat()
    )

    if log_level == "REGISTRATION":
        nodes[node_id] = service_name
        socketio.emit("node_registration", {"node_id": node_id, "timestamp": timestamp})
    elif log_level == "ERROR":
        socketio.emit(
            "error_alert",
            {
                "node_id": node_id,
                "service_name": service_name,
                "message": message,
                "timestamp": timestamp,
            },
        )
    elif log_level == "WARN":
        socketio.emit(
            "warn_message",
            {
                "node_id": node_id,
                "service_name": service_name,
                "message": message,
                "timestamp": timestamp,
            },
        )
    else:
        socketio.emit(
            "info_message",
            {"node_id": node_id, "message": message, "timestamp": timestamp},
        )


def emit_heartbeat_alert(msg, res):
    """Emit node heartbeat status alerts and registration"""
    node_id = msg["node_id"]
    status = msg["status"]
    msg["timestamp"]

    if res:
        socketio.emit("delay_detected", {"node_id": node_id})

    # Handle node UP or DOWN status
    if status == "DOWN":
        socketio.emit("node_failure_alert", {"node_id": node_id, "status": "DOWN"})
    else:
        socketio.emit("node_up_alert", {"node_id": node_id, "status": "UP"})


def start_consumers():
    """Start the consumers and WebSocket handler"""
    asyncio.run(consume_logs_and_heartbeats())


def handle_shutdown_signal(signal, frame):
    """Handle shutdown signals like Ctrl+C."""
    print("Application is shutting down...")
    # socketio.stop()  # Stop the SocketIO server gracefully
    asyncio.run(manager.close())  # Close any asynchronous resources like Elasticsearch
    sys.exit(0)


# Register the signal handler for SIGINT (Ctrl+C)
signal.signal(signal.SIGINT, handle_shutdown_signal)

if __name__ == "__main__":
    try:
        # Start background task to consume logs and heartbeats
        socketio.start_background_task(start_consumers)
        print("Server started. Press Ctrl+C to shut down.")
        socketio.run(app, host="0.0.0.0", port=5000)  # nosec
    except KeyboardInterrupt:
        handle_shutdown_signal(None, None)
