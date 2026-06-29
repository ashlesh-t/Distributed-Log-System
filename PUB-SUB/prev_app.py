import asyncio
import json
from flask import Flask, render_template
from flask_socketio import SocketIO
from logs_consumer import LogConsumer
from heartBeat_consumer import Heartbeat
from datetime import datetime, timedelta, timezone

app = Flask(__name__)
socketio = SocketIO(app)


# Initialize consumers
log_consumer = LogConsumer(
    topic="logs",
    bootstrap_servers="localhost:9092",  # Kafka server
    group_id="logs-consumer-group",
    offset_reset="latest",
)

heartbeat_consumer = Heartbeat()

nodes = {}


@app.route("/")
def index():
    """Serve the main UI"""
    return render_template("index.html")


async def consume_logs_and_heartbeats():
    """Run both consumers and emit data to frontend"""

    # Start log consumer
    await log_consumer.start_consumer()
    await heartbeat_consumer.start_consumer()  # Start the heartbeat consumer

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

    # Run both tasks concurrently
    await asyncio.gather(log_consumer_task(), heartbeat_consumer_task())


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


if __name__ == "__main__":
    # Start background task to consume logs and heartbeats
    socketio.start_background_task(start_consumers)
    socketio.run(app, host="0.0.0.0", port=5000)  # nosec
