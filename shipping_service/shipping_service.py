import random
import asyncio
import uuid
import json
from datetime import datetime, timedelta, timezone
from log_accumulator import Logger1
from generate_id import GETKEY

id = GETKEY(service="shipping_service", key_file="shipping_service")
node_id = id.get_uuid()
# node_id = str(uuid.uuid4())
logs = None
heart_beat_status = ["UP", "DOWN"]
log_messages = {
    "INFO": [
        "ShippingService started successfully",
        "Order shipped successfully",
        "Tracking number generated",
        "Shipping address verified",
        "Delivery date updated",
    ],
    "WARN": [
        "Delayed shipment warning",
        "Incomplete shipping address detected",
        "Carrier response delay",
        "High shipment volume warning",
        "Unauthorized access attempt",
    ],
    "ERROR": [
        {
            "error_code": "SHIP001",
            "error_message": "Failed to generate tracking number",
        },
        {"error_code": "SHIP002", "error_message": "Carrier service unavailable"},
        {"error_code": "SHIP003", "error_message": "Invalid shipping address"},
        {"error_code": "SHIP004", "error_message": "Shipment lost in transit"},
        {
            "error_code": "SHIP005",
            "error_message": "Unexpected server error in ShippingService",
        },
    ],
}


async def registration():
    IST = timezone(timedelta(hours=5, minutes=30))
    data = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": "shipping_service",
        "timestamp": datetime.now(IST).isoformat(),
    }
    Logger1(reg=data)


async def generate_log():
    IST = timezone(timedelta(hours=5, minutes=30))
    return (
        random.choices(["INFO", "WARN", "ERROR"], weights=[0.75, 0.15, 0.10], k=1)[0],
        str(uuid.uuid4()),
        datetime.now(IST).isoformat(),
    )


async def print_heartbeat():
    randomTimeDelayCount = random.randint(5, 10)
    time_count = 0
    count = 0
    while True:
        IST = timezone(timedelta(hours=5, minutes=30))
        heartbeat_message = {
            "node_id": node_id,
            "message_type": "HEARTBEAT",
            "status": heart_beat_status[0],
            "timestamp": datetime.now(IST).isoformat(),
        }
        delay = 5
        if count == 10:
            delay = 30
            count = 0
            time_count = 0
        time_count += 1
        count += 1
        if time_count == randomTimeDelayCount:
            time_count = 0
            randomTimeDelayCount = random.randint(5, 10)
            delay = random.randint(6, 7)
        Logger1(heartbeat=heartbeat_message)
        await asyncio.sleep(delay)  # Non-blocking sleep


def getmessage(log_level):
    return random.choice(log_messages[log_level])


async def generate_logs():
    while True:
        generated_log, id, date = await generate_log()
        if generated_log == "INFO":
            logs = {
                "log_id": id,
                "node_id": node_id,
                "log_level": "INFO",
                "message_type": "LOG",
                "message": getmessage(generated_log),
                "service_name": "shipping_service",
                "timestamp": date,
            }
        elif generated_log == "WARN":
            logs = {
                "log_id": id,
                "node_id": node_id,
                "log_level": "WARN",
                "message_type": "LOG",
                "message": getmessage(generated_log),
                "service_name": "shipping_service",
                "response_time_ms": random.randint(10, 100),
                "threshold_limit_ms": 100,
                "timestamp": date,
            }
        elif generated_log == "ERROR":
            logs = {
                "log_id": id,
                "node_id": node_id,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": getmessage(generated_log)["error_message"],
                "service_name": "shipping_service",
                "error_details": {
                    "error_code": getmessage(generated_log)["error_code"],
                    "error_message": getmessage(generated_log)["error_message"],
                },
                "timestamp": date,
            }
        else:
            logs = None

        Logger1(logs=logs)
        await asyncio.sleep(random.uniform(0.1, 1.0))


async def main():
    await registration()
    await asyncio.gather(print_heartbeat(), generate_logs())


try:
    asyncio.run(main())
except KeyboardInterrupt:
    IST = timezone(timedelta(hours=5, minutes=30))
    heartbeat_message = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": heart_beat_status[1],
        "timestamp": datetime.now(IST).isoformat(),
    }
    Logger1(heartbeat=heartbeat_message)
    print("\nKeyboardInterrupt detected. Shutting down gracefully...")
    print(json.dumps(heartbeat_message, indent=4))
    close_log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "CLOSE_LOG",
        "message": "SERVICE SHUTDOWN GRACEFULLY",
        "service_name": "shipping_service",
        "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat(),
    }

    Logger1().close(close_log)
