import random
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from logger_accumulator import FluentdLogger
import json

from generate_id import GETKEY

id = GETKEY(service="order_service", key_file="order_service")
node_id = id.get_uuid()
# node_id = str(uuid.uuid4())
heart_beat_status = ["UP", "DOWN"]

log_messages = {
    "INFO": [
        "OrderService started successfully",
        "Order placed successfully",
        "Order payment verified",
        "Inventory checked and updated",
        "Shipping process initiated",
    ],
    "WARN": [
        "Inventory low for product",
        "Order processing delayed",
        "Third-party shipping service timeout",
        "Slow response from inventory system",
        "Customer address verification issue",
    ],
    "ERROR": [
        {"error_code": "ERR101", "error_message": "Order placement failed"},
        {"error_code": "ERR102", "error_message": "Inventory check failed"},
        {"error_code": "ERR103", "error_message": "Payment verification failed"},
        {"error_code": "ERR104", "error_message": "Shipping process failed"},
        {"error_code": "ERR105", "error_message": "Unexpected error in order workflow"},
    ],
}

logger = FluentdLogger(tag="fluentd.order_service")


async def registration():
    IST = timezone(timedelta(hours=5, minutes=30))
    data = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": "Order_Service",
        "timestamp": datetime.now(IST).isoformat(),
    }
    logger.add_registration(data)


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
        logger.add_heartbeat(heartbeat_message)
        await asyncio.sleep(delay)


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
                "service_name": "Order_Service",
                "timestamp": date,
            }
        elif generated_log == "WARN":
            logs = {
                "log_id": id,
                "node_id": node_id,
                "log_level": "WARN",
                "message_type": "LOG",
                "message": getmessage(generated_log),
                "service_name": "Order_Service",
                "response_time_ms": random.randint(10, 100),
                "threshold_limit_ms": 100,
                "timestamp": date,
            }
        elif generated_log == "ERROR":
            message = getmessage(generated_log)
            logs = {
                "log_id": id,
                "node_id": node_id,
                "log_level": "ERROR",
                "message_type": "LOG",
                "message": message["error_message"],
                "service_name": "Order_Service",
                "error_details": {
                    "error_code": message["error_code"],
                    "error_message": message["error_message"],
                },
                "timestamp": date,
            }
        else:
            logs = None

        if logs:
            logger.add_log(logs)
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
    logger.add_heartbeat(heartbeat_message)
    print("\nKeyboardInterrupt detected. Shutting down gracefully...")
    print(json.dumps(heartbeat_message, indent=4))
    close_log = {
        "log_id": str(uuid.uuid4()),
        "node_id": node_id,
        "log_level": "INFO",
        "message_type": "CLOSE_LOG",
        "message": "SERVICE SHUTDOWN GRACEFULLY",
        "service_name": "Order_Service",
        "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat(),
    }
    logger.close(close_log)
