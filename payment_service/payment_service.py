import random
import asyncio
import uuid
from datetime import datetime, timedelta, timezone
from log_accumulator import Logger1
import json

from generate_id import GETKEY

id = GETKEY(service="payment_service", key_file="payment_service")
node_id = id.get_uuid()
# node_id = str(uuid.uuid4())
logs = None
heart_beat_status = ["UP", "DOWN"]
log_messages = {
    "INFO": [
        "PaymentService started successfully",
        "Order processed successfully",
        "User authenticated",
        "Database connection established",
        "Cache hit for user data",
    ],
    "WARN": [
        "Payment gateway timeout, retrying",
        "High memory usage detected",
        "Unexpected response from third-party API",
        "Slow response time detected",
        "User attempted unauthorized action",
    ],
    "ERROR": [
        {"error_code": "ERR001", "error_message": "Payment transaction failed"},
        {"error_code": "ERR002", "error_message": "Database connection lost"},
        {"error_code": "ERR003", "error_message": "Unable to send email notification"},
        {"error_code": "ERR004", "error_message": "Invalid request payload"},
        {
            "error_code": "ERR005",
            "error_message": "Server encountered an unexpected error",
        },
    ],
}


async def registration():
    IST = timezone(timedelta(hours=5, minutes=30))
    data = {
        "node_id": node_id,
        "message_type": "REGISTRATION",
        "service_name": "Payment_Service",
        "timestamp": datetime.now(IST).isoformat(),
    }
    Logger1(reg=data)


async def generate_log():
    IST = timezone(timedelta(hours=5, minutes=30))
    # added weights for logs
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
                "service_name": "Payment_Service",
                "timestamp": date,
            }
        elif generated_log == "WARN":
            logs = {
                "log_id": id,
                "node_id": node_id,
                "log_level": "WARN",
                "message_type": "LOG",
                "message": getmessage(generated_log),
                "service_name": "Payment_Service",
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
                "service_name": "Payment_Service",
                "error_details": {
                    "error_code": getmessage(generated_log)["error_code"],
                    "error_message": getmessage(generated_log)["error_message"],
                },
                "timestamp": date,
            }
        else:
            logs = None

        Logger1(logs=logs)
        await asyncio.sleep(random.uniform(0.1, 1.0))  # Non-blocking sleep


async def main():
    await registration()
    await asyncio.gather(print_heartbeat(), generate_logs())
    print("In main..")


try:
    print("Running paymentService...")
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
        "service_name": "Payment_Service",
        "timestamp": datetime.now(timezone(timedelta(hours=5, minutes=30))).isoformat(),
    }
    Logger1().close(close_log)
