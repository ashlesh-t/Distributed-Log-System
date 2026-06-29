import asyncio
import random
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from common.config import load_settings
from common.node_id import get_node_id
from common.heartbeat import HeartbeatPublisher
from common.logger import get_json_logger

SERVICE_NAME = "Inventory_Service"
logger = get_json_logger(SERVICE_NAME)

LOG_MESSAGES = {
    "INFO": [
        "InventoryService started successfully",
        "Stock updated successfully",
        "Product added to inventory",
        "Inventory checked",
        "Cache hit for product data",
    ],
    "WARN": [
        "Low stock warning",
        "Inventory sync delay detected",
        "Unexpected response from supplier API",
        "Slow stock update response time",
        "Product access unauthorized",
    ],
    "ERROR": [
        {"error_code": "INV001", "error_message": "Stock update failed"},
        {"error_code": "INV002", "error_message": "Database connection lost"},
        {"error_code": "INV003", "error_message": "Unable to retrieve supplier data"},
        {"error_code": "INV004", "error_message": "Invalid product ID"},
        {
            "error_code": "INV005",
            "error_message": "Unexpected server error in InventoryService",
        },
    ],
}


async def generate_logs(node_id: str) -> None:
    while True:
        level = random.choices(["INFO", "WARN", "ERROR"], weights=[0.75, 0.15, 0.10])[0]
        entry = random.choice(LOG_MESSAGES[level])

        data = {
            "node_id": node_id,
            "service_name": SERVICE_NAME,
            "message_type": "LOG",
            "log_level": level,
            "message": entry["error_message"] if level == "ERROR" else entry,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if level == "ERROR":
            data["error_details"] = entry
        elif level == "WARN":
            data["response_time_ms"] = random.randint(10, 100)

        if level == "ERROR":
            logger.error(data["message"], extra=data)
        elif level == "WARN":
            logger.warning(data["message"], extra=data)
        else:
            logger.info(data["message"], extra=data)

        await asyncio.sleep(random.uniform(0.1, 1.0))


async def main() -> None:
    cfg = load_settings()
    node_id = get_node_id(SERVICE_NAME)
    hb = HeartbeatPublisher(node_id, SERVICE_NAME, cfg.heartbeat_interval)

    registration_data = {
        "node_id": node_id,
        "service_name": SERVICE_NAME,
        "message_type": "REGISTRATION",
        "status": "UP",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    logger.info("Service registered", extra=registration_data)

    try:
        await asyncio.gather(hb.run_loop(), generate_logs(node_id))
    except asyncio.CancelledError:
        pass
    finally:
        await hb.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print(f"\n{SERVICE_NAME} shutting down gracefully.")
