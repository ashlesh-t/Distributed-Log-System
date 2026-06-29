from datetime import datetime, timezone


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def build_registration(node_id: str, service_name: str) -> dict:
    return {
        "node_id": node_id,
        "service_name": service_name,
        "message_type": "REGISTRATION",
        "status": "UP",
        "timestamp": _now(),
    }


def build_log(
    node_id: str,
    service_name: str,
    log_level: str,
    message: str,
    error_details: dict | None = None,
    response_time_ms: int | None = None,
) -> dict:
    data = {
        "node_id": node_id,
        "service_name": service_name,
        "message_type": "LOG",
        "log_level": log_level,
        "message": message,
        "timestamp": _now(),
    }
    if error_details:
        data["error_details"] = error_details
    if response_time_ms is not None:
        data["response_time_ms"] = response_time_ms
    return data


def build_heartbeat(node_id: str, service_name: str, status: str = "UP") -> dict:
    return {
        "node_id": node_id,
        "service_name": service_name,
        "message_type": "HEARTBEAT",
        "status": status,
        "timestamp": _now(),
    }


def build_close(node_id: str, service_name: str) -> dict:
    return {
        "node_id": node_id,
        "service_name": service_name,
        "message_type": "CLOSE_LOG",
        "status": "DOWN",
        "timestamp": _now(),
    }
