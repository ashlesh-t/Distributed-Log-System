import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    fluentd_host: str
    fluentd_port: int
    kafka_bootstrap_servers: str
    elasticsearch_host: str
    service_name: str
    heartbeat_interval: float
    heartbeat_timeout: float
    api_key_salt: str
    admin_secret: str
    restart_backend: str  # "docker" or "kubernetes"
    webhook_url: str  # global fallback webhook (optional)
    k8s_namespace: str


def load_settings() -> Settings:
    return Settings(
        fluentd_host=os.environ.get("FLUENTD_HOST", "fluentd"),
        fluentd_port=int(os.environ.get("FLUENTD_PORT", "9880")),
        kafka_bootstrap_servers=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
        elasticsearch_host=os.environ.get(
            "ELASTICSEARCH_HOST", "http://elasticsearch:9200"
        ),
        service_name=os.environ.get("SERVICE_NAME", "unknown_service"),
        heartbeat_interval=float(os.environ.get("HEARTBEAT_INTERVAL_SECONDS", "5")),
        heartbeat_timeout=float(os.environ.get("HEARTBEAT_TIMEOUT_SECONDS", "15")),
        api_key_salt=os.environ.get("API_KEY_SALT", "change-me-in-production"),
        admin_secret=os.environ.get("ADMIN_SECRET", "change-me-in-production"),
        restart_backend=os.environ.get("RESTART_BACKEND", "docker"),
        webhook_url=os.environ.get("WEBHOOK_URL", ""),
        k8s_namespace=os.environ.get("K8S_NAMESPACE", "distributed-logs"),
    )
