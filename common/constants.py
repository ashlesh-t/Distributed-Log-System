"""Shared constants for the API gateway and heartbeat_service.

Single source of truth for Elasticsearch index names, status values and
default/placeholder settings so the two services can't drift apart.
"""

# ── Elasticsearch indices ─────────────────────────────────────────────────────
LOG_INDEX_PATTERN = "distributed_logs-*"
NODE_STATUS_INDEX = "node_status"
NODE_HEARTBEAT_INDEX = "node_heartbeats"  # sampled heartbeats, served by the API
RESTART_REQUEST_INDEX = "restart_requests"  # queue: gateway -> heartbeat_service
API_KEY_INDEX = "api_keys"
WEBHOOK_INDEX = "webhooks"
ERROR_SPIKE_STATE_INDEX = "error_spike_state"  # shared alert cooldown claims

# ── Defaults ──────────────────────────────────────────────────────────────────
DEFAULT_ES_HOST = "http://elasticsearch:9200"
# Placeholder shipped in compose/k8s; services warn if it is still in use.
INSECURE_DEFAULT_SECRET = "change-me-in-production"
MAX_RESULT_WINDOW = 10000  # Elasticsearch index.max_result_window default

# ── Environment variable names ────────────────────────────────────────────────
ENV_ES_HOST = "ELASTICSEARCH_HOST"
ENV_WEBHOOK_ENCRYPTION_KEY = "WEBHOOK_ENCRYPTION_KEY"

# ── Secret storage ────────────────────────────────────────────────────────────
ENCRYPTED_PREFIX = "enc:"  # marks encrypted values; unprefixed = legacy plaintext


# ── Node status values ────────────────────────────────────────────────────────
class NodeStatus:
    ACTIVE = "active"
    FAILED = "failed"


# ── Restart request lifecycle ─────────────────────────────────────────────────
class RestartStatus:
    PENDING = "pending"
    RUNNING = "running"
    DONE = "done"
    FAILED = "failed"
