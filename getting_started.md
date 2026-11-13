# Getting Started with Distributed Log System (DLS)

This guide walks you through configuring and starting the DLS stack with Docker Compose, and integrating structured JSON logging into your own applications.

---

## 1. Prerequisites

- Docker and Docker Compose v2 (`docker compose version`)

No other local toolchain is required — Python and Go services all build inside their containers.

---

## 2. Configuration

Copy the example environment file and fill in real values:

```bash
cp .env.example .env
```

At minimum, set `API_KEY_SALT`, `ADMIN_SECRET` and `WEBHOOK_ENCRYPTION_KEY` to random values instead of the `change-me-in-production` defaults:

```bash
python -c "import secrets; print(secrets.token_hex(32))"
```

Other settings in `.env` (all optional, sensible defaults are pre-filled):

* **`RESTART_BACKEND`**: `docker` (local Docker-based restart) or `kubernetes` (K8s pod restart)
* **Restarts:** `POST /v1/nodes/{id}/restart` only *queues* a request (HTTP 202); `heartbeat_service` executes it, so the API gateway needs no Docker socket. The key must have been issued with `can_restart: true`.
* **Tenant scoping:** issue keys with `POST /admin/keys` and `{"owner_email": "...", "node_ids": ["<node-id>"], "can_restart": false}`. A key with `node_ids` only sees those nodes' logs/nodes/heartbeats and only receives webhook events for them; an empty list means all nodes.
* **`WEBHOOK_ALLOW_PRIVATE_TARGETS`** / **`WEBHOOK_ALLOW_HTTP`**: webhook targets must be public https by default; enable the first for a local server, the second for public plain http. Link-local/metadata addresses are always blocked.
* **`ERROR_SPIKE_THRESHOLD`** / **`ERROR_SPIKE_WINDOW_SECONDS`**: when a node logs that many ERRORs in the window, the `error_spike` webhook event fires (5 min cooldown per node)
* **`K8S_NAMESPACE`**: only used when `RESTART_BACKEND=kubernetes`
* **`WEBHOOK_URL`** / **`WEBHOOK_SECRET`**: if set, `heartbeat_service` POSTs `node_down`/`node_up` events there, HMAC-signed
* **`HEARTBEAT_INTERVAL_SECONDS`** / **`HEARTBEAT_TIMEOUT_SECONDS`**: how often services heartbeat and how long before a missed node is marked failed

---

## 3. Starting the Local Log Pipeline

```bash
# Start the full stack: Zookeeper, Kafka, Elasticsearch, Kibana, Fluentd,
# log_pipeline, heartbeat_service, api_gateway, nginx, and demo microservices
docker compose up -d --build

# Verify all services are healthy
docker compose ps
```

To iterate faster on just the microservices, start infrastructure only first:

```bash
docker compose up -d zookeeper kafka elasticsearch
```

---

## 4. Connecting Your Applications (Structured JSON Logging)

To stay decoupled from Fluentd, your services must write structured JSON messages directly to standard output (`stdout`). This repo ships a ready-to-use JSON formatter in `common/logger.py` — reuse it rather than adding a new logging dependency.

### Python JSON Logging Example

```python
from common.logger import get_json_logger

logger = get_json_logger("order_service")

# Emit a log — Fluentd automatically parses and ships this JSON to Kafka -> ES
logger.info("Order placed successfully", extra={
    "node_id": "node-abc123",
    "service_name": "Order_Service",
    "message_type": "LOG",
    "order_id": "ord-88390",
    "amount": 99.95,
})
```

See `services/order/service.py` for a complete example, including heartbeat publishing via `common/heartbeat.py`.

---

## 5. Launching the Control Dashboard

The Streamlit dashboard is included as an optional Compose profile:

```bash
docker compose --profile demo up -d demo
```

The control UI is available at `http://localhost:8501`. It talks to the `api_gateway` REST API — set your API key in the sidebar to authenticate.

---

## 6. Browsing Logs in Kibana

Elasticsearch and Kibana are included in the stack for manual inspection:

```bash
# Kibana is available at:
open http://localhost:5601
```

Logs are indexed under the `distributed_logs-*` pattern; node health is in the `node_status` index.
