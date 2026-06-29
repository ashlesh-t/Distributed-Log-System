# System Architecture — Decoupled PaaS Log Engine

This document details the system design, data flow, and components of the production-grade Distributed Log System (DLS).

---

## 1. High-Level Design Diagram

```
       ┌─────────────────────────────────────────────────────────────┐
       │                 User Microservices (PaaS)                   │
       │  Writes structured JSON logs asynchronously to stdout/stderr│
       └──────────────┬───────────────────────────────┬──────────────┘
                      │                               │
                      ▼                               ▼
               [Container Logs]                [Container Logs]
                      │                               │
       ┌──────────────▼───────────────────────────────▼──────────────┐
       │             Fluent Bit Daemon (Host/DaemonSet)             │
       │ - Tail logs, parse JSON, inject metadata                    │
       │ - Buffer logs locally on disk in case of network outages    │
       └──────────────────────────────┬──────────────────────────────┘
                                      │
                                      ▼
                                 Kafka Broker (Ingestion Buffer)
                                      │
                               ┌──────┴──────┐
                               ▼             ▼
                           [Topic: logs]   [Topic: heartbeat]
                               │             │
                       ┌───────▼───────┐     └───────────────┐
                       │ log_pipeline  │                     │
                       │ (Bulk ES In)  │                     │
                       └───────┬───────┘                     ▼
                               │                     heartbeat_service
                               │                     (Orchestrator Probes)
                               │                             │
                               ▼                             ▼
       ┌─────────────────────────────────────────────────────────────┐
       │           Elasticsearch Cluster (Storage Engine)            │
       │  - logs index: distributed_logs-*                           │
       │  - status index: node_status                                │
       └──────────────────────────────▲──────────────────────────────┘
                                      │
                                      ▼
                               api_gateway (FastAPI Rest API)
                                      ▲
                                      │
                                 Streamlit Dashboard
```

---

## 2. Component Roles

### 1. Application Layer (User Microservices)
Client microservices are completely decoupled from the logging infrastructure. Rather than instantiating TCP sockets or using custom network logs drivers within the app code, services simply write standard structured JSON objects to `sys.stdout` or `sys.stderr`. 

### 2. Collection Layer (Fluentd / Fluent Bit)
Fluentd runs as an independent container/daemon. It tails container standard output streams, parses the nested JSON payloads, promotes custom fields (like `node_id`, `service_name`, `message_type`) to root attributes, and dynamically routes the records using a rewrite tag filter:
* Logs with `message_type: HEARTBEAT` are tagged and pushed to the Kafka **`heartbeat`** topic.
* All other log categories (e.g. `LOG`, `REGISTRATION`) are pushed to the Kafka **`logs`** topic.

### 3. Broker Buffer (Kafka)
Kafka serves as a highly scalable buffer, protecting Elasticsearch from write ingestion spikes and ensuring log preservation during backend outages.

### 4. Processing Pipeline (`log_pipeline`)
Consumes log payloads from the Kafka `logs` topic, formats them, and writes them in batches using the bulk ingestion protocol to **Elasticsearch**.

### 5. Failure Detection & Alerting (`heartbeat_service`)
Consumes heartbeats from the Kafka `heartbeat` topic. It tracks active node timestamps. If a node fails to send a heartbeat within `HEARTBEAT_TIMEOUT_SECONDS`, the service upserts the node state as failed, executes container restarts (using Docker/K8s APIs), and triggers registered client webhooks.

### 6. Control Gateway (`api_gateway`)
A secure FastAPI gateway proxying read requests to Elasticsearch. It handles tenant routing via custom API Keys, enforces client rate limiting, and manages webhook registrations.

---

## 3. Data Schema & Elasticsearch Mapping

Logs are indexed in Elasticsearch under `distributed_logs-*` pattern. 
* **Settings**: `number_of_shards: 1`, `number_of_replicas: 1`
* **Properties**:
  * `node_id` (`keyword`) — ID of the container instance.
  * `message_type` (`keyword`) — Record category (`LOG`, `REGISTRATION`, `HEARTBEAT`, `CLOSE_LOG`).
  * `service_name` (`keyword`) — Service identifier (e.g. `Order_Service`).
  * `timestamp` (`date`) — ISO 8601 creation timestamp.
  * `log_level` (`keyword`) — `INFO`, `WARN`, `ERROR`.
  * `message` (`text`) — Full-text searchable message field.
  * `response_time_ms` / `threshold_limit_ms` (`integer`) — Optional performance telemetry.
  * `error_details` (`object`) — Holds structured exception stack details.
