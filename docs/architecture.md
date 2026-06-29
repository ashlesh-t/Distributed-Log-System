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

## 2. Component Explanations

### 1. User Application Logs (Non-Intrusive)
In production, your apps are decoupled from Fluent Bit. Rather than instantiating socket senders inside the application code, the app outputs structured logs as JSON to standard output. This eliminates network overhead from within the application threads and guarantees high availability.

### 2. Log Collection Daemon (Fluent Bit / Fluentd)
An agent runs on the host (or as a DaemonSet in Kubernetes). It pulls container engine logs, decodes the JSON, appends orchestration metadata (container name, node host, IP, pod name, namespace), and maps them into Kafka topics based on the specified log category.

### 3. Queue Buffer (Kafka)
Kafka cushions the system from spikes in ingestion traffic. If Elasticsearch experiences high write loads or becomes temporarily unavailable, Kafka queues the log events safely.

### 4. Storage Engine (Elasticsearch / OpenSearch)
* **Logs Indices**: Written to time-series indices structured as `distributed_logs-<node_id>`. Index settings are dynamically defined using Elasticsearch templates.
* **Health Status Index**: The `node_status` index maps active hosts.

### 5. API Gateway
A FastAPI service proxying Elasticsearch query searches. It handles client API Key validation, authorization checks, rate-limiting, and registration of HMAC-signed outbound alert webhooks.

---

## 3. Dynamic Configuration Flow (`dls init`)

```
   ┌──────────┐      1. configures settings       ┌────────────────┐
   │ dls init │ ────────────────────────────────► │ ~/.dls/config  │
   └────┬─────┘                                   └───────┬────────┘
        │                                                 │
        │ 2. generates configs                            │ 3. loads configurations
        ▼                                                 ▼
   ┌──────────────────────────────────────────────────────────────┐
   │  - Configures Elasticsearch mappings                         │
   │  - Deploys custom Fluent Bit configurations                  │
   │  - Initializes API Gateway environment                       │
   └──────────────────────────────────────────────────────────────┘
```
This dynamic process allows adding custom topics on the fly. The CLI translates your config options directly into routing keys and indices.
