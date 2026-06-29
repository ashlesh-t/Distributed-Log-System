# Getting Started with Distributed Log System (DLS)

This guide walks you through installing the `dls` CLI tool, initializing your PaaS environment, configuring log topics, and integrating structured JSON logging into your applications.

---

## 1. Installation

Install the DLS CLI command-line manager via your system's package manager.

### macOS & Linux (Homebrew)
```bash
brew tap ashlesh/dls
brew install dls
```

### Arch Linux (AUR via Yay)
```bash
yay -S dls-cli
```

---

## 2. Initialization & Configuration Wizard

Once installed, initialize your local configuration profile by running:

```bash
dls init
```

The terminal setup wizard will guide you through all necessary configurations:

1. **Deploy Model**: Choose between `Local` (Docker Compose development environment) or `Cloud` (Managed AWS/GCP services).
2. **Standard Log Topics**: Define your active logging channels. By default, the system initiates `logs` and `heartbeat`.
3. **Custom Topics**: Add extra log topics as a comma-separated list (e.g. `audit, payments, shipping`).
4. **Wiring Details**: For each custom topic, you will be prompted for:
   * **Topic Name**: (e.g., `payments`)
   * **Short Description**: (e.g., `Credit card transactions and gateway payloads`)
5. **Connection Settings**:
   * **Kafka Broker Addresses**: Default: `localhost:9092`
   * **Elasticsearch/OpenSearch URL**: Default: `http://localhost:9200`
   * **Restart Handler Backend**: `docker` or `kubernetes`

The wizard generates your configuration profile at `~/.dls/config.yaml` and prepares the dynamic configuration files (such as Fluent Bit filters and Elasticsearch mapping templates).

---

## 3. Starting the Local Log Pipeline

For local PaaS orchestration using Docker containers:

```bash
# Start all pipeline services (Zookeeper, Kafka, Elasticsearch, Fluent Bit, API Gateway)
dls start --local

# Verify all services are healthy
dls status
```

---

## 4. Connecting Your Applications (Structured JSON Logging)

To stay decoupled from Fluent Bit, your services must write structured JSON messages directly to standard output (`stdout`).

### Python JSON Logging Example
First, install the python JSON logging library:
```bash
pip install python-json-logger
```

Configure your logger in python:
```python
import logging
import sys
from pythonjsonlogger import jsonlogger

# Initialize logging to stdout
logHandler = logging.StreamHandler(sys.stdout)
formatter = jsonlogger.JsonFormatter(
    fmt='%(asctime)s %(levelname)s %(name)s %(message)s'
)
logHandler.setFormatter(formatter)

logger = logging.getLogger("order_service")
logger.addHandler(logHandler)
logger.setLevel(logging.INFO)

# Emit a log - Fluent Bit automatically parses and ships this JSON to Kafka -> ES
logger.info("Order placed successfully", extra={
    "node_id": "node-abc123",
    "service_name": "Order_Service",
    "order_id": "ord-88390",
    "amount": 99.95
})
```

---

## 5. Launching the Control Dashboard

Open the Streamlit dashboard locally to view node health, query logs, and manage alerts:

```bash
dls dashboard
```

The control UI is available at `http://localhost:8501`.
