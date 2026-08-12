# Distributed Log System (DLS) — PaaS Framework

> **A production-ready Platform as a Service (PaaS) framework for decoupled log aggregation, unified health monitoring, and alerting. Package-installable and fully configurable for local development or cloud clusters.**

---

## Architecture Overview

DLS eliminates client-side coupling by utilizing a **decoupled log shipping pattern**. Microservices publish structured JSON logs directly to standard output (`stdout`), where a low-overhead collector (Fluentd) tails, parses, and pushes them to **Kafka**. An ingestion pipeline then consumes the logs in bulk and writes them to **Elasticsearch**.

For detailed system diagrams and component logs, see [architecture.md](file:///home/ashlesh/my_works/Distributed-Log-System/docs/architecture.md).

---

## Key Components

1. **Stdout Ingestion Routing**: Client applications are completely decoupled from Fluentd. Standard logging libraries write to stdout, making log aggregation non-blocking and robust.
2. **Docker Compose Deployment**: The entire stack (Kafka, Elasticsearch, Fluentd, pipeline, gateway, microservices) is defined in `docker-compose.yml` and started with a single command — no separate CLI install required.
3. **PaaS API Gateway**: Secure FastAPI endpoints (`/v1/logs`, `/v1/nodes`) with built-in API key verification, rate limiting, and HMAC-signed webhook delivery.

---

## Quick Start

### 1. Configure environment variables
```bash
cp .env.example .env
# Edit .env and set real values for API_KEY_SALT and ADMIN_SECRET, e.g.:
python -c "import secrets; print(secrets.token_hex(32))"
```

### 2. Spin up the local development stack
```bash
docker compose up -d --build
```
This starts Zookeeper, Kafka, Elasticsearch, Kibana, Fluentd, the Go `log_pipeline`, `heartbeat_service`, `api_gateway`, `nginx`, and all four demo microservices.

### 3. Verify it's running
```bash
docker compose ps
curl http://localhost:8000/health
```

For a comprehensive walkthrough, follow the [Getting Started Guide](file:///home/ashlesh/my_works/Distributed-Log-System/getting_started.md).

---

## Documentation Index

* **Installation & Integration**: [getting_started.md](file:///home/ashlesh/my_works/Distributed-Log-System/getting_started.md)
* **Design & Infrastructure**: [docs/architecture.md](file:///home/ashlesh/my_works/Distributed-Log-System/docs/architecture.md)
* **Security & Production Hardening**: [SECURITY.md](file:///home/ashlesh/my_works/Distributed-Log-System/SECURITY.md)
* **API Endpoints Reference**: [docs/api-reference.md](file:///home/ashlesh/my_works/Distributed-Log-System/docs/api-reference.md)
* **Open Source License**: [LICENSE](file:///home/ashlesh/my_works/Distributed-Log-System/LICENSE)

---

## Contributing

Please read [CONTRIBUTING.md](file:///home/ashlesh/my_works/Distributed-Log-System/CONTRIBUTING.md) before opening pull requests.

---

## License

Distributed under the MIT License. See [LICENSE](file:///home/ashlesh/my_works/Distributed-Log-System/LICENSE) for more details.
