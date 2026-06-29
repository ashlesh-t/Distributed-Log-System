# Distributed Log System (DLS) — PaaS Framework

> **A production-ready Platform as a Service (PaaS) framework for decoupled log aggregation, unified health monitoring, and alerting. Package-installable and fully configurable for local development or cloud clusters.**

---

## Architecture Overview

DLS eliminates client-side coupling by utilizing a **decoupled log shipping pattern**. Microservices publish structured JSON logs directly to standard output (`stdout`), where a low-overhead collector (Fluent Bit / Fluentd) tails, parses, and pushes them to **Kafka**. A ingestion pipeline then consumes the logs in bulk and writes them to **Elasticsearch**.

For detailed system diagrams and component logs, see [architecture.md](file:///home/ashlesh/my_works/Distributed-Log-System/docs/architecture.md).

---

## Key Upgrades in this PaaS Release

1. **Stdout Ingestion Routing**: Client applications are completely decoupled from Fluentd classes. Standard logging libraries write to stdout, making log aggregation non-blocking and robust.
2. **Unified Package Management**: Installable CLI client (`dls`) via Arch Linux AUR (`yay`) or macOS/Linux Homebrew (`brew`).
3. **Interactive Setup Wizard (`dls init`)**: Interactive prompt system to configure log topics dynamically, mapping database credentials and custom ingest routes.
4. **PaaS API Gateway**: Secure FastAPI endpoints (`/v1/logs`, `/v1/nodes`) with built-in API key verification, rate limiting, and HMAC-signed webhook delivery.

---

## Quick Start

### 1. Install the CLI Tool
```bash
# Via Homebrew
brew tap ashlesh/dls
brew install dls

# Via Yay
yay -S dls-cli
```

### 2. Run the Initialization Wizard
Configure your local environment and custom log topics interactively:
```bash
dls init
```

### 3. Spin up the Local Development Stack
```bash
# Starts local Docker instances for Zookeeper, Kafka, Elasticsearch, Fluent Bit, and the API Gateway
dls start --local
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
