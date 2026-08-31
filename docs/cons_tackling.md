# Tackling Project Cons & Drawbacks

This document outlines the known architectural and security drawbacks of the Distributed Log System (DLS) and provides actionable strategies to resolve them.

---

## 1. The Docker Socket Mount (Critical Security Risk)
**Issue:** The `heartbeat_service` mounts `/var/run/docker.sock` to restart failed containers. If compromised, an attacker gains root access to the host machine.
**Resolution:**
*   **Kubernetes Native:** In K8s environments, remove the docker socket entirely. Instead, use a Service Account with a restricted RBAC Role allowing only `pods/delete` or `deployments/restart` within specific namespaces.
*   **API-Driven Restarts:** For Docker environments, instead of mounting the socket, expose a highly secured, internal-only daemon (e.g., via a secured TCP socket with mTLS) whose sole job is to receive restart commands and execute them.
*   **Native Probes:** Rely on Kubernetes Liveness/Readiness probes or Docker Compose `healthcheck` auto-healing (via tools like autoheal) rather than building custom restart logic into the heartbeat service.

## 2. Single Points of Failure (SPOF)
**Issue:** Kafka, Zookeeper, and Elasticsearch are configured as single nodes in the `docker-compose.yml`.
**Resolution:**
*   Provide a `docker-compose.prod.yml` that configures a minimum 3-node Zookeeper ensemble, 3-broker Kafka cluster, and a 3-node Elasticsearch cluster.
*   Ensure Kafka topics (`logs`, `heartbeat`) are created with a replication factor of at least 2 or 3 in production scripts.

## 3. Internal Security Disabled by Default
**Issue:** Elasticsearch is running with `xpack.security.enabled: "false"`.
**Resolution:**
*   Enable `xpack.security.enabled: true` in the production configuration.
*   Provide an initialization script that automatically generates the `elastic` built-in user passwords and injects them into a `.env` file or Docker secrets.
*   Configure the Go `log_pipeline` and FastAPI `api_gateway` to authenticate with Elasticsearch using these credentials.

## 4. Resource Intensive Local Stack
**Issue:** Running the entire stack (Kafka, ES, Kibana, Zookeeper, APIs) locally consumes significant RAM/CPU.
**Resolution:**
*   **Lightweight Profiles:** Create Docker Compose profiles (e.g., `--profile lightweight`). Swap Elasticsearch/Kibana for a lighter alternative like SQLite/PostgreSQL for basic testing, or use Redpanda (which removes the Zookeeper dependency) instead of Kafka+Zookeeper.
*   **Cloud Dev Environments:** Provide instructions for deploying the dev stack to a cheap cloud VM or using remote Kubernetes clusters (like minikube/k3d with resource limits).

## 5. Dropped Logs on Bad JSON
**Issue:** In the Go `log_pipeline` (`consumer_logs.go`), unparseable JSON messages are logged and the Kafka offset is committed, resulting in permanent data loss for those messages.
**Resolution:**
*   **Implement a Dead Letter Queue (DLQ):** Create a new Kafka topic named `logs-dlq`. When `json.Unmarshal` fails, publish the raw byte payload to the DLQ topic before committing the offset.
*   This allows administrators to inspect malformed logs later without halting the main ingestion pipeline.
