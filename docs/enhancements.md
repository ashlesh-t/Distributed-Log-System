# Future Enhancements & Features

This document outlines the roadmap for features and enhancements to improve the scalability, observability, and maintainability of the Distributed Log System (DLS).

---

## 1. Index Lifecycle Management (ILM)
Currently, logs are written to Elasticsearch indefinitely. Over time, this will exhaust disk space.
*   **Implementation:** Define an ILM policy in Elasticsearch during the `log_pipeline` startup (or via an init script).
*   **Policy Rules:** For example, automatically transition indices older than 7 days to a "warm" phase, and delete indices older than 30 days. 
*   **Dynamic Indices:** Ensure the Go pipeline writes to an alias that rolls over based on the ILM policy rather than daily hardcoded indices.

## 2. OpenTelemetry (OTLP) Integration
While the custom JSON stdout logging is decoupled and easy to use, adopting industry standards ensures future compatibility.
*   **Implementation:** Allow Fluentd/Fluent Bit to accept standard OTLP metrics and traces.
*   **Benefit:** Users can utilize standard OpenTelemetry SDKs in their microservices to generate distributed traces, which can be correlated with the logs in Elasticsearch.

## 3. Kubernetes Native Operator
Managing the lifecycle of the DLS stack on Kubernetes via raw manifests or Helm charts can be complex.
*   **Implementation:** Develop a Kubernetes Operator (using Kubebuilder or Operator SDK).
*   **Benefit:** The operator can automatically manage Kafka topic creation, Elasticsearch user provisioning, and automatic scaling of the `log_pipeline` pods based on Kafka lag.

## 4. Advanced Dead Letter Queue (DLQ) Dashboard
Once the DLQ is implemented in the Go pipeline, users need a way to interact with it.
*   **Implementation:** Add endpoints to the FastAPI `api_gateway` to read from the DLQ topic.
*   **UI Integration:** Update the Streamlit/Kibana dashboard to display a "Malformed Logs" section, allowing developers to see which services are emitting invalid JSON.

## 5. Schema Validation Registry
Currently, logs are loosely structured JSON. 
*   **Implementation:** Integrate a Schema Registry (like Confluent Schema Registry).
*   **Benefit:** Enforce strict log schemas (using Avro or Protobuf). If a microservice emits a log that breaks the schema, it gets rejected or sent to the DLQ early, ensuring data consistency in Elasticsearch.

## 6. Role-Based Access Control (RBAC) in API Gateway
Currently, API keys grant broad access to read logs.
*   **Implementation:** Introduce tenant-based or service-based RBAC.
*   **Benefit:** An API key could be restricted to only query logs where `service_name == "Inventory_Service"`, enabling multi-tenant isolation.
