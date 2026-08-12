# Ingestion Pipeline in Go (`log_pipeline`)

To support high-throughput log ingestion and horizontal scalability, the core pipeline consumer (`log_pipeline`) has been rewritten in **Go**.

---

## 1. Why Go?

1. **Native Concurrency**: Go's native goroutine concurrency model enables concurrent consumption from partitioned Kafka topics without thread synchronization overhead.
2. **Resource Efficiency**: The Go binary has a negligible memory footprint (~10MB–20MB RAM) compared to the standard Python container (~150MB RAM).
3. **No GIL**: Eliminating Python's Global Interpreter Lock (GIL) allows the daemon to parse high-frequency JSON strings and batch-write to Elasticsearch using all available CPU cores.
4. **Fast Compilation & Tiny Containers**: Multi-stage docker builds compile the application into a static binary, executing inside a tiny `alpine` container with minimal dependencies.

---

## 2. Ingestion Design & Flow

```
                  ┌───────────────┐
                  │ Kafka Topics  │
                  └───────┬───────┘
                          │
            ┌─────────────┴─────────────┐ (Concurrent Readers)
            ▼                           ▼
    [Topic: logs]              [Topic: heartbeat]
            │                           │
            ▼                           ▼
    LogConsumer                 HeartbeatConsumer
   (Memory Buffering)        (Immediate ES Upsert)
            │                           │
            ▼                           ▼
    BulkStoreLogs()             UpsertNodeStatus()
            │                           │
            └─────────────┬─────────────┘
                          ▼
             Elasticsearch / OpenSearch
```

### Log Aggregator (`consumer_logs.go`)
* Consumes messages from the Kafka `logs` topic.
* Accumulates logs in a thread-safe, in-memory slice buffer.
* Flushes logs to Elasticsearch using the Bulk API when:
  1. The slice length matches `LOG_BUFFER_SIZE` (default: `10`).
  2. The flush timer fires (every `1` second) to ensure low-latency visibility for sparse log volumes.
* Commits Kafka offsets only after successful Elasticsearch responses to guarantee at-least-once delivery.

### Heartbeat Drain (`consumer_heartbeat.go`)
* Consumes from the Kafka `heartbeat` topic and commits offsets so the topic doesn't build up a backlog.
* Does **not** write to the `node_status` index — that's owned exclusively by `heartbeat_service`, which has the timeout-based failure detection logic needed to decide `active` vs. `failed`. Having both services independently upsert the same document caused the status to flap between writers.

---

## 3. Configuration Properties

The Go service reads the following environment variables:

| Variable Name | Description | Default Value |
| :--- | :--- | :--- |
| `KAFKA_BOOTSTRAP_SERVERS` | Comma-separated list of Kafka broker endpoints. | `kafka:9092` |
| `ELASTICSEARCH_HOST` | Elasticsearch cluster endpoint URL. | `http://elasticsearch:9200` |
| `LOG_BUFFER_SIZE` | Max number of log records to buffer before triggering a bulk flush. | `10` |

---

## 4. Local Execution & Diagnostics

### Run Locally
```bash
cd log_pipeline
go run .
```

### Build Binary
```bash
cd log_pipeline
go build -o log_pipeline .
```
