# API Reference — Distributed Log System PaaS

**Base URL:** `https://your-host` (production) | `https://localhost` (local, via nginx; self-signed cert, use `curl -k`)  
**Interactive docs:** `https://localhost/docs` (Swagger UI) | `https://localhost/redoc` (ReDoc)

---

## Authentication

All `/v1/*` endpoints require:
```
X-API-Key: dls_<your_key>
```

Admin endpoints require:
```
X-Admin-Secret: <your_admin_secret>
```

---

## Admin

### Issue API Key
```
POST /admin/keys
Header: X-Admin-Secret: <secret>
Body:   {"owner_email": "user@example.com"}
```
**Response 201:**
```json
{
  "api_key": "dls_xF3kP...",
  "key_id": "a1b2c3d4...",
  "owner_email": "user@example.com",
  "note": "Store this key safely — it will not be shown again."
}
```

### Revoke API Key
```
DELETE /admin/keys/{key_id}
Header: X-Admin-Secret: <secret>
```
**Response 204:** No content

---

## Logs

### Query Logs
```
GET /v1/logs
Header: X-API-Key: dls_<key>
Query params:
  service_name  string   Filter by service name
  log_level     string   INFO | WARN | ERROR
  node_id       string   Filter by node ID
  from_ts       string   ISO8601 start timestamp
  to_ts         string   ISO8601 end timestamp
  page          int      Page number (default: 1)
  page_size     int      Results per page (default: 50, max: 500)
```
**Response 200:**
```json
{
  "total": 1234,
  "page": 1,
  "page_size": 50,
  "logs": [
    {
      "log_id": "uuid",
      "node_id": "uuid",
      "service_name": "Inventory_Service",
      "log_level": "ERROR",
      "message": "Stock update failed",
      "timestamp": "2026-01-01T12:00:00Z",
      "error_details": {
        "error_code": "INV001",
        "error_message": "Stock update failed"
      },
      "response_time_ms": null
    }
  ]
}
```

### Get Single Log
```
GET /v1/logs/{log_id}
Header: X-API-Key: dls_<key>
```
**Response 200:** Single log object (same shape as above)  
**Response 404:** `{"detail": "Log not found"}`

### Log Statistics
```
GET /v1/logs/stats
Header: X-API-Key: dls_<key>
Query params:
  from_ts       string   ISO8601 start timestamp
  to_ts         string   ISO8601 end timestamp
  service_name  string   Optional service filter
```
**Response 200:**
```json
{
  "total_logs": 5000,
  "by_level": {"INFO": 3750, "WARN": 750, "ERROR": 500},
  "by_service": {"Inventory_Service": 1200, "Order_Service": 1100},
  "error_rate_pct": 10.0
}
```

---

## Nodes

### List All Nodes
```
GET /v1/nodes
Header: X-API-Key: dls_<key>
```
**Response 200:**
```json
{
  "nodes": [
    {
      "node_id": "uuid",
      "service_name": "Inventory_Service",
      "status": "active",
      "updated_at": "2026-01-01T12:00:00Z"
    }
  ]
}
```
Status values: `active`, `inactive`, `failed`

### Get Node Detail
```
GET /v1/nodes/{node_id}
Header: X-API-Key: dls_<key>
```
**Response 200:** Single node object  
**Response 404:** `{"detail": "Node not found"}`

### Trigger Node Restart
```
POST /v1/nodes/{node_id}/restart
Header: X-API-Key: dls_<key>
```
**Response 200:**
```json
{"triggered": true, "backend": "docker"}
```
The system will restart the node's container (Docker) or pod (Kubernetes) immediately.

### Node Heartbeat History
```
GET /v1/nodes/{node_id}/heartbeats
Header: X-API-Key: dls_<key>
Query params:
  limit  int  Max heartbeats to return (default: 20, max: 100)
```
**Response 200:**
```json
{
  "node_id": "uuid",
  "heartbeats": [
    {"status": "UP", "timestamp": "2026-01-01T12:00:00Z", "service_name": "Inventory_Service"}
  ]
}
```

---

## Webhooks

### Register Webhook
```
POST /v1/webhooks
Header: X-API-Key: dls_<key>
Body:
{
  "url": "https://your-server.com/webhook",
  "events": ["node_down", "node_up"],
  "secret": "optional-hmac-secret"
}
```
Events: `node_down`, `node_up`, `error_spike`

**Response 201:**
```json
{
  "webhook_id": "a1b2c3d4...",
  "url": "https://your-server.com/webhook",
  "events": ["node_down", "node_up"]
}
```

### List Webhooks
```
GET /v1/webhooks
Header: X-API-Key: dls_<key>
```

### Delete Webhook
```
DELETE /v1/webhooks/{webhook_id}
Header: X-API-Key: dls_<key>
```
**Response 204:** No content

### Test Webhook
```
POST /v1/webhooks/{webhook_id}/test
Header: X-API-Key: dls_<key>
```
**Response 200:**
```json
{"sent": true, "status_code": 200}
```

---

## Webhook Payload Format

When a `node_down` event fires, your registered URL receives:
```json
{
  "event": "node_down",
  "node_id": "uuid",
  "service_name": "Inventory_Service",
  "timestamp": "2026-01-01T12:00:00Z"
}
```

If you registered a `secret`, verify the `X-Signature-256` header:
```python
import hmac, hashlib, json

def verify_signature(body: bytes, signature: str, secret: str) -> bool:
    expected = "sha256=" + hmac.new(
        secret.encode(), body, hashlib.sha256
    ).hexdigest()
    return hmac.compare_digest(expected, signature)
```

---

## Health Check
```
GET /health
```
No authentication required.
```json
{"status": "ok", "elasticsearch": "ok", "version": "1.0.0"}
```

---

## Error Responses

| Status | Meaning |
|--------|---------|
| 400 | Bad request — invalid parameters |
| 401 | Missing or invalid `X-API-Key` |
| 403 | Missing or invalid `X-Admin-Secret` |
| 404 | Resource not found |
| 422 | Validation error (malformed request body) |
| 429 | Rate limit exceeded — wait and retry |
| 500 | Internal server error |

All errors return:
```json
{"detail": "Human-readable error message"}
```
