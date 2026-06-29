# Security Policy

## Reporting a Vulnerability

**Do not open a public GitHub Issue for security vulnerabilities.**

Email: ashleshat5@gmail.com  
Subject: `[SECURITY] Distributed Log System — <brief description>`

Include:
- Description of the vulnerability
- Steps to reproduce
- Potential impact
- Suggested fix (if any)

You will receive a response within 72 hours. We follow responsible disclosure — please allow 90 days for a fix before public disclosure.

---

## Security Model

### Decoupled Logging Security
- **No Direct Socket Access**: Microservices do not establish connections to Fluentd. This removes network access requirements and credential configurations from microservice namespaces.
- **Log Sanitation**: The log aggregation daemon (Fluent Bit) can be configured to strip out sensitive data (credit cards, passwords) using regex filters before writing logs to Kafka.

### API Authentication
- All `/v1/*` API endpoints require the `X-API-Key` header.
- API keys are prefixed with `dls_` followed by 32 URL-safe random characters.
- **API keys are stored as `SHA-256(salt + key)` — never in plaintext.**
- The salt is defined via `API_KEY_SALT`. Set a strong salt value in production.
- The plaintext key is displayed only once during creation.
- Keys can be instantly revoked using `DELETE /admin/keys/{key_id}`.

### Admin Endpoints Protection
- Sensitive operations (issuing/revoking keys) require the `X-Admin-Secret` header.
- NGINX restricts the `/admin/` paths to secure internal networks (CIDR lists like `10.x`, `172.16.x`, `192.168.x`, `127.0.0.1`).
- Set a strong `ADMIN_SECRET` value during initialization.

### Rate Limiting
- **API Gateway level**: Rate-limits are enforced per API Key (e.g., 100 requests/minute sliding window).
- **NGINX Proxy level**: Enforces IP-based rate limiting to prevent DDoS.
- Returns `429 Too Many Requests` on breach.

### Transport Security (TLS)
- NGINX redirects HTTP traffic to HTTPS (301 redirect).
- Supports TLS 1.2+ only, with weak cipher suites disabled.
- In production Kubernetes environments, deploy `cert-manager` for automatic Let's Encrypt certificates.

### Webhook Signatures
- Outbound alerts feature an optional HMAC-SHA256 signature header: `X-Signature-256: sha256=<hex_digest>`.
- Client listeners should verify signatures to guarantee payloads originate from the DLS control plane.

---

## Hardening Checklist for Production

- [ ] Change `API_KEY_SALT`, `ADMIN_SECRET`, and broker passwords from defaults.
- [ ] Set `xpack.security.enabled: true` in Elasticsearch configurations.
- [ ] Enable TLS between microservices, Fluent Bit, Kafka, and Elasticsearch.
- [ ] Remove `/var/run/docker.sock` mounts from container configs. Use Kubernetes Liveness Probes for restarts.
- [ ] Implement index lifecycle management (ILM) in Elasticsearch to automatically prune stale log indices.
- [ ] Block external public access to Elasticsearch port `9200` and Kibana port `5601` behind private subnets/VPNs.
- [ ] Use Kubernetes Secret resources or cloud-specific key vaults (AWS Secrets Manager, GCP Secret Manager) to manage salts and secrets.
