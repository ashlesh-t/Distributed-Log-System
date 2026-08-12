# Contributing to Distributed Log System

Thank you for your interest in contributing!

## Development Setup

```bash
git clone https://github.com/ASHLESHA05/Distributed-Log-System
cd Distributed-Log-System

# Install dev tools
pip install -r requirements.txt   # pylint, ruff, pytest, bandit, black

# Start infrastructure only (skip microservices for faster iteration)
docker compose up -d zookeeper kafka elasticsearch
```

## Project Structure

| Directory | Purpose |
|-----------|---------|
| `common/` | Shared library — config, log sender, heartbeat publisher, node ID |
| `services/{name}/` | Individual microservices (import from `common/`) |
| `log_pipeline/` | Kafka→Elasticsearch consumer pipeline |
| `heartbeat_service/` | Kafka consumer, failure detection, restart, webhooks |
| `api_gateway/` | FastAPI PaaS REST API |
| `nginx/` | Reverse proxy config + Dockerfile |
| `k8s/` | Kubernetes manifests (Kustomize base + overlays) |
| `demo.py` | Streamlit 5-tab control panel |

## Code Style

- **Python 3.11+** — use native type hints (`list[str]`, `dict | None`)
- **Formatter:** `black .`
- **Linter:** `ruff check .`
- **No comments explaining what the code does** — only comments explaining *why* (hidden constraints, non-obvious decisions)
- No `asyncio==x.y.z` in requirements — it's a built-in

## Running Tests

```bash
pytest tests/ -v
```

## Running Security Scan

```bash
bandit -r . -ll --exclude .venv,k8s
```

## Pull Request Process

1. Fork the repo and create a feature branch: `git checkout -b feat/your-feature`
2. Make your changes — keep PRs focused (one concern per PR)
3. Run `black .`, `ruff check .`, and `pytest` before pushing
4. Open a PR against `main` — fill in the PR template
5. All CI checks must pass before merge

## Adding a New Microservice

1. Create `services/<name>/service.py` — import from `common.*`
2. Add `services/<name>/Dockerfile` and `requirements.txt`
3. Add service to `docker-compose.yml`
4. Add `k8s/base/services.yaml` Deployment entry
5. Reuse `common/heartbeat.py` for heartbeat publishing — do **not** re-implement heartbeat logic

## Reporting Bugs

Open a GitHub Issue with:
- Steps to reproduce
- Expected vs actual behaviour
- Docker Compose version, OS, Python version

## Security Issues

See [SECURITY.md](SECURITY.md) — do **not** open a public GitHub Issue for security vulnerabilities.
