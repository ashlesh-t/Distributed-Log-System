import os
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from elasticsearch import NotFoundError
from fastapi.testclient import TestClient

from api_gateway import auth, main
from api_gateway.routers import logs, nodes, webhooks
from api_gateway.url_safety import UnsafeURLError, assert_public_url


class FakeES:
    def __init__(self):
        self.get = AsyncMock()
        self.search = AsyncMock(
            return_value={"hits": {"hits": [], "total": {"value": 0}}}
        )
        self.index = AsyncMock()
        self.delete = AsyncMock()
        self.cluster = MagicMock()
        self.cluster.health = AsyncMock(return_value={"status": "green"})


def not_found():
    return NotFoundError(404, "not_found", {})


@pytest.fixture
def env(monkeypatch):
    es = FakeES()
    for mod in (auth, logs, nodes, webhooks, main):
        if hasattr(mod, "get_es"):
            monkeypatch.setattr(mod, "get_es", lambda es=es: es)
    app = main.app
    app.dependency_overrides[auth.verify_api_key] = lambda: {"key_id": "A"}
    yield es, TestClient(app)
    app.dependency_overrides.clear()


# --- #16 ---------------------------------------------------------------
def test_invalid_timestamp_is_422(env):
    _, c = env
    assert c.get("/v1/logs?from_ts=garbage").status_code == 422
    assert c.get("/v1/logs?to_ts=garbage").status_code == 422


def test_valid_timestamp_ok(env):
    _, c = env
    assert c.get("/v1/logs?from_ts=2026-01-01T00:00:00Z").status_code == 200


def test_deep_pagination_is_422(env):
    _, c = env
    assert c.get("/v1/logs?page=500&page_size=500").status_code == 422


def test_health_503_when_es_down(env):
    es, c = env
    es.cluster.health.side_effect = RuntimeError("down")
    r = c.get("/health")
    assert r.status_code == 503
    assert r.json()["status"] == "unhealthy"


def test_health_ok(env):
    _, c = env
    assert c.get("/health").status_code == 200


def test_es_outage_during_auth_is_503(monkeypatch):
    es = FakeES()
    es.search.side_effect = RuntimeError("down")
    monkeypatch.setattr(auth, "get_es", lambda: es)
    import asyncio
    from fastapi import HTTPException

    with pytest.raises(HTTPException) as e:
        asyncio.run(auth.verify_api_key("k"))
    assert e.value.status_code == 503


def test_bad_key_is_401(monkeypatch):
    import asyncio
    from fastapi import HTTPException

    es = FakeES()
    monkeypatch.setattr(auth, "get_es", lambda: es)
    with pytest.raises(HTTPException) as e:
        asyncio.run(auth.verify_api_key("k"))
    assert e.value.status_code == 401


def test_admin_delete_missing_key_is_404(env, monkeypatch):
    es, c = env
    es.delete.side_effect = not_found()
    monkeypatch.setattr(auth, "get_es", lambda: es)
    monkeypatch.setattr(auth, "ADMIN_SECRET", "s")
    r = c.delete("/admin/keys/nope", headers={"X-Admin-Secret": "s"})
    assert r.status_code == 404
    assert (
        c.delete("/admin/keys/nope", headers={"X-Admin-Secret": "x"}).status_code == 403
    )


# --- #15 ---------------------------------------------------------------
@pytest.mark.parametrize(
    "url",
    [
        "http://127.0.0.1/x",
        "http://localhost:9200/",
        "http://169.254.169.254/latest",
        "http://10.0.0.5/",
        "ftp://example.com/",
        "http://[::1]/",
    ],
)
def test_ssrf_urls_rejected(url):
    import asyncio

    with pytest.raises(UnsafeURLError):
        asyncio.run(assert_public_url(url))


def test_create_webhook_blocks_internal(env):
    _, c = env
    r = c.post(
        "/v1/webhooks",
        json={"url": "http://127.0.0.1:9200/", "events": ["node_down"]},
    )
    assert r.status_code == 422


def test_cannot_delete_or_test_other_tenants_webhook(env):
    es, c = env
    es.get.return_value = {
        "_source": {"url": "https://example.com", "owner_key_id": "B"}
    }
    assert c.delete("/v1/webhooks/w1").status_code == 404
    assert c.post("/v1/webhooks/w1/test").status_code == 404
    es.delete.assert_not_called()


def test_owner_can_delete_webhook(env):
    es, c = env
    es.get.return_value = {
        "_source": {"url": "https://example.com", "owner_key_id": "A"}
    }
    assert c.delete("/v1/webhooks/w1").status_code == 204
    es.delete.assert_awaited_once()


# --- #11 / #24 ---------------------------------------------------------
def test_restart_unknown_node_is_404_and_not_attempted(env, monkeypatch):
    es, c = env
    es.get.side_effect = not_found()
    restart = AsyncMock(return_value=True)
    monkeypatch.setattr(nodes._restarter, "restart", restart)
    assert c.post("/v1/nodes/does-not-exist/restart").status_code == 404
    restart.assert_not_called()


def test_restart_failure_is_502(env, monkeypatch):
    es, c = env
    es.get.return_value = {"_source": {"service_name": "Order_Service"}}
    monkeypatch.setattr(nodes._restarter, "restart", AsyncMock(return_value=False))
    assert c.post("/v1/nodes/n1/restart").status_code == 502


def test_restart_success(env, monkeypatch):
    es, c = env
    es.get.return_value = {"_source": {"service_name": "Order_Service"}}
    restart = AsyncMock(return_value=True)
    monkeypatch.setattr(nodes._restarter, "restart", restart)
    r = c.post("/v1/nodes/n1/restart")
    assert r.status_code == 200 and r.json()["triggered"] is True
    restart.assert_awaited_once_with("n1", "Order_Service")
