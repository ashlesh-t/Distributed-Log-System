import os
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

import pytest
from elasticsearch import NotFoundError
from fastapi.testclient import TestClient

from api_gateway import auth, main
from api_gateway.routers import logs, nodes, webhooks
from common.url_safety import UnsafeURLError, assert_public_url


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
        "http://93.184.216.34/",
        "http://10.0.0.5/",
        "ftp://example.com/",
        "http://[::1]/",
    ],
)
def test_ssrf_urls_rejected(url):
    import asyncio

    with pytest.raises(UnsafeURLError):
        asyncio.run(assert_public_url(url))


def test_private_target_allowed_with_opt_in(monkeypatch):
    import asyncio

    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_TARGETS", "true")
    asyncio.run(assert_public_url("http://127.0.0.1:8080/hook"))
    # metadata endpoint stays blocked even with the opt-in
    with pytest.raises(UnsafeURLError):
        asyncio.run(assert_public_url("http://169.254.169.254/"))


def test_public_http_allowed_only_with_opt_in(monkeypatch):
    import asyncio

    monkeypatch.setenv("WEBHOOK_ALLOW_HTTP", "true")
    asyncio.run(assert_public_url("http://93.184.216.34/"))


def test_webhook_store_outage_is_503_not_500(env):
    es, c = env
    es.get.side_effect = RuntimeError("down")
    assert c.delete("/v1/webhooks/w1").status_code == 503


def test_delete_race_is_404(env):
    es, c = env
    es.get.return_value = {"_source": {"url": "https://e.com", "owner_key_id": "A"}}
    es.delete.side_effect = not_found()
    assert c.delete("/v1/webhooks/w1").status_code == 404


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


# --- restart (queued, gated) -------------------------------------------
def as_key(app_client, **meta):
    main.app.dependency_overrides[auth.verify_api_key] = lambda: {"key_id": "A", **meta}


def test_restart_requires_can_restart(env):
    es, c = env
    assert c.post("/v1/nodes/n1/restart").status_code == 403
    es.index.assert_not_called()


def test_restart_unknown_node_is_404_and_not_queued(env):
    es, c = env
    as_key(c, can_restart=True)
    es.get.side_effect = not_found()
    assert c.post("/v1/nodes/does-not-exist/restart").status_code == 404
    es.index.assert_not_called()


def test_restart_other_tenants_node_is_404(env):
    es, c = env
    as_key(c, can_restart=True, node_ids=["mine"])
    assert c.post("/v1/nodes/theirs/restart").status_code == 404
    es.index.assert_not_called()


def test_restart_is_queued_not_executed_by_gateway(env):
    es, c = env
    as_key(c, can_restart=True)
    es.get.return_value = {"_source": {"service_name": "Order_Service"}}
    r = c.post("/v1/nodes/n1/restart")
    assert r.status_code == 202 and r.json()["triggered"] is True
    doc = es.index.await_args.kwargs["body"]
    assert doc["node_id"] == "n1" and doc["status"] == "pending"
    assert doc["service_name"] == "Order_Service"


def test_gateway_does_not_import_docker_restart_handler():
    assert not hasattr(nodes, "RestartHandler")


# --- tenant scoping --------------------------------------------------------
def test_scoped_key_cannot_read_other_node(env):
    es, c = env
    as_key(c, node_ids=["mine"])
    assert c.get("/v1/nodes/theirs").status_code == 404
    assert c.get("/v1/nodes/theirs/heartbeats").status_code == 404
    es.get.assert_not_called()


def test_scoped_key_node_list_filtered(env):
    es, c = env
    as_key(c, node_ids=["mine"])
    c.get("/v1/nodes")
    q = es.search.await_args.kwargs["body"]["query"]
    assert q == {"ids": {"values": ["mine"]}}


def test_get_log_applies_tenant_filter(env):
    es, c = env
    as_key(c, node_ids=["mine"])
    c.get("/v1/logs/abc")
    flt = es.search.await_args.kwargs["body"]["query"]["bool"]["filter"]
    assert {"terms": {"node_id": ["mine"]}} in flt


def test_heartbeats_read_from_heartbeat_index(env):
    es, c = env
    es.search.return_value = {
        "hits": {"hits": [{"_source": {"node_id": "n1", "timestamp": "t"}}]}
    }
    r = c.get("/v1/nodes/n1/heartbeats")
    assert r.json()["heartbeats"][0]["node_id"] == "n1"
    assert es.search.await_args.kwargs["index"] == "node_heartbeats"


def test_admin_key_stores_scope(env, monkeypatch):
    es, c = env
    monkeypatch.setattr(auth, "get_es", lambda: es)
    monkeypatch.setattr(auth, "ADMIN_SECRET", "s")
    r = c.post(
        "/admin/keys",
        headers={"X-Admin-Secret": "s"},
        json={"owner_email": "a@b.c", "node_ids": ["n1"], "can_restart": True},
    )
    assert r.status_code == 201
    body = es.index.await_args.kwargs["body"]
    assert body["node_ids"] == ["n1"] and body["can_restart"] is True


def test_webhook_secret_encrypted_and_scope_recorded(env, monkeypatch):
    es, c = env
    as_key(c, node_ids=["n1"])
    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_TARGETS", "true")
    r = c.post(
        "/v1/webhooks",
        json={
            "url": "https://93.184.216.34/h",
            "events": ["node_down"],
            "secret": "s3",
        },
    )
    assert r.status_code == 201
    doc = es.index.await_args.kwargs["body"]
    assert doc["secret"].startswith("enc:") and "s3" not in doc["secret"]
    assert doc["owner_node_ids"] == ["n1"]
