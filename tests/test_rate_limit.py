import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api_gateway import rate_limit


def make_client(monkeypatch, per_key=3, per_ip=5):
    monkeypatch.setattr(rate_limit, "MAX_REQUESTS", per_key)
    monkeypatch.setattr(rate_limit, "MAX_REQUESTS_PER_IP", per_ip)
    app = FastAPI()
    app.add_middleware(rate_limit.RateLimitMiddleware)

    @app.get("/ping")
    async def ping():
        return {"ok": True}

    @app.get("/health")
    async def health():
        return {"ok": True}

    return TestClient(app)


def test_returns_429_not_500_with_retry_after(monkeypatch):
    c = make_client(monkeypatch)
    codes = [c.get("/ping", headers={"X-API-Key": "k"}).status_code for _ in range(4)]
    assert codes == [200, 200, 200, 429]
    r = c.get("/ping", headers={"X-API-Key": "k"})
    assert r.status_code == 429
    assert r.headers["Retry-After"] == "60"


def test_rotating_fake_keys_cannot_bypass(monkeypatch):
    c = make_client(monkeypatch, per_key=3, per_ip=5)
    codes = [
        c.get("/ping", headers={"X-API-Key": f"fake{i}"}).status_code for i in range(7)
    ]
    assert codes[:5] == [200] * 5
    assert codes[5:] == [429, 429]


def test_limit_is_per_key(monkeypatch):
    c = make_client(monkeypatch, per_key=2, per_ip=100)
    for _ in range(2):
        assert c.get("/ping", headers={"X-API-Key": "a"}).status_code == 200
    assert c.get("/ping", headers={"X-API-Key": "a"}).status_code == 429
    assert c.get("/ping", headers={"X-API-Key": "b"}).status_code == 200


def test_x_real_ip_separates_clients(monkeypatch):
    c = make_client(monkeypatch, per_key=100, per_ip=2)
    for _ in range(2):
        assert c.get("/ping", headers={"X-Real-IP": "1.1.1.1"}).status_code == 200
    assert c.get("/ping", headers={"X-Real-IP": "1.1.1.1"}).status_code == 429
    assert c.get("/ping", headers={"X-Real-IP": "2.2.2.2"}).status_code == 200


def test_health_is_exempt(monkeypatch):
    c = make_client(monkeypatch, per_key=1, per_ip=1)
    assert all(c.get("/health").status_code == 200 for _ in range(10))


def test_idle_buckets_are_swept(monkeypatch):
    c = make_client(monkeypatch, per_key=100, per_ip=100)
    for i in range(50):
        c.get("/ping", headers={"X-API-Key": f"k{i}"})
    mw = c.app.middleware_stack  # built lazily on first request
    while not isinstance(mw, rate_limit.RateLimitMiddleware):
        mw = mw.app
    assert len(mw._windows) >= 50
    # age every bucket past the window and force a sweep
    for w in mw._windows.values():
        w.clear()
    mw._last_sweep = 0
    c.get("/ping", headers={"X-API-Key": "fresh"})
    assert len(mw._windows) <= 2  # fresh key + ip
