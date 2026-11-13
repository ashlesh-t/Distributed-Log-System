import asyncio
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from heartbeat_service import restart_handler
from heartbeat_service.restart_handler import RestartHandler


def fake_docker(monkeypatch, containers):
    client = MagicMock()
    client.containers.list.return_value = containers
    mod = MagicMock()
    mod.from_env.return_value = client
    monkeypatch.setitem(sys.modules, "docker", mod)
    return client


def test_resolves_compose_container_by_label(monkeypatch):
    c = MagicMock()
    c.name = "proj-order_service-1"
    client = fake_docker(monkeypatch, [c])
    ok = asyncio.run(RestartHandler().restart("n1", "Order_Service"))
    assert ok is True
    c.restart.assert_called_once()
    client.containers.list.assert_called_once_with(
        all=True, filters={"label": "com.docker.compose.service=order_service"}
    )


def test_returns_false_when_container_missing(monkeypatch):
    client = fake_docker(monkeypatch, [])
    client.containers.get.side_effect = RuntimeError("404")
    assert asyncio.run(RestartHandler().restart("n1", "Order_Service")) is False


def test_restart_runs_off_the_event_loop(monkeypatch):
    seen = {}
    monkeypatch.setattr(
        RestartHandler,
        "_docker_restart_blocking",
        staticmethod(lambda *_: seen.setdefault("loop_running", _in_loop()) or "x"),
    )

    def _in_loop():
        try:
            asyncio.get_running_loop()
            return True
        except RuntimeError:
            return False

    assert asyncio.run(RestartHandler().restart("n1", "S")) is True
    assert seen["loop_running"] is False
    assert restart_handler  # module imported
