import asyncio
import os
import sys
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common import secret_box
from heartbeat_service import error_spike, node_watcher, webhook_notifier
from heartbeat_service.error_spike import ErrorSpikeDetector
from heartbeat_service.restart_worker import RestartWorker
from heartbeat_service.webhook_notifier import WebhookNotifier


def test_secret_roundtrip_and_legacy_plaintext():
    enc = secret_box.encrypt("topsecret")
    assert enc.startswith("enc:") and "topsecret" not in enc
    assert secret_box.decrypt(enc) == "topsecret"
    assert secret_box.decrypt("legacy") == "legacy"
    assert secret_box.encrypt("") == ""


def test_secret_wrong_key_does_not_decrypt(monkeypatch):
    enc = secret_box.encrypt("x")
    monkeypatch.setenv("WEBHOOK_ENCRYPTION_KEY", "other")
    assert secret_box.decrypt(enc) == ""


def _notifier_with(hooks, monkeypatch):
    es = MagicMock()
    es.search = AsyncMock(
        return_value={"hits": {"hits": [{"_source": h} for h in hooks]}}
    )
    es.close = AsyncMock()
    import elasticsearch

    monkeypatch.setattr(elasticsearch, "AsyncElasticsearch", lambda *_: es)
    n = WebhookNotifier()
    n._post = AsyncMock()
    return n


def test_notifier_only_fires_webhooks_owner_may_see(monkeypatch):
    monkeypatch.setenv("WEBHOOK_ALLOW_PRIVATE_TARGETS", "true")
    hooks = [
        {"url": "https://93.184.216.34/a", "owner_node_ids": ["n1"]},
        {"url": "https://93.184.216.34/b", "owner_node_ids": ["other"]},
        {"url": "https://93.184.216.34/c", "owner_node_ids": []},
    ]
    n = _notifier_with(hooks, monkeypatch)
    asyncio.run(n.notify("n1", "Svc", "node_down"))
    urls = [c.args[0] for c in n._post.await_args_list]
    assert urls == ["https://93.184.216.34/a", "https://93.184.216.34/c"]


def test_notifier_skips_unsafe_stored_target(monkeypatch):
    n = _notifier_with([{"url": "http://169.254.169.254/x"}], monkeypatch)
    asyncio.run(n.notify("n1", "Svc", "node_down"))
    n._post.assert_not_called()
    assert webhook_notifier  # module under test imported


def test_restart_worker_runs_pending_and_marks_status():
    es = MagicMock()
    es.search = AsyncMock(
        return_value={
            "hits": {
                "hits": [
                    {"_id": "r1", "_source": {"node_id": "n1", "service_name": "S"}},
                    {"_id": "r2", "_source": {"node_id": "n2", "service_name": "T"}},
                ]
            }
        }
    )
    es.update = AsyncMock()
    restarter = MagicMock()
    restarter.restart = AsyncMock(side_effect=[True, False])
    n = asyncio.run(RestartWorker(es=es, restarter=restarter).process_once())
    assert n == 2
    statuses = [c.kwargs["body"]["doc"]["status"] for c in es.update.await_args_list]
    assert statuses == ["running", "done", "running", "failed"]


def test_error_spike_alerts_once_per_cooldown():
    es = MagicMock()
    es.search = AsyncMock(
        return_value={
            "aggregations": {
                "by_node": {
                    "buckets": [
                        {
                            "key": "n1",
                            "doc_count": 50,
                            "svc": {"buckets": [{"key": "Order"}]},
                        }
                    ]
                }
            }
        }
    )
    notifier = MagicMock()
    notifier.notify = AsyncMock()
    t = [1000.0]
    d = ErrorSpikeDetector(es=es, notifier=notifier, clock=lambda: t[0])
    assert asyncio.run(d.check_once()) == ["n1"]
    assert asyncio.run(d.check_once()) == []  # cooldown
    t[0] += error_spike.COOLDOWN_SECONDS + 1
    assert asyncio.run(d.check_once()) == ["n1"]
    assert notifier.notify.await_args.args[2] == "error_spike"


def test_heartbeats_are_sampled_not_stored_every_beat():
    class NS:
        def __init__(self):
            self.n = 0

        async def upsert(self, *_):
            pass

        async def record_heartbeat(self, *_):
            self.n += 1

    node_watcher.NodeStatusManager = NS
    node_watcher.WebhookNotifier = MagicMock
    node_watcher.RestartHandler = MagicMock

    async def scenario():
        w = node_watcher.NodeWatcher()
        for _ in range(5):
            w.update("n1", "S", "HEARTBEAT")
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        return w._ns.n

    assert asyncio.run(scenario()) == 1
