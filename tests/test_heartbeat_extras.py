import asyncio
import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from elasticsearch import ConflictError, NotFoundError

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


def _hit(i, node):
    return {
        "_id": i,
        "_seq_no": 1,
        "_primary_term": 1,
        "_source": {"node_id": node, "service_name": "S"},
    }


def test_restart_worker_runs_pending_and_marks_status():
    es = MagicMock()
    es.search = AsyncMock(
        return_value={"hits": {"hits": [_hit("r1", "n1"), _hit("r2", "n2")]}}
    )
    es.update = AsyncMock()
    restarter = MagicMock()
    restarter.restart = AsyncMock(side_effect=[True, False])
    n = asyncio.run(RestartWorker(es=es, restarter=restarter).process_once())
    assert n == 2
    statuses = [c.kwargs["body"]["doc"]["status"] for c in es.update.await_args_list]
    assert statuses == ["running", "done", "running", "failed"]
    # claims are optimistic-concurrency guarded
    assert es.update.await_args_list[0].kwargs["if_seq_no"] == 1


def test_two_workers_cannot_both_execute_one_request():
    """Simulates two replicas racing for the same pending request."""
    state = {"claimed": False}

    async def update(**kw):
        if "if_seq_no" in kw:
            if state["claimed"]:
                raise ConflictError(409, "conflict", {})
            state["claimed"] = True

    def make():
        es = MagicMock()
        es.search = AsyncMock(return_value={"hits": {"hits": [_hit("r1", "n1")]}})
        es.update = AsyncMock(side_effect=update)
        r = MagicMock()
        r.restart = AsyncMock(return_value=True)
        return RestartWorker(es=es, restarter=r), r

    w1, r1 = make()
    w2, r2 = make()

    async def both():
        return await asyncio.gather(w1.process_once(), w2.process_once())

    results = asyncio.run(both())
    assert sorted(results) == [0, 1]
    assert r1.restart.await_count + r2.restart.await_count == 1


class SpikeES:
    """In-memory stand-in for the ES calls the detector makes."""

    def __init__(self, buckets):
        self.buckets = buckets
        self.state = {}  # node_id -> (doc, seq)
        self.searches = []

    async def search(self, **kw):
        self.searches.append(kw["body"])
        return {"aggregations": {"by_node": {"buckets": self.buckets}}}

    async def get(self, index, id):
        if id not in self.state:
            raise NotFoundError(404, "nf", {})
        doc, seq = self.state[id]
        return {"_source": doc, "_seq_no": seq, "_primary_term": 1}

    async def index(self, index, id, body, op_type=None, if_seq_no=None, **_):
        cur = self.state.get(id)
        if op_type == "create" and cur is not None:
            raise ConflictError(409, "c", {})
        if if_seq_no is not None and (cur is None or cur[1] != if_seq_no):
            raise ConflictError(409, "c", {})
        self.state[id] = (body, (cur[1] + 1) if cur else 0)


BUCKET = {"key": "n1", "doc_count": 50, "svc": {"buckets": [{"key": "Order"}]}}


def test_error_spike_alerts_once_per_cooldown():
    es = SpikeES([BUCKET])
    notifier = MagicMock()
    notifier.notify = AsyncMock()
    t = [1_000_000.0]
    d = ErrorSpikeDetector(es=es, notifier=notifier, clock=lambda: t[0])
    assert asyncio.run(d.check_once()) == ["n1"]
    assert asyncio.run(d.check_once()) == []  # cooldown (shared state in ES)
    t[0] += error_spike.COOLDOWN_SECONDS + 1
    assert asyncio.run(d.check_once()) == ["n1"]
    assert notifier.notify.await_args.args[2] == "error_spike"


def test_replicas_do_not_double_alert():
    es = SpikeES([BUCKET])  # shared ES == shared state
    notifiers = []
    dets = []
    for _ in range(2):
        n = MagicMock()
        n.notify = AsyncMock()
        notifiers.append(n)
        dets.append(ErrorSpikeDetector(es=es, notifier=n, clock=lambda: 1_000_000.0))

    async def both():
        return await asyncio.gather(*(d.check_once() for d in dets))

    asyncio.run(both())
    assert sum(n.notify.await_count for n in notifiers) == 1


def test_spike_query_uses_closed_concrete_window():
    es = SpikeES([])
    d = ErrorSpikeDetector(es=es, notifier=MagicMock(), clock=lambda: 1_000_000.0)
    asyncio.run(d.check_once())
    rng = es.searches[0]["query"]["bool"]["filter"][1]["range"]["timestamp"]
    assert "now" not in str(rng)  # concrete timestamps, not re-evaluated "now-60s"
    assert "gte" in rng and "lt" in rng  # half-open: later logs can't enter it
    start = datetime.fromisoformat(rng["gte"])
    end = datetime.fromisoformat(rng["lt"])
    assert (end - start).total_seconds() == error_spike.WINDOW_SECONDS
    # window ends LAG before "now", so ES has refreshed everything inside it
    now = datetime.fromtimestamp(1_000_000.0, timezone.utc)
    assert (now - end).total_seconds() == error_spike.LAG_SECONDS


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
