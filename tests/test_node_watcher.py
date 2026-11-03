import asyncio
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from heartbeat_service import node_watcher


class FakeStatus:
    def __init__(self):
        self.calls = []

    async def upsert(self, node_id, service_name, status):
        self.calls.append((node_id, status))


class FakeWebhooks:
    def __init__(self):
        self.events = []

    async def notify(self, node_id, service_name, event):
        self.events.append((node_id, event))


class FakeRestarter:
    def __init__(self):
        self.restarted = []

    async def restart(self, node_id, service_name):
        self.restarted.append(node_id)


def make_watcher():
    # Replace the collaborators before construction so no real ES/Docker/HTTP
    # clients are created.
    node_watcher.NodeStatusManager = FakeStatus
    node_watcher.WebhookNotifier = FakeWebhooks
    node_watcher.RestartHandler = FakeRestarter
    return node_watcher.NodeWatcher()


async def settle():
    await asyncio.sleep(0)
    await asyncio.sleep(0)


async def expire(w, node_id):
    """Run the watch loop until the node is declared failed (two missed checks)."""
    w._last_seen[node_id] = w._last_seen[node_id].replace(year=2000)
    node_watcher.CHECK_INTERVAL = 0
    task = asyncio.create_task(w.watch_loop())
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()


def test_node_recovers_after_failure_and_node_up_fires():
    async def scenario():
        w = make_watcher()
        w.update("n1", "svc", "UP")
        await settle()
        assert w._ns.calls == [("n1", "active")]

        await expire(w, "n1")
        await settle()
        assert ("n1", "failed") in w._ns.calls
        assert w._restarter.restarted == ["n1"]

        w.update("n1", "svc", "UP")  # node came back
        await settle()
        assert w._ns.calls[-1] == ("n1", "active")
        assert ("n1", "node_up") in w._webhooks.events

    asyncio.run(scenario())


def test_suspected_node_that_returns_sends_no_node_up():
    async def scenario():
        w = make_watcher()
        w.update("n1", "svc", "UP")
        await settle()
        w._suspected.add("n1")
        w.update("n1", "svc", "UP")
        await settle()
        assert w._webhooks.events == []
        assert "n1" not in w._suspected

    asyncio.run(scenario())


def test_graceful_down_is_not_restarted_later():
    async def scenario():
        w = make_watcher()
        w.update("n1", "svc", "UP")
        await settle()
        w.update("n1", "svc", "DOWN")
        await settle()
        assert ("n1", "failed") in w._ns.calls
        assert ("n1", "node_down") in w._webhooks.events

        # No more heartbeats arrive. If the watcher still tracked the node, it
        # would age past the timeout and be declared failed (and restarted) again.
        if "n1" in w._last_seen:
            await expire(w, "n1")
        await settle()
        assert w._restarter.restarted == []
        assert w._webhooks.events.count(("n1", "node_down")) == 1

    asyncio.run(scenario())


def test_repeated_failure_cycles_keep_working():
    async def scenario():
        w = make_watcher()
        for _ in range(2):
            w.update("n1", "svc", "UP")
            await settle()
            await expire(w, "n1")
            await settle()
            w.update("n1", "svc", "UP")
            await settle()
        assert w._webhooks.events.count(("n1", "node_down")) == 2
        assert w._webhooks.events.count(("n1", "node_up")) == 2

    asyncio.run(scenario())
