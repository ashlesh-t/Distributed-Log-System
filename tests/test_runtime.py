import asyncio
import os
import signal
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from common.runtime import run_until_shutdown


def test_sigterm_stops_tasks_and_lets_caller_clean_up():
    cleaned = []

    async def forever():
        await asyncio.sleep(3600)

    async def scenario():
        asyncio.get_running_loop().call_later(
            0.05, os.kill, os.getpid(), signal.SIGTERM
        )
        try:
            await asyncio.wait_for(run_until_shutdown(forever(), forever()), 5)
        finally:
            cleaned.append(True)

    asyncio.run(scenario())
    assert cleaned == [True]


def test_task_crash_is_propagated():
    async def boom():
        raise RuntimeError("boom")

    async def forever():
        await asyncio.sleep(3600)

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(run_until_shutdown(boom(), forever()))
