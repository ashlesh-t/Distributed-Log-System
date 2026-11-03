import asyncio
import signal


async def run_until_shutdown(*coros) -> None:
    """Run coroutines concurrently until SIGTERM/SIGINT (or one of them fails).

    The services run as PID 1 in their containers; the kernel ignores SIGTERM for
    PID 1 unless a handler is installed, so without this ``docker stop`` waits the
    full grace period and SIGKILLs the process before it can announce shutdown.
    All tasks are cancelled and awaited before returning, so callers can run
    their cleanup (e.g. publishing a CLOSE_LOG heartbeat) in a ``finally`` block.
    """
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, stop.set)
        except NotImplementedError:  # e.g. Windows event loops
            pass

    tasks = [asyncio.create_task(c) for c in coros]
    waiter = asyncio.create_task(stop.wait())
    try:
        done, _ = await asyncio.wait(
            [*tasks, waiter], return_when=asyncio.FIRST_COMPLETED
        )
    finally:
        waiter.cancel()
        for t in tasks:
            t.cancel()
        await asyncio.gather(waiter, *tasks, return_exceptions=True)

    # Surface a crash in one of the tasks instead of swallowing it.
    for t in done:
        if t is not waiter and not t.cancelled() and t.exception():
            raise t.exception()
