import os
import time
from collections import deque

from fastapi import Request, status
from fastapi.responses import JSONResponse
from starlette.middleware.base import BaseHTTPMiddleware

WINDOW_SECONDS = 60
# Per API key, and per client IP. The IP limit is what stops a client from
# bypassing the key limit by sending a different (fake) X-API-Key every request.
MAX_REQUESTS = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS", "100"))
MAX_REQUESTS_PER_IP = int(os.environ.get("RATE_LIMIT_MAX_REQUESTS_PER_IP", "300"))
SWEEP_INTERVAL_SECONDS = 60
EXEMPT_PATHS = {"/health"}  # keep container/K8s healthchecks from being throttled


def _client_ip(request: Request) -> str:
    # nginx sets X-Real-IP; without it every request would appear to come from
    # the proxy container and all clients would share one bucket.
    real_ip = request.headers.get("x-real-ip")
    if real_ip:
        return real_ip.strip()
    return request.client.host if request.client else "unknown"


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._windows: dict[str, deque] = {}
        self._last_sweep = time.time()

    def _sweep(self, now: float) -> None:
        """Drop buckets with no recent requests so unique keys can't grow memory forever."""
        if now - self._last_sweep < SWEEP_INTERVAL_SECONDS:
            return
        self._last_sweep = now
        cutoff = now - WINDOW_SECONDS
        for name in [n for n, w in self._windows.items() if not w or w[-1] < cutoff]:
            del self._windows[name]

    def _over_limit(self, bucket: str, limit: int, now: float) -> bool:
        window = self._windows.setdefault(bucket, deque())
        while window and window[0] < now - WINDOW_SECONDS:
            window.popleft()
        return len(window) >= limit

    async def dispatch(self, request: Request, call_next):
        if request.url.path in EXEMPT_PATHS:
            return await call_next(request)

        now = time.time()
        self._sweep(now)

        buckets = [(f"ip:{_client_ip(request)}", MAX_REQUESTS_PER_IP)]
        api_key = request.headers.get("x-api-key")
        if api_key:
            buckets.append((f"key:{api_key}", MAX_REQUESTS))

        if any(self._over_limit(name, limit, now) for name, limit in buckets):
            # Return a response rather than raising: HTTPException raised inside
            # BaseHTTPMiddleware bypasses FastAPI's handlers and becomes a 500.
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={
                    "detail": "Rate limit exceeded — "
                    f"{MAX_REQUESTS} requests per minute per API key"
                },
                headers={"Retry-After": str(WINDOW_SECONDS)},
            )

        for name, _ in buckets:
            self._windows[name].append(now)
        return await call_next(request)
