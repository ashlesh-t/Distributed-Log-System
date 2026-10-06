import time
from collections import defaultdict, deque

from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

WINDOW_SECONDS = 60
MAX_REQUESTS = 100


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._windows: dict[str, deque] = defaultdict(deque)

    async def dispatch(self, request: Request, call_next):
        key = request.headers.get("x-api-key") or request.client.host
        now = time.time()
        window = self._windows[key]

        # Evict timestamps outside the window
        while window and window[0] < now - WINDOW_SECONDS:
            window.popleft()

        if len(window) >= MAX_REQUESTS:
            # NOTE: Must return JSONResponse here — raising HTTPException inside
            # BaseHTTPMiddleware bypasses FastAPI's exception handlers and causes
            # Starlette to return HTTP 500 instead of the intended 429.
            return JSONResponse(
                status_code=429,
                content={
                    "detail": "Rate limit exceeded — 100 requests per minute per API key"
                },
                headers={"Retry-After": str(WINDOW_SECONDS)},
            )

        window.append(now)
        return await call_next(request)
