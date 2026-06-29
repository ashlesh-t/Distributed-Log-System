import time
from collections import defaultdict, deque

from fastapi import Request, HTTPException, status
from starlette.middleware.base import BaseHTTPMiddleware

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
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail="Rate limit exceeded — 100 requests per minute per API key",
                headers={"Retry-After": str(WINDOW_SECONDS)},
            )

        window.append(now)
        return await call_next(request)
