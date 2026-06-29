from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api_gateway.elastic_client import close_es, get_es
from api_gateway.rate_limit import RateLimitMiddleware
from api_gateway.routers import admin, logs, nodes, webhooks
from api_gateway.schemas import HealthResponse


@asynccontextmanager
async def lifespan(app: FastAPI):
    get_es()  # warm up connection
    yield
    await close_es()


app = FastAPI(
    title="Distributed Log System — PaaS API",
    description=(
        "Real-time log ingestion and node health monitoring as a service. "
        "Authenticate with `X-API-Key` header on all /v1/* endpoints."
    ),
    version="1.0.0",
    lifespan=lifespan,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "DELETE"],
    allow_headers=["X-API-Key", "X-Admin-Secret", "Content-Type"],
)
app.add_middleware(RateLimitMiddleware)

app.include_router(admin.router)
app.include_router(logs.router)
app.include_router(nodes.router)
app.include_router(webhooks.router)


@app.get("/health", response_model=HealthResponse, tags=["health"])
async def health():
    es = get_es()
    try:
        info = await es.cluster.health()
        es_status = "ok" if info["status"] != "red" else "degraded"
    except Exception:
        es_status = "error"
    return HealthResponse(status="ok", elasticsearch=es_status)
