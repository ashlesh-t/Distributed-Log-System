from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, field_validator

# ── Logs ──────────────────────────────────────────────────────────────────────


class ErrorDetails(BaseModel):
    error_code: str
    error_message: str


class LogEntry(BaseModel):
    log_id: str
    node_id: str
    service_name: str
    log_level: str
    message: str
    timestamp: str
    error_details: ErrorDetails | None = None
    response_time_ms: int | None = None


class LogsResponse(BaseModel):
    total: int
    page: int
    page_size: int
    logs: list[LogEntry]


class LogStats(BaseModel):
    total_logs: int
    by_level: dict[str, int]
    by_service: dict[str, int]
    error_rate_pct: float


# ── Nodes ─────────────────────────────────────────────────────────────────────


class NodeInfo(BaseModel):
    node_id: str
    service_name: str
    status: str
    updated_at: str


class NodesResponse(BaseModel):
    nodes: list[NodeInfo]


class RestartResponse(BaseModel):
    triggered: bool
    backend: str


# ── Webhooks ──────────────────────────────────────────────────────────────────


class WebhookCreate(BaseModel):
    url: str
    events: list[Literal["node_down", "node_up", "error_spike"]]
    secret: str = ""

    @field_validator("url")
    @classmethod
    def url_must_be_https_or_http(cls, v: str) -> str:
        if not v.startswith(("http://", "https://")):
            raise ValueError("url must start with http:// or https://")
        return v


class WebhookInfo(BaseModel):
    webhook_id: str
    url: str
    events: list[str]


class WebhooksResponse(BaseModel):
    webhooks: list[WebhookInfo]


# ── Admin ─────────────────────────────────────────────────────────────────────


class AdminKeyCreate(BaseModel):
    owner_email: str


class AdminKeyResponse(BaseModel):
    api_key: str
    key_id: str
    owner_email: str
    note: str = "Store this key safely — it will not be shown again."


# ── Health ────────────────────────────────────────────────────────────────────


class HealthResponse(BaseModel):
    status: str
    elasticsearch: str
    version: str = "1.0.0"
