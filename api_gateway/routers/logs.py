from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Query

from api_gateway.auth import verify_api_key
from api_gateway.elastic_client import get_es
from api_gateway.schemas import LogEntry, LogsResponse, LogStats

MAX_RESULT_WINDOW = 10000  # Elasticsearch index.max_result_window default

router = APIRouter(prefix="/v1/logs", tags=["logs"])


def _tenant_filter(key_meta: dict) -> list[dict]:
    node_ids = key_meta.get("node_ids", [])
    if node_ids:
        return [{"terms": {"node_id": node_ids}}]
    return []


@router.get("", response_model=LogsResponse)
async def query_logs(
    service_name: str | None = Query(None),
    log_level: str | None = Query(None),
    node_id: str | None = Query(None),
    from_ts: datetime | None = Query(None),
    to_ts: datetime | None = Query(None),
    page: int = Query(1, ge=1),
    page_size: int = Query(50, ge=1, le=500),
    key_meta: dict = Depends(verify_api_key),
):
    must = _tenant_filter(key_meta)
    if service_name:
        must.append({"term": {"service_name": service_name}})
    if log_level:
        must.append({"term": {"log_level": log_level.upper()}})
    if node_id:
        must.append({"term": {"node_id": node_id}})
    if from_ts or to_ts:
        rng: dict = {}
        if from_ts:
            rng["gte"] = from_ts.isoformat()
        if to_ts:
            rng["lte"] = to_ts.isoformat()
        must.append({"range": {"timestamp": rng}})

    if page * page_size > MAX_RESULT_WINDOW:
        raise HTTPException(
            status_code=422,
            detail=f"page * page_size must not exceed {MAX_RESULT_WINDOW}",
        )
    body = {
        "query": {"bool": {"filter": must}},
        "sort": [{"timestamp": {"order": "desc"}}],
        "from": (page - 1) * page_size,
        "size": page_size,
    }
    es = get_es()
    resp = await es.search(index="distributed_logs-*", body=body)
    hits = resp["hits"]["hits"]
    total = resp["hits"]["total"]["value"]
    logs = [LogEntry(**h["_source"]) for h in hits if "node_id" in h["_source"]]
    return LogsResponse(total=total, page=page, page_size=page_size, logs=logs)


@router.get("/stats", response_model=LogStats)
async def log_stats(
    from_ts: datetime | None = Query(None),
    to_ts: datetime | None = Query(None),
    service_name: str | None = Query(None),
    key_meta: dict = Depends(verify_api_key),
):
    must = _tenant_filter(key_meta)
    if service_name:
        must.append({"term": {"service_name": service_name}})
    if from_ts or to_ts:
        rng: dict = {}
        if from_ts:
            rng["gte"] = from_ts.isoformat()
        if to_ts:
            rng["lte"] = to_ts.isoformat()
        must.append({"range": {"timestamp": rng}})

    body = {
        "query": {"bool": {"filter": must}},
        "size": 0,
        "aggs": {
            "by_level": {"terms": {"field": "log_level"}},
            "by_service": {"terms": {"field": "service_name"}},
        },
    }
    es = get_es()
    resp = await es.search(index="distributed_logs-*", body=body)
    total = resp["hits"]["total"]["value"]
    by_level = {
        b["key"]: b["doc_count"] for b in resp["aggregations"]["by_level"]["buckets"]
    }
    by_service = {
        b["key"]: b["doc_count"] for b in resp["aggregations"]["by_service"]["buckets"]
    }
    error_count = by_level.get("ERROR", 0)
    error_rate = round((error_count / total * 100), 2) if total else 0.0
    return LogStats(
        total_logs=total,
        by_level=by_level,
        by_service=by_service,
        error_rate_pct=error_rate,
    )


@router.get("/{log_id}", response_model=LogEntry)
async def get_log(log_id: str, key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    resp = await es.search(
        index="distributed_logs-*",
        body={"query": {"term": {"log_id": log_id}}, "size": 1},
    )
    hits = resp["hits"]["hits"]
    if not hits:
        raise HTTPException(status_code=404, detail="Log not found")
    return LogEntry(**hits[0]["_source"])
