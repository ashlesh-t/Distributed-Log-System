import os
import secrets
from datetime import datetime, timezone

from elasticsearch import NotFoundError
from fastapi import APIRouter, Depends, HTTPException

from api_gateway.auth import node_allowed, verify_api_key
from api_gateway.elastic_client import get_es
from api_gateway.schemas import NodeInfo, NodesResponse, RestartResponse

router = APIRouter(prefix="/v1/nodes", tags=["nodes"])

RESTART_INDEX = "restart_requests"
HEARTBEAT_INDEX = "node_heartbeats"


@router.get("", response_model=NodesResponse)
async def list_nodes(key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    query: dict = {"match_all": {}}
    node_ids = key_meta.get("node_ids") or []
    if node_ids:
        query = {"ids": {"values": node_ids}}  # node_status doc id == node_id
    resp = await es.search(index="node_status", body={"query": query, "size": 1000})
    nodes = [NodeInfo(**h["_source"]) for h in resp["hits"]["hits"]]
    return NodesResponse(nodes=nodes)


@router.get("/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str, key_meta: dict = Depends(verify_api_key)):
    if not node_allowed(key_meta, node_id):
        raise HTTPException(status_code=404, detail="Node not found")
    es = get_es()
    try:
        doc = await es.get(index="node_status", id=node_id)
        return NodeInfo(**doc["_source"])
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Node not found")
    except Exception:
        raise HTTPException(status_code=503, detail="Node store unavailable")


@router.post("/{node_id}/restart", response_model=RestartResponse, status_code=202)
async def restart_node(node_id: str, key_meta: dict = Depends(verify_api_key)):
    """Queue a restart. The gateway holds no Docker/K8s credentials: the
    heartbeat_service (which does) picks the request up from Elasticsearch."""
    if not key_meta.get("can_restart"):
        raise HTTPException(status_code=403, detail="Key is not allowed to restart")
    if not node_allowed(key_meta, node_id):
        raise HTTPException(status_code=404, detail="Node not found")
    es = get_es()
    # Only known nodes can be restarted; node_id is never used as a raw
    # container name.
    try:
        doc = await es.get(index="node_status", id=node_id)
    except NotFoundError:
        raise HTTPException(status_code=404, detail="Node not found")
    except Exception:
        raise HTTPException(status_code=503, detail="Node store unavailable")
    service_name = doc["_source"].get("service_name")
    if not service_name:
        raise HTTPException(status_code=404, detail="Node has no service name")
    try:
        await es.index(
            index=RESTART_INDEX,
            id=secrets.token_hex(16),
            body={
                "node_id": node_id,
                "service_name": service_name,
                "requested_by": key_meta.get("key_id", ""),
                "status": "pending",
                "requested_at": datetime.now(timezone.utc).isoformat(),
            },
            refresh="wait_for",
        )
    except Exception:
        raise HTTPException(status_code=503, detail="Restart queue unavailable")
    return RestartResponse(
        triggered=True, backend=os.environ.get("RESTART_BACKEND", "docker")
    )


@router.get("/{node_id}/heartbeats")
async def node_heartbeats(
    node_id: str, limit: int = 20, key_meta: dict = Depends(verify_api_key)
):
    if not node_allowed(key_meta, node_id):
        raise HTTPException(status_code=404, detail="Node not found")
    es = get_es()
    resp = await es.search(
        index=HEARTBEAT_INDEX,
        ignore_unavailable=True,  # index is created on the first heartbeat
        body={
            "query": {"term": {"node_id": node_id}},
            "sort": [{"timestamp": {"order": "desc", "unmapped_type": "date"}}],
            "size": max(1, min(limit, 100)),
        },
    )
    heartbeats = [h["_source"] for h in resp["hits"]["hits"]]
    return {"node_id": node_id, "heartbeats": heartbeats}
