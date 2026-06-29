from fastapi import APIRouter, Depends, HTTPException

from api_gateway.auth import verify_api_key
from api_gateway.elastic_client import get_es
from api_gateway.schemas import NodeInfo, NodesResponse, RestartResponse
from heartbeat_service.restart_handler import RestartHandler
import os

router = APIRouter(prefix="/v1/nodes", tags=["nodes"])
_restarter = RestartHandler()


@router.get("", response_model=NodesResponse)
async def list_nodes(key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    resp = await es.search(index="node_status", body={"query": {"match_all": {}}, "size": 1000})
    nodes = [NodeInfo(**h["_source"]) for h in resp["hits"]["hits"]]
    return NodesResponse(nodes=nodes)


@router.get("/{node_id}", response_model=NodeInfo)
async def get_node(node_id: str, key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    try:
        doc = await es.get(index="node_status", id=node_id)
        return NodeInfo(**doc["_source"])
    except Exception:
        raise HTTPException(status_code=404, detail="Node not found")


@router.post("/{node_id}/restart", response_model=RestartResponse)
async def restart_node(node_id: str, key_meta: dict = Depends(verify_api_key)):
    backend = os.environ.get("RESTART_BACKEND", "docker")
    service_name = node_id  # fallback; real lookup via ES node_status
    es = get_es()
    try:
        doc = await es.get(index="node_status", id=node_id)
        service_name = doc["_source"].get("service_name", node_id)
    except Exception:
        pass
    await _restarter.restart(node_id, service_name)
    return RestartResponse(triggered=True, backend=backend)


@router.get("/{node_id}/heartbeats")
async def node_heartbeats(node_id: str, limit: int = 20, key_meta: dict = Depends(verify_api_key)):
    es = get_es()
    resp = await es.search(
        index="distributed_logs-*",
        body={
            "query": {"bool": {"filter": [{"term": {"node_id": node_id}}, {"term": {"message_type": "HEARTBEAT"}}]}},
            "sort": [{"timestamp": {"order": "desc"}}],
            "size": min(limit, 100),
        },
    )
    heartbeats = [h["_source"] for h in resp["hits"]["hits"]]
    return {"node_id": node_id, "heartbeats": heartbeats}
