import os
from datetime import datetime, timezone

from elasticsearch import AsyncElasticsearch

INDEX = "node_status"


class NodeStatusManager:
    def __init__(self):
        host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.es = AsyncElasticsearch(host)

    async def upsert(self, node_id: str, service_name: str, status: str) -> None:
        doc = {
            "node_id": node_id,
            "service_name": service_name,
            "status": status,
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            await self.es.update(
                index=INDEX,
                id=node_id,
                body={"doc": doc, "doc_as_upsert": True},
            )
        except Exception as exc:
            print(f"[node_status] upsert error: {exc}")

    async def get_all(self) -> list[dict]:
        try:
            resp = await self.es.search(
                index=INDEX,
                body={"query": {"match_all": {}}, "size": 1000},
            )
            return [hit["_source"] for hit in resp["hits"]["hits"]]
        except Exception:
            return []

    async def delete(self, node_id: str) -> None:
        try:
            await self.es.delete(index=INDEX, id=node_id)
        except Exception as exc:
            if "404" not in str(exc):
                print(f"[node_status] delete error: {exc}")

    async def close(self) -> None:
        await self.es.close()
