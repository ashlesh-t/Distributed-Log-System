import os
import uuid
from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk


class ElasticsearchLogStorage:
    def __init__(self):
        host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        self.es = AsyncElasticsearch(host)
        self.index_prefix = "distributed_logs"
        self._template_created = False

    async def ensure_template(self) -> None:
        if self._template_created:
            return
        template = {
            "index_patterns": [f"{self.index_prefix}-*"],
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1,
            },
            "mappings": {
                "properties": {
                    "node_id": {"type": "keyword"},
                    "message_type": {"type": "keyword"},
                    "service_name": {"type": "keyword"},
                    "api_key_hash": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "log_id": {"type": "keyword"},
                    "log_level": {"type": "keyword"},
                    "message": {"type": "text"},
                    "response_time_ms": {"type": "integer"},
                    "threshold_limit_ms": {"type": "integer"},
                    "error_details": {
                        "type": "object",
                        "properties": {
                            "error_code": {"type": "keyword"},
                            "error_message": {"type": "text"},
                        },
                    },
                }
            },
        }
        try:
            await self.es.indices.put_index_template(
                name=f"{self.index_prefix}_template",
                body={
                    "index_patterns": template["index_patterns"],
                    "template": {
                        "settings": template["settings"],
                        "mappings": template["mappings"],
                    },
                },
            )
            self._template_created = True
        except Exception as exc:
            print(f"[elastic] template creation warning: {exc}")

    async def store_logs(self, logs: list[dict]) -> None:
        await self.ensure_template()
        try:

            def gen():
                for log in logs:
                    if "log_id" not in log:
                        log["log_id"] = str(uuid.uuid4())
                    index = f"{self.index_prefix}-{log.get('node_id', 'unknown')}"
                    yield {"_index": index, "_id": log["log_id"], "_source": log}

            success, failed = await async_bulk(self.es, gen(), raise_on_error=False)
            print(f"[elastic] indexed {success}, failed {len(failed)}")
        except Exception as exc:
            print(f"[elastic] bulk index error: {exc}")

    async def close(self) -> None:
        await self.es.close()
