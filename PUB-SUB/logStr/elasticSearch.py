from elasticsearch import AsyncElasticsearch
from elasticsearch.helpers import async_bulk
import uuid


class ElasticsearchLogStorage:
    def __init__(
        self, hosts=["http://localhost:9200"], index_prefix="distributed_logs"
    ):
        """
        Initialize asynchronous Elasticsearch connection with flexible configuration.
        """
        self.es_client = AsyncElasticsearch(hosts)
        self.index_prefix = index_prefix

    async def _create_index_template(self):
        """
        Create an asynchronous reusable index template for structured log storage.
        """
        template = {
            "index_patterns": [f"{self.index_prefix}-*"],
            "settings": {
                "number_of_shards": 3,
                "number_of_replicas": 1,
                "max_ngram_diff": 50,
            },
            "mappings": {
                "properties": {
                    "node_id": {"type": "keyword"},
                    "message_type": {"type": "keyword"},
                    "service_name": {"type": "keyword"},
                    "timestamp": {"type": "date"},
                    "log_id": {"type": "keyword"},
                    "log_level": {"type": "keyword"},
                    "message": {"type": "text"},
                    "response_time_ms": {"type": "integer"},
                    "threshold_limit_ms": {"type": "integer"},
                    "registration_details": {"type": "object", "enabled": True},
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
            # Create or update the template
            await self.es_client.indices.put_template(
                name=f"{self.index_prefix}_template", body=template
            )
            print(
                f"Index template {self.index_prefix}_template created/updated successfully."
            )
        except Exception as e:
            print(f"Error creating/updating index template: {e}")

    def _get_daily_index_name(self):
        """Generate daily index name."""
        return f"{self.index_prefix}"

    async def store_logs(self, logs):
        """
        Store logs in Elasticsearch using asynchronous bulk indexing.
        Supports multiple log types: Registration, INFO, WARN, ERROR.
        """
        try:
            # Ensure logs is a list
            if not isinstance(logs, list):
                logs = [logs]

            index_name = self._get_daily_index_name()

            # Ensure all required indices are created before bulk indexing
            unique_indices = set()
            for log in logs:
                current_index = (
                    f"{index_name}-{log['node_id']}" if "node_id" in log else index_name
                )
                unique_indices.add(current_index)

            # Create missing indices asynchronously
            for current_index in unique_indices:
                if not await self.es_client.indices.exists(index=current_index):
                    await self.es_client.indices.create(index=current_index)

            # Prepare logs for bulk indexing
            def log_generator():
                for log in logs:
                    current_index = (
                        f"{index_name}-{log['node_id']}"
                        if "node_id" in log
                        else index_name
                    )
                    if "log_id" not in log:
                        log["log_id"] = str(uuid.uuid4())
                    yield {
                        "_index": current_index,
                        "_id": log["log_id"],
                        "_source": log,
                    }

            # Asynchronous bulk index
            success, failed = await async_bulk(self.es_client, log_generator())
            print(f"Successfully indexed {success} logs, {failed} failed.")

        except Exception as e:
            print(f"Error in log indexing: {e}")

    async def close_connection(self):
        """
        Close the Elasticsearch connection gracefully.
        """
        await self.es_client.close()
