from elasticsearch import AsyncElasticsearch
from datetime import datetime, timedelta, timezone


class NodeStatusManager:
    """
    A class to manage node status in Elasticsearch.
    """

    def __init__(
        self, es_host: str = "http://localhost:9200", index_name: str = "node_status"
    ):
        """
        Initialize the NodeStatusManager.

        Parameters:
            es_host (str): The Elasticsearch host URL.
            index_name (str): The Elasticsearch index name.
        """
        self.es = AsyncElasticsearch(es_host)
        self.index_name = index_name

    async def upsert_node_status(self, node_id: str, status: str):
        """
        Upsert (create or update) a document for a given node_id.

        Parameters:
            node_id (str): The ID of the node, used as the document ID.
            status (str): The status of the node (e.g., "active", "failed").
        """
        try:
            # Current timestamp
            IST = timezone(timedelta(hours=5, minutes=30))
            current_time = datetime.now(IST).isoformat()

            # Prepare the document
            update_doc = {
                "doc": {"status": status, "timestamp": current_time},
                "doc_as_upsert": True,
            }

            # Perform the upsert
            await self.es.update(index=self.index_name, id=node_id, body=update_doc)
            print(f"Upserted node {node_id} with status '{status}'.")
        except Exception as e:
            print(f"Error upserting node {node_id}: {e}")

    async def retrieve_node_status(self):
        """
        Retrieve all nodes from the Elasticsearch index.

        Returns:
            dict: A dictionary of all node data.
        """
        try:
            response = await self.es.search(
                index=self.index_name, body={"query": {"match_all": {}}}
            )
            nodes = {hit["_id"]: hit["_source"] for hit in response["hits"]["hits"]}
            return nodes
        except Exception as e:
            print(f"Error retrieving all nodes: {e}")
            return {}

    async def get_all_nodes(self):
        """
        Retrieve all nodes from the Elasticsearch index.

        Returns:
            dict: A dictionary of all node data.
        """
        try:
            response = await self.es.search(
                index=self.index_name, body={"query": {"match_all": {}}}
            )
            nodes = {hit["_id"]: hit["_source"] for hit in response["hits"]["hits"]}
            return nodes
        except Exception as e:
            print(f"Error retrieving all nodes: {e}")
            return {}

    async def close(self):
        """
        Close the Elasticsearch client.
        """
        await self.es.close()

    async def delete_node_status(self, node_id: str):
        """
        Delete a node's status from Elasticsearch.

        Parameters:
            node_id (str): The ID of the node to be deleted.
        """
        try:
            # Check if the document exists before attempting to delete
            exists = await self.es.exists(index=self.index_name, id=node_id)
            if exists:
                await self.es.delete(index=self.index_name, id=node_id)
                print(f"Node {node_id} successfully deleted from Elasticsearch.")
            else:
                print(f"Node {node_id} does not exist in Elasticsearch.")
        except Exception as e:
            print(f"Error deleting node {node_id}: {e}")


# Usage Example
# async def main():
#     manager = NodeStatusManager()
#     await manager.upsert_node_status("node_1", "active")
#     await manager.retrieve_node_status("node_1")
#     nodes = await manager.get_all_nodes()
#     print(nodes)
#     await manager.close()

# asyncio.run(main())
