import os

from elasticsearch import AsyncElasticsearch

_es: AsyncElasticsearch | None = None


def get_es() -> AsyncElasticsearch:
    global _es
    if _es is None:
        host = os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")
        _es = AsyncElasticsearch(host)
    return _es


async def close_es() -> None:
    global _es
    if _es:
        await _es.close()
        _es = None
