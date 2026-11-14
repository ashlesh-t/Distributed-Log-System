import os
from elasticsearch import AsyncElasticsearch

from common.constants import DEFAULT_ES_HOST, ENV_ES_HOST

_es: AsyncElasticsearch | None = None


def get_es() -> AsyncElasticsearch:
    global _es
    if _es is None:
        host = os.environ.get(ENV_ES_HOST, DEFAULT_ES_HOST)
        _es = AsyncElasticsearch(host)
    return _es


async def close_es() -> None:
    global _es
    if _es:
        await _es.close()
        _es = None
