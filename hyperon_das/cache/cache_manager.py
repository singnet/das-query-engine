from typing import Any, Dict, List, Optional, TypeVar, Union

from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB

from hyperon_das.client import FunctionsClient

AdapterDBType = TypeVar("AdapterDBType", RedisMongoDB, InMemoryDB)


class CacheManager:
    def __init__(self, backend: AdapterDBType, **kwargs):
        self.backend = backend

    def fetch_data(
        self,
        query: Optional[Union[List[dict], dict]] = None,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        try:
            if not (server := kwargs.pop('server', None)):
                server = FunctionsClient(host, port)
            return server.fetch(query=query, **kwargs)
        except Exception as e:
            # TODO: Map possible errors
            raise e

    def bulk_insert(self, documents: Dict[str, Any]) -> None:
        """insert statements in "bulk", not returning rows"""
        self.backend.bulk_insert(documents)
