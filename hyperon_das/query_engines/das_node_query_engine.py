import time
from random import randint
from time import sleep
from typing import Any, Dict, List, Optional, Union
from collections.abc import Iterator


from hyperon_das_atomdb.database import (
    AtomT,
    HandleListT,
    HandleSetT,
    HandleT,
    IncomingLinksT,
    LinkT,
)

from hyperon_das.context import Context
from hyperon_das.das_node.das_node import DASNode
from hyperon_das.das_node.query_answer import QueryAnswer
from hyperon_das.das_node.remote_iterator import RemoteIterator
from hyperon_das.link_filters import LinkFilter
from hyperon_das.query_engines.query_engine_protocol import QueryEngine
from hyperon_das.tokenizers.dict_query_tokenizer import DictQueryTokenizer
from hyperon_das.type_alias import Query


class QueryIterator(Iterator):

    def __init__(self, remote_iterator: RemoteIterator):
        self.remote_iterator = remote_iterator

    def finished(self):
        return self.remote_iterator.finished()

    def __next__(self):
        if not self.finished():
            return self.remote_iterator.pop()
        else:
            if hasattr(self, "remote_iterator") and self.remote_iterator:
                del self.remote_iterator
            raise StopIteration()

    def __enter__(self):
        return self

    def __del__(self):
        if hasattr(self, "remote_iterator") and self.remote_iterator:
            del self.remote_iterator

    def __exit__(self, exc_type, exc_val, exc_tb):
        if hasattr(self, "remote_iterator") and self.remote_iterator:
            del self.remote_iterator



def simple_retry(function):
    def wrapper(*args, **kwargs):
        retries = (kwargs.get("parameters", {}) if len(args) < 3 else args[2]).get("retry", 0) + 1
        for r in range(retries):
            try:
                yield from function(*args, **kwargs)
                break
            except Exception as e:
                print(f"Retrying {r + 1}")
                if r == retries -1:
                    raise e
    return wrapper


class DASNodeQueryEngine(QueryEngine):
    def __init__(self, backend, cache_controller, system_parameters: Dict[str, Any], **kwargs):
        self.next_query_port = randint(60000, 61999)
        self.timeout = kwargs.get("timeout", 0)
        self.host = kwargs.get("hostname", "localhost")
        self.requester_ip = kwargs.get("requester_ip", "localhost")
        self.id = f"{self.requester_ip}:" + str(self.next_query_port)
        self.port = kwargs.get("port", 35700)
        self.remote_das_node = ":".join([self.host, str(self.port)])
        self.requestor = DASNode(self.id, self.remote_das_node)


    def _parse_query(self, query, parameters):
        tokenize = parameters.get("tokenize", True) if parameters else True
        if tokenize:
            if isinstance(query, list):
                query = {"and": query}
            return DictQueryTokenizer.tokenize(query).split()
        return query



    @simple_retry
    def query(
        self, query: Query, parameters: dict[str, Any] | None = None
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        query = self._parse_query(query, parameters)
        max_responses = parameters.get("max_responses", 0) if parameters else 0
        response: RemoteIterator = self.requestor.pattern_matcher_query(query)
        print(" ".join(query))
        start = time.time()
        count = 0
        try:
            while not response.finished():
                while (qs := response.pop()) is None:
                    if 0 < self.timeout < time.time() - start:
                        raise TimeoutError("Timeout")
                    if response.finished():
                        break
                    sleep(1)
                if qs is not None:
                    yield qs
                    if 0 < max_responses < count:
                        break
        except TimeoutError as e:
            raise e
        finally:
            del response


    def query_async(self, query: Query, parameters: dict[str, Any] | None = None
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        query = self._parse_query(query, parameters)
        response: RemoteIterator = self.requestor.pattern_matcher_query(query)
        print(" ".join(query))
        return QueryIterator(response)

    def get_atom(self, handle: HandleT) -> AtomT:
        pass

    def get_atoms(self, handles: HandleListT, **kwargs) -> List[AtomT]:
        pass

    def get_links(self, link_filter: LinkFilter) -> List[LinkT]:
        pass

    def get_link_handles(self, link_filter: LinkFilter) -> HandleSetT:
        pass

    def get_incoming_links(self, atom_handle: HandleT, **kwargs) -> IncomingLinksT:
        pass

    def custom_query(self, index_id: str, query: Query, **kwargs) -> Union[Iterator, List[AtomT]]:
        pass

    def count_atoms(self, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        pass

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        pass

    def create_field_index(
        self,
        atom_type: str,
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[str] = None,
    ) -> str:
        pass

    def fetch(
        self, query: Query, host: Optional[str] = None, port: Optional[int] = None, **kwargs
    ) -> Any:
        pass

    def create_context(self, name: str, queries: list[Query] | None = None) -> Context:
        pass

    def commit(self, **kwargs) -> None:
        pass

    def get_atoms_by_field(self, query: Query) -> HandleListT:
        pass

    def get_atoms_by_text_field(
        self, text_value: str, field: Optional[str] = None, text_index_id: Optional[str] = None
    ) -> HandleListT:
        pass

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> HandleListT:
        pass
