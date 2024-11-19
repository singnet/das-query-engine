import time
from time import sleep
from random import randint
from typing import Any, Union, Iterator, List, Optional, Dict

from hyperon_das_atomdb.database import HandleListT, AtomT, HandleT, IncomingLinksT, HandleSetT, LinkT


from hyperon_das.context import Context
from hyperon_das.link_filters import LinkFilter
from hyperon_das.query_engines.query_engine_protocol import QueryEngine
from hyperon_das.type_alias import Query
from hyperon_das.tokenizers.dict_query_tokenizer import DictQueryTokenizer
from hyperon_das.utils import QueryAnswer
from hyperon_das.das_node.das_node import DASNode
from hyperon_das.das_node.simple_node import SimpleNodeClient, SimpleNodeServer
from hyperon_das.das_node.remote_iterator import RemoteIterator
from hyperon_das.das_node.query_answer import QueryAnswer


class DASNodeQueryEngine(QueryEngine):

    def __init__(
            self,
            backend,
            cache_controller,
            system_parameters: Dict[str, Any],
            **kwargs
    ):
        self.next_query_port =  randint(60000, 61999)
        self.timeout = system_parameters.get("timeout", 5)
        self.id = "localhost:" + str(self.next_query_port)
        self.host = system_parameters.get("hostname", "localhost")
        self.port = system_parameters.get("port", 35700)
        self.remote_das_node = ":".join([self.host, str(self.port)])
        self.requestor = DASNode(self.id, self.remote_das_node)



    def query(
        self, query: Query, parameters: dict[str, Any] | None = None
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        if parameters and parameters.get("tokenize"):
            query = DictQueryTokenizer.tokenize(query).split()
        response: RemoteIterator = self.requestor.pattern_matcher_query(query)
        start = time.time()
        try:
            while not response.finished():
                while (qs  := response.pop()) is None:
                    if time.time() - start > self.timeout:
                        raise Exception("Timeout")
                    if response.finished():
                        break
                    else:
                        sleep(1)
                if qs is not None:
                    yield qs.get_handles()
        except Exception as e:
            raise e
        finally:
            del response



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

    def create_field_index(self, atom_type: str, fields: List[str], named_type: Optional[str] = None,
                           composite_type: Optional[List[Any]] = None, index_type: Optional[str] = None) -> str:
        pass

    def fetch(self, query: Query, host: Optional[str] = None, port: Optional[int] = None, **kwargs) -> Any:
        pass

    def create_context(self, name: str, queries: list[Query] | None = None) -> Context:
        pass

    def commit(self, **kwargs) -> None:
        pass

    def get_atoms_by_field(self, query: Query) -> HandleListT:
        pass

    def get_atoms_by_text_field(self, text_value: str, field: Optional[str] = None,
                                text_index_id: Optional[str] = None) -> HandleListT:
        pass

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> HandleListT:
        pass


