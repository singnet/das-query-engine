from typing import Any, Dict, Iterator, List, Optional, Union

from hyperon_das_atomdb.database import IncomingLinksT
from hyperon_das_atomdb.exceptions import AtomDoesNotExist

from hyperon_das.cache.cache_controller import CacheController
from hyperon_das.cache.iterators import (
    CustomQuery,
    ListIterator,
    RemoteGetLinks,
    RemoteIncomingLinks,
)
from hyperon_das.client import FunctionsClient
from hyperon_das.context import Context
from hyperon_das.exceptions import InvalidDASParameters, QueryParametersException
from hyperon_das.query_engines.local_query_engine import LocalQueryEngine
from hyperon_das.query_engines.query_engine_protocol import QueryEngine
from hyperon_das.type_alias import Query
from hyperon_das.utils import QueryAnswer, das_error


class RemoteQueryEngine(QueryEngine):
    def __init__(
        self,
        backend,
        cache_controller: CacheController,
        system_parameters: Dict[str, Any],
        **kwargs,
    ):
        self.system_parameters = system_parameters
        self.local_query_engine = LocalQueryEngine(backend, cache_controller, kwargs)
        self.cache_controller = cache_controller
        self.__mode = kwargs.get('mode', 'read-only')
        self.host = kwargs.get('host')
        self.port = kwargs.get('port')
        if not self.host or not self.port:
            das_error(InvalidDASParameters(message="'host' and 'port' are mandatory parameters"))
        self.remote_das = FunctionsClient(self.host, self.port)

    @property
    def mode(self):
        return self.__mode

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        atom = self.cache_controller.get_atom(handle)
        if atom is None:
            try:
                atom = self.local_query_engine.get_atom(handle, **kwargs)
            except AtomDoesNotExist:
                try:
                    atom = self.remote_das.get_atom(handle, **kwargs)
                except AtomDoesNotExist as exception:
                    das_error(exception)
        return atom

    def get_links(
        self,
        link_type: str,
        target_types: list[str] | None = None,
        link_targets: list[str] | None = None,
        **kwargs,
    ) -> Union[Iterator, List[str], List[Dict], tuple[int, List[Dict]]]:  # TODO: simplify
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        links = self.local_query_engine.get_links(link_type, target_types, link_targets, **kwargs)
        remote_links = self.remote_das.get_links(link_type, target_types, link_targets, **kwargs)
        if isinstance(remote_links, tuple):
            cursor, remote_links = remote_links
            kwargs['cursor'] = cursor
        kwargs['backend'] = self.remote_das
        kwargs['link_type'] = link_type
        kwargs['target_types'] = target_types
        kwargs['link_targets'] = link_targets
        links.extend(remote_links)
        return RemoteGetLinks(ListIterator(links), **kwargs)

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> tuple[int | None, IncomingLinksT | Iterator]:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        kwargs['handles_only'] = False
        _, links = self.local_query_engine.get_incoming_links(atom_handle, **kwargs)
        cursor, remote_links = self.remote_das.get_incoming_links(atom_handle, **kwargs)
        kwargs['cursor'] = cursor
        kwargs['backend'] = self.remote_das
        kwargs['atom_handle'] = atom_handle
        links.extend(remote_links)
        if (
            links
            and kwargs.get('targets_document')
            and isinstance(links, list)
            and isinstance(links[0], dict)
            and 'targets_document' in links[0]
        ):
            links = [(link, link.pop('targets_document', [])) for link in links]
        return cursor, RemoteIncomingLinks(ListIterator(links), **kwargs)

    def custom_query(self, index_id: str, query: Query, **kwargs) -> Iterator:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        cursor, answer = self.remote_das.custom_query(index_id, query=query, **kwargs)
        kwargs['backend'] = self.remote_das
        kwargs['index_id'] = index_id
        kwargs['cursor'] = cursor
        kwargs['is_remote'] = True
        return CustomQuery(ListIterator(answer), **kwargs)

    def query(
        self,
        query: Query,
        parameters: Optional[Dict[str, Any]] = {},
    ) -> Union[Iterator[QueryAnswer], List[QueryAnswer]]:
        query_scope = parameters.get('query_scope', 'remote_only')
        if query_scope == 'remote_only' or query_scope == 'synchronous_update':
            if query_scope == 'synchronous_update':
                self.commit()
            parameters['no_iterator'] = True
            answer = self.remote_das.query(query, parameters)
        elif query_scope == 'local_only':
            answer = self.local_query_engine.query(query, parameters)
        elif query_scope == 'local_and_remote':
            das_error(
                QueryParametersException(
                    message=(
                        f"Invalid value for parameter 'query_scope': '{query_scope}'. "
                        "This type of query scope is not implemented yet"
                    )
                )
            )
        else:
            das_error(
                QueryParametersException(
                    message=f'Invalid value for "query_scope": "{query_scope}"'
                )
            )
        return answer

    def count_atoms(self, parameters: Optional[Dict[str, Any]] = None) -> Dict[str, int]:
        if (context := parameters.get('context') if parameters else None) == 'local':
            return self.local_query_engine.count_atoms(parameters)
        if context == 'remote':
            return self.remote_das.count_atoms(parameters)

        local_answer = self.local_query_engine.count_atoms(parameters)
        remote_answer = self.remote_das.count_atoms(parameters)
        return {
            k: (local_answer.get(k, 0) + remote_answer.get(k, 0))
            for k in ['node_count', 'link_count', 'atom_count']
        }

    #
    def commit(self, **kwargs) -> None:
        if self.__mode == 'read-write':
            if self.local_query_engine.has_buffer():
                return self.remote_das.commit_changes(buffer=self.local_query_engine.buffer)
            return self.remote_das.commit_changes()
        elif self.__mode == 'read-only':
            das_error(PermissionError("Commit can't be executed in read mode"))
        else:
            das_error(ValueError("Invalid mode: '{self.__mode}'. Use 'read-only' or 'read-write'"))

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        das_error(NotImplementedError())

    def create_field_index(
        self,
        atom_type: str,
        fields: List[str],
        named_type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
        index_type: Optional[str] = None,
    ) -> str:
        return self.remote_das.create_field_index(
            atom_type, fields, named_type, composite_type, index_type
        )

    def fetch(
        self,
        query: Query,
        host: Optional[str] = None,
        port: Optional[int] = None,
        **kwargs,
    ) -> Any:
        if host is not None and port is not None:
            server = FunctionsClient(host, port)
        else:
            server = self.remote_das
        return server.fetch(query=query, **kwargs)

    def create_context(
        self,
        name: str,
        queries: Optional[List[Query]] = None,
    ) -> Context:
        return self.remote_das.create_context(name, queries)

    def get_atoms_by_field(self, query: Query) -> List[str]:
        return self.remote_das.get_atoms_by_field(query)

    def get_atoms_by_text_field(
        self, text_value: str, field: Optional[str] = None, text_index_id: Optional[str] = None
    ) -> List[str]:
        return self.remote_das.get_atoms_by_text_field(text_value, field, text_index_id)

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> List[str]:
        return self.remote_das.get_node_by_name_starting_with(node_type, startswith)
