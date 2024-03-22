import json  # noqa: F401
from abc import ABC, abstractmethod
from http import HTTPStatus  # noqa: F401
from typing import Any, Dict, Iterator, List, Optional, Set, Tuple, Union

from hyperon_das_atomdb import WILDCARD
from hyperon_das_atomdb.exceptions import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist
from requests import sessions
from requests.exceptions import (  # noqa: F401
    ConnectionError,
    HTTPError,
    JSONDecodeError,
    RequestException,
    Timeout,
)

from hyperon_das.cache import (
    AndEvaluator,
    CustomQuery,
    LazyQueryEvaluator,
    ListIterator,
    LocalGetLinks,
    LocalIncomingLinks,
    QueryAnswerIterator,
    RemoteGetLinks,
    RemoteIncomingLinks,
)
from hyperon_das.client import FunctionsClient
from hyperon_das.decorators import retry
from hyperon_das.exceptions import (
    InvalidDASParameters,
    QueryParametersException,
    UnexpectedQueryFormat,
)
from hyperon_das.logger import logger
from hyperon_das.utils import Assignment, QueryAnswer, get_package_version, serialize  # noqa: F401


class QueryEngine(ABC):
    @abstractmethod
    def get_atom(self, handle: str) -> Union[Dict[str, Any], None]:
        ...  # pragma no cover

    @abstractmethod
    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        ...  # pragma no cover

    @abstractmethod
    def get_link(self, link_type: str, targets: List[str]) -> Union[Dict[str, Any], None]:
        ...  # pragma no cover

    @abstractmethod
    def get_links(
        self, link_type: str, target_types: List[str] = None, link_targets: List[str] = None
    ) -> Union[List[str], List[Dict]]:
        ...  # pragma no cover

    @abstractmethod
    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> List[Union[dict, str, Tuple[dict, List[dict]]]]:
        ...  # pragma no cover

    @abstractmethod
    def query(
        self, query: Dict[str, Any], parameters: Optional[Dict[str, Any]] = {}
    ) -> Union[QueryAnswerIterator, List[Tuple[Assignment, Dict[str, Any]]]]:
        ...  # pragma no cover

    @abstractmethod
    def custom_query(self, index_id: str, **kwargs) -> Union[Iterator, List[Dict[str, Any]]]:
        ...  # pragma no cover

    @abstractmethod
    def count_atoms(self) -> Tuple[int, int]:
        ...  # pragma no cover

    @abstractmethod
    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        ...  # pragma no cover

    @abstractmethod
    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        ...  # pragma no cover


class LocalQueryEngine(QueryEngine):
    def __init__(self, backend, kwargs: Optional[dict] = None) -> None:
        self.local_backend = backend

    def _error(self, exception: Exception):
        logger().error(str(exception))
        raise exception

    def _recursive_query(
        self,
        query: Union[Dict[str, Any], List[Dict[str, Any]]],
        mappings: Set[Assignment] = None,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> QueryAnswerIterator:
        if isinstance(query, list):
            sub_expression_results = [
                self._recursive_query(expression, mappings, parameters) for expression in query
            ]
            return AndEvaluator(sub_expression_results)
        elif query["atom_type"] == "node":
            try:
                atom_handle = self.local_backend.get_node_handle(query["type"], query["name"])
                return ListIterator(
                    [QueryAnswer(self.local_backend.get_atom_as_dict(atom_handle), None)]
                )
            except NodeDoesNotExist:
                return ListIterator([])
        elif query["atom_type"] == "link":
            matched_targets = []
            for target in query["targets"]:
                if target["atom_type"] == "node" or target["atom_type"] == "link":
                    matched = self._recursive_query(target, mappings, parameters)
                    if matched:
                        matched_targets.append(matched)
                elif target["atom_type"] == "variable":
                    matched_targets.append(ListIterator([QueryAnswer(target, None)]))
                else:
                    self._error(
                        UnexpectedQueryFormat(
                            message="Query processing reached an unexpected state",
                            details=f'link: {str(query)} link target: {str(query)}',
                        )
                    )
            return LazyQueryEvaluator(query["type"], matched_targets, self, parameters)
        else:
            self._error(
                UnexpectedQueryFormat(
                    message="Query processing reached an unexpected state",
                    details=f'query: {str(query)}',
                )
            )

    def _to_link_dict_list(self, db_answer: Union[List[str], List[Dict]]) -> List[Dict]:
        if not db_answer:
            return []
        flat_handle = isinstance(db_answer[0], str)
        answer = []
        for atom in db_answer:
            handle = atom if flat_handle else atom[0]
            answer.append(self.local_backend.get_atom_as_dict(handle))
        return answer

    def _get_related_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ):
        if link_type != WILDCARD and target_types is not None:
            return self.local_backend.get_matched_type_template(
                [link_type, *target_types], **kwargs
            )
        elif link_targets is not None:
            try:
                return self.local_backend.get_matched_links(link_type, link_targets, **kwargs)
            except LinkDoesNotExist:
                return None, [] if kwargs.get('cursor') is not None else []
        elif link_type != WILDCARD:
            return self.local_backend.get_matched_type(link_type, **kwargs)
        else:
            self._error(ValueError("Invalid parameters"))

    def get_atom(self, handle: str, **kwargs) -> Union[Dict[str, Any], None]:
        try:
            return self.local_backend.get_atom(handle, **kwargs)
        except AtomDoesNotExist as e:
            raise e

    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        try:
            node_handle = self.local_backend.node_handle(node_type, node_name)
            return self.local_backend.get_atom(node_handle)
        except AtomDoesNotExist:
            raise NodeDoesNotExist(
                message='This node does not exist', details=f'{node_type}:{node_name}'
            )

    def get_link(self, link_type: str, link_targets: List[str]) -> Union[Dict[str, Any], None]:
        try:
            link_handle = self.local_backend.link_handle(link_type, link_targets)
            return self.local_backend.get_atom(link_handle)
        except AtomDoesNotExist:
            raise LinkDoesNotExist(
                message='This link does not exist', details=f'{link_type}:{link_targets}'
            )

    def get_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ) -> Union[Iterator, List[str], List[Dict]]:
        if kwargs.get('no_iterator', True):
            answer = self._get_related_links(link_type, target_types, link_targets, **kwargs)
            if answer and isinstance(answer[0], int):
                return answer[0], self._to_link_dict_list(answer[1])
            return self._to_link_dict_list(answer)
        else:
            if kwargs.get('cursor') is None:
                kwargs['cursor'] = 0
            answer = self._get_related_links(link_type, target_types, link_targets, **kwargs)
            kwargs['backend'] = self
            kwargs['link_type'] = link_type
            kwargs['target_types'] = target_types
            kwargs['link_targets'] = link_targets
            if isinstance(answer, tuple):  # redis_mongo use case
                kwargs['cursor'] = answer[0]
                answer = answer[1]
            return LocalGetLinks(ListIterator(answer), **kwargs)

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> Union[Iterator, List[Union[dict, str, Tuple[dict, List[dict]]]]]:
        if kwargs.get('no_iterator', True):
            return self.local_backend.get_incoming_links(atom_handle, **kwargs)
        else:
            kwargs['handles_only'] = True
            links = self.local_backend.get_incoming_links(atom_handle, **kwargs)
            kwargs['backend'] = self.local_backend
            kwargs['atom_handle'] = atom_handle
            if isinstance(links, tuple):  # redis_mongo use case
                kwargs['cursor'] = links[0]
                links = links[1]
            return LocalIncomingLinks(ListIterator(links), **kwargs)

    def query(
        self,
        query: Union[List[Dict[str, Any]], Dict[str, Any]],
        parameters: Optional[Dict[str, Any]] = {},
    ) -> Union[QueryAnswerIterator, List[Tuple[Assignment, Dict[str, str]]]]:
        no_iterator = parameters.get("no_iterator", False)
        if no_iterator:
            logger().debug(
                {
                    'message': '[DistributedAtomSpace][query] - Start',
                    'data': {'query': query, 'parameters': parameters},
                }
            )
        query_results = self._recursive_query(query, parameters)
        if no_iterator:
            answer = []
            for result in query_results:
                answer.append(tuple([result.assignment, result.subgraph]))
            logger().debug(f"query: {query} result: {str(answer)}")
            return answer
        else:
            return query_results

    def custom_query(self, index_id: str, **kwargs) -> Union[Iterator, List[Dict[str, Any]]]:
        if kwargs.pop('no_iterator', True):
            return self.local_backend.get_atoms_by_index(index_id, **kwargs)
        else:
            if kwargs.get('cursor') is None:
                kwargs['cursor'] = 0
            cursor, answer = self.local_backend.get_atoms_by_index(index_id, **kwargs)
            kwargs['backend'] = self.local_backend
            kwargs['index_id'] = index_id
            kwargs['cursor'] = cursor
            return CustomQuery(ListIterator(answer), **kwargs)

    def count_atoms(self) -> Tuple[int, int]:
        return self.local_backend.count_atoms()

    def commit(self):
        self.local_backend.commit()

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]] = None):
        self.local_backend.reindex(pattern_index_templates)

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        return self.local_backend.create_field_index(atom_type, field, type, composite_type)


class RemoteQueryEngine(QueryEngine):
    def __init__(self, backend, kwargs):
        self.local_query_engine = LocalQueryEngine(backend, kwargs)
        host = kwargs.get('host')
        port = kwargs.get('port')
        if not host:
            raise InvalidDASParameters(message='Send `host` parameter to connect in a remote DAS')
        url = self._connect_server(host, port)
        self.remote_das = FunctionsClient(url)

    @retry(attempts=5, timeout_seconds=120)
    def _connect_server(self, host: str, port: Optional[str] = None):
        port = port or '8081'
        openfaas_uri = f'http://{host}:{port}/function/query-engine'
        aws_lambda_uri = f'http://{host}/prod/query-engine'
        url = None
        if self._is_server_connect(openfaas_uri):
            url = openfaas_uri
        elif self._is_server_connect(aws_lambda_uri):
            url = aws_lambda_uri
        return url

    # TODO: Use this method when version checking is running on the server
    # def _is_server_connect(self, url: str) -> bool:
    #     logger().debug(f'connecting to remote Das {url}')
    #     das_version = get_package_version('hyperon_das')

    #     try:
    #         with sessions.Session() as session:
    #             response = session.request(
    #                 method='POST',
    #                 url=url,
    #                 data=json.dumps(
    #                     {
    #                         'action': 'handshake',
    #                         'input': {
    #                             'das_version': das_version,
    #                             'atomdb_version': get_package_version('hyperon_das_atomdb'),
    #                         },
    #                     }
    #                 ),
    #                 timeout=10,
    #             )
    #         if response.status_code == HTTPStatus.CONFLICT:
    #             try:
    #                 remote_das_version = response.json().get('das').get('version')
    #             except JSONDecodeError as e:
    #                 raise Exception(str(e))
    #             logger().error(
    #                 f'Package version conflict error when connecting to remote DAS - Local DAS: `{das_version}` - Remote DAS: `{remote_das_version}`'
    #             )
    #             raise Exception(
    #                 f'The version sent by the local DAS is {das_version}, but the expected version on the server is {remote_das_version}'
    #             )
    #         elif response.status_code == HTTPStatus.OK:
    #             return True
    #         else:
    #             response.raise_for_status()
    #             return False
    #     except (ConnectionError, Timeout, HTTPError, RequestException):
    #         return False

    def _is_server_connect(self, url: str) -> bool:
        logger().debug(f'connecting to remote Das {url}')
        try:
            with sessions.Session() as session:
                response = session.request(
                    method='POST',
                    url=url,
                    data=serialize({"action": "ping", "input": {}}),
                    headers={'Content-Type': 'application/octet-stream'},
                    timeout=10,
                )
        except Exception:
            return False
        if response.status_code == 200:
            return True
        return False

    def get_atom(self, handle: str, **kwargs) -> Dict[str, Any]:
        try:
            atom = self.local_query_engine.get_atom(handle, **kwargs)
        except AtomDoesNotExist:
            try:
                atom = self.remote_das.get_atom(handle, **kwargs)
            except AtomDoesNotExist:
                raise AtomDoesNotExist(
                    message='This atom does not exist', details=f'handle:{handle}'
                )
        return atom

    def get_node(self, node_type: str, node_name: str) -> Dict[str, Any]:
        try:
            node = self.local_query_engine.get_node(node_type, node_name)
        except NodeDoesNotExist:
            try:
                node = self.remote_das.get_node(node_type, node_name)
            except NodeDoesNotExist:
                raise NodeDoesNotExist(
                    message='This node does not exist', details=f'{node_type}:{node_name}'
                )
        return node

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        try:
            link = self.local_query_engine.get_link(link_type, link_targets)
        except LinkDoesNotExist:
            try:
                link = self.remote_das.get_link(link_type, link_targets)
            except LinkDoesNotExist:
                raise LinkDoesNotExist(
                    message='This link does not exist', details=f'{link_type}:{link_targets}'
                )
        return link

    def get_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ) -> Union[Iterator, List[str], List[Dict]]:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        links = self.local_query_engine.get_links(link_type, target_types, link_targets, **kwargs)
        cursor, remote_links = self.remote_das.get_links(
            link_type, target_types, link_targets, **kwargs
        )
        kwargs['cursor'] = cursor
        kwargs['backend'] = self.remote_das
        kwargs['link_type'] = link_type
        kwargs['target_types'] = target_types
        kwargs['link_targets'] = link_targets
        links.extend(remote_links)
        return RemoteGetLinks(ListIterator(links), **kwargs)

    def get_incoming_links(self, atom_handle: str, **kwargs) -> Iterator:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        kwargs['handles_only'] = False
        links = self.local_query_engine.get_incoming_links(atom_handle, **kwargs)
        cursor, remote_links = self.remote_das.get_incoming_links(atom_handle, **kwargs)
        kwargs['cursor'] = cursor
        kwargs['backend'] = self.remote_das
        kwargs['atom_handle'] = atom_handle
        links.extend(remote_links)
        return RemoteIncomingLinks(ListIterator(links), **kwargs)

    def custom_query(self, index_id: str, **kwargs) -> Iterator:
        kwargs.pop('no_iterator', None)
        if kwargs.get('cursor') is None:
            kwargs['cursor'] = 0
        cursor, answer = self.remote_das.custom_query(index_id, **kwargs)
        kwargs['backend'] = self.remote_das
        kwargs['index_id'] = index_id
        kwargs['cursor'] = cursor
        kwargs['is_remote'] = True
        return CustomQuery(ListIterator(answer), **kwargs)

    def query(
        self,
        query: Union[List[Dict[str, Any]], Dict[str, Any]],
        parameters: Optional[Dict[str, Any]] = {},
    ) -> List[Dict[str, Any]]:
        query_scope = parameters.get('query_scope', 'remote_only')
        if query_scope == 'remote_only' or query_scope == 'synchronous_update':
            if query_scope == 'synchronous_update':
                self.commit()
            previous_value = parameters.get('no_iterator', False)
            parameters['no_iterator'] = True
            answer = self.remote_das.query(query, parameters)
            parameters['no_iterator'] = previous_value
        elif query_scope == 'local_only':
            answer = self.local_query_engine.query(query, parameters)
        elif query_scope == 'local_and_remote':
            # This type is not available yet
            raise QueryParametersException
        else:
            raise QueryParametersException(
                message=f'Invalid value for parameter "query_scope": "{query_scope}"'
            )
        return answer

    def count_atoms(self) -> Tuple[int, int]:
        local_answer = self.local_query_engine.count_atoms()
        remote_answer = self.remote_das.count_atoms()
        return tuple([x + y for x, y in zip(local_answer, remote_answer)])

    def commit(self):
        return self.remote_das.commit_changes()

    def reindex(self, pattern_index_templates: Optional[Dict[str, Dict[str, Any]]]):
        raise NotImplementedError()

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        return self.remote_das.create_field_index(atom_type, field, type, composite_type)
