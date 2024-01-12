import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from hyperon_das_atomdb import WILDCARD
from hyperon_das_atomdb.exceptions import LinkDoesNotExist, AtomDoesNotExist, NodeDoesNotExist

from hyperon_das.cache import AndEvaluator, LazyQueryEvaluator, ListIterator, QueryAnswerIterator
from hyperon_das.client import FunctionsClient
from hyperon_das.decorators import retry
from hyperon_das.exceptions import (
    InvalidDASParameters,
    QueryParametersException,
    UnexpectedQueryFormat,
)
from hyperon_das.logger import logger
from hyperon_das.utils import Assignment, QueryAnswer


class QueryEngine(ABC):
    @abstractmethod
    def get_atom(self, handle: str) -> Union[Dict[str, Any], None]:
        ...

    @abstractmethod
    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        ...

    @abstractmethod
    def get_link(self, link_type: str, targets: List[str]) -> Union[Dict[str, Any], None]:
        ...

    @abstractmethod
    def get_links(
        self, link_type: str, target_types: List[str] = None, link_targets: List[str] = None
    ) -> Union[List[str], List[Dict]]:
        ...

    @abstractmethod
    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = {},
    ) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def count_atoms(self) -> Tuple[int, int]:
        ...


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
            if flat_handle:
                handle = atom
                arity = -1
            else:
                handle, targets = atom
                arity = len(targets)
            answer.append(self.local_backend.get_atom_as_dict(handle, arity))
        return answer

    def get_atom(self, handle: str) -> Union[Dict[str, Any], None]:
        try:
            return self.local_backend.get_atom(handle)
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
        self, link_type: str, target_types: List[str] = None, link_targets: List[str] = None
    ) -> Union[List[str], List[Dict]]:
        if target_types is not None and link_type != WILDCARD:
            db_answer = self.local_backend.get_matched_type_template([link_type, *target_types])
        elif link_targets is not None:
            db_answer = self.local_backend.get_matched_links(link_type, link_targets)
        elif link_type != WILDCARD:
            db_answer = self.local_backend.get_matched_type(link_type)
        else:
            self._error(ValueError("Invalid parameters"))

        return self._to_link_dict_list(db_answer)

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = {},
    ) -> List[Dict[str, Any]]:
        logger().debug(
            {
                'message': '[DistributedAtomSpace][query] - Start',
                'data': {'query': query, 'parameters': parameters},
            }
        )
        query_results = self._recursive_query(query, parameters)
        logger().debug(f"query: {query} result: {str(query_results)}")
        answer = []
        for result in query_results:
            answer.append(result.subgraph)
        return answer

    def count_atoms(self) -> Tuple[int, int]:
        return self.local_backend.count_atoms()

    def commit(self):
        self.local_backend.commit()


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

    def _is_server_connect(self, url: str) -> bool:
        logger().debug(f'connecting to remote Das {url}')
        try:
            response = requests.request(
                'POST',
                url=url,
                data=json.dumps({"action": "ping", "input": {}}),
                timeout=10,
            )
        except Exception as e:
            return False
        if response.status_code == 200:
            return True
        return False

    def get_atom(self, handle: str) -> Dict[str, Any]:
        try:
            atom = self.local_query_engine.get_atom(handle)
        except AtomDoesNotExist:
            atom = self.remote_das.get_atom(handle)
        if not atom:
            raise AtomDoesNotExist(message='This atom does not exist', details=f'handle:{handle}')
        return atom

    def get_node(self, node_type: str, node_name: str) -> Dict[str, Any]:
        try:
            node = self.local_query_engine.get_node(node_type, node_name)
        except NodeDoesNotExist:
            node = self.remote_das.get_node(node_type, node_name)
        if not node:
            raise NodeDoesNotExist(
                message='This node does not exist', details=f'{node_type}:{node_name}'
            )
        return node

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        try:
            link = self.local_query_engine.get_link(link_type, link_targets)
        except LinkDoesNotExist:
            link = self.remote_das.get_link(link_type, link_targets)
        if not link:
            raise LinkDoesNotExist(
                message='This link does not exist', details=f'{link_type}:{link_targets}'
            )
        return link

    def get_links(
        self, link_type: str, target_types: List[str] = None, link_targets: List[str] = None
    ) -> Union[List[str], List[Dict]]:
        local = self.local_query_engine.get_links(link_type, target_types, link_targets)
        if not local:
            return self.remote_das.get_links(link_type, target_types, link_targets)

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = {},
    ) -> List[Dict[str, Any]]:
        query_scope = parameters.get('query_scope', 'remote_only')
        if query_scope == 'remote_only':
            answer = self.remote_das.query(query, parameters)
        elif query_scope == 'local_only':
            answer = self.local_query_engine.query(query, parameters)
        elif query_scope == 'local_and_remote':
            # This type is not available yet
            raise QueryParametersException
        elif query_scope == 'synchronous_update':
            self.commit()
            answer = self.remote_das.query(query, parameters)
        return answer

    def count_atoms(self) -> Tuple[int, int]:
        local_answer = self.local_query_engine.count_atoms()
        remote_answer = self.remote_das.count_atoms()
        return tuple([x + y for x, y in zip(local_answer, remote_answer)])

    def commit(self):
        return self.remote_das.commit_changes()
