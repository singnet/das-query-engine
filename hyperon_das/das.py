import json
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB
from hyperon_das_atomdb.exceptions import (
    AtomDoesNotExist,
    InvalidAtomDB,
    LinkDoesNotExist,
    NodeDoesNotExist,
)

from hyperon_das.cache import AndEvaluator, LazyQueryEvaluator, ListIterator, QueryAnswerIterator
from hyperon_das.client import FunctionsClient
from hyperon_das.decorators import retry
from hyperon_das.exceptions import (
    InvalidDASParameters,
    InvalidQueryEngine,
    QueryParametersException,
    UnexpectedQueryFormat,
)
from hyperon_das.logger import logger
from hyperon_das.utils import Assignment, QueryAnswer, QueryOutputFormat


class DistributedAtomSpace:
    def __init__(self, kwargs: Dict[str, Any]) -> None:
        atomdb_parameter = kwargs.get('atomdb', 'ram')
        query_engine_parameter = kwargs.get('query_engine', 'local')

        if atomdb_parameter == "ram":
            self.backend = InMemoryDB()
        elif atomdb_parameter == "redis_mongo":
            mongo_db_hostname = kwargs.get('mongo_db_hostname')
            mongo_db_port = kwargs.get('mongo_db_port')
            mongo_db_username = kwargs.get('mongo_db_username')
            mongo_db_password = kwargs.get('mongo_db_password')
            mongo_tls_ca_file = kwargs.get('mongo_tls_ca_file')
            redis_hostname = kwargs.get('redis_hostname')
            redis_port = kwargs.get('redis_port')
            redis_username = kwargs.get('redis_username')
            redis_password = kwargs.get('redis_password')
            redis_cluster = kwargs.get('redis_cluster')
            redis_ssl = kwargs.get('redis_ssl')
            required_parameters = [
                mongo_db_hostname,
                mongo_db_port,
                mongo_db_username,
                mongo_db_password,
                redis_hostname,
                redis_port,
            ]
            if not all([False if p is None else True for p in required_parameters]):
                raise InvalidDASParameters(
                    message='Send required parameters to instantiate a RedisMongo',
                    details="'mongo_db_hostname', 'mongo_db_port', 'mongo_db_username', 'mongo_db_password', 'redis_hostname', 'redis_port'",
                )
            # Implemnt this parameters in ATOMDB
            self.backend = RedisMongoDB(
                mongo_db_hostname=mongo_db_hostname,
                mongo_db_port=mongo_db_port,
                mongo_db_username=mongo_db_username,
                mongo_db_password=mongo_db_password,
                mongo_tls_ca_file=mongo_tls_ca_file,
                redis_hostname=redis_hostname,
                redis_port=redis_port,
                redis_username=redis_username,
                redis_password=redis_password,
                redis_cluster=redis_cluster,
                redis_ssl=redis_ssl,
            )
            if query_engine_parameter != "local":
                raise InvalidDASParameters(
                    message='Can`t instantiate a RedisMongo with query_engine=`local`'
                )
        else:
            # Implemente this exception in ATOMDB
            raise InvalidAtomDB

        if query_engine_parameter == 'local':
            self.query_engine = LocalQueryEngine(self.backend, kwargs)
        elif query_engine_parameter == "remote":
            self.query_engine = RemoteQueryEngine(self.backend, kwargs)
        else:
            raise InvalidQueryEngine(
                message='The possible values are: `local` or `remote`',
                details=f'query_engine={query_engine_parameter}',
            )

    def _error(self, exception: Exception):
        logger().error(str(exception))
        raise exception

    def get_atom(self, handle: str) -> Dict[str, Any]:
        """
        Retrieve information about an Atom using its handle.

        This method retrieves information about an Atom from the database
        based on the provided handle. The retrieved atom information can be
        presented in different output formats as specified by the output_format parameter.

        Args:
            handle (str): The unique handle of the atom.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the Atom (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing detailed Atom information (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the Atom (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Example:
            >>> result = obj.get_atom(
                    handle="af12f10f9ae2002a1607ba0b47ba8407",
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human"
            }
        """
        return self.query_engine.get_atom(handle)

    def get_node(self, node_type: str, node_name: str) -> Dict[str, Any]:
        """
        Retrieve information about a Node of a specific type and name.

        This method retrieves information about a Node from the database
        based on its type and name. The retrieved node information can be
        presented in different output formats as specified by the output_format parameter.

        Args:
            node_type (str): The type of the node being queried.
            node_name (str): The name of the specific node being queried.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the node (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing atom information of the node (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the node (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If the specified node does not exist, a warning is logged and None is returned.

        Example:
            >>> result = obj.get_node(
                    node_type='Concept',
                    node_name='human',
                    output_format=QueryOutputFormat.ATOM_INFO
                )
            >>> print(result)
            {
                "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                "type": "Concept",
                "name": "human"
            }
        """
        return self.query_engine.get_node(node_type, node_name)

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        """
        Retrieve information about a link of a specific type and its targets.

        This method retrieves information about a link from the database based on
        type with given targets. The retrieved link information can be presented in different
        output formats as specified by the output_format parameter.

        Args:
            link_type (str): The type of the link being queried.
            targets (List[str], optional): A list of target identifiers that the link is associated with.
                Defaults to None.
            output_format (QueryOutputFormat, optional): The desired output format.
                Defaults to QueryOutputFormat.HANDLE.

        Returns:
            Union[str, Dict]: Depending on the output_format, returns either:
                - A string representing the handle of the link (output_format == QueryOutputFormat.HANDLE),
                - A dictionary containing atom information of the link (output_format == QueryOutputFormat.ATOM_INFO),
                - A JSON-formatted string representing the deep representation of the link (output_format == QueryOutputFormat.JSON).

        Raises:
            ValueError: If an invalid output format is provided.

        Note:
            If the specified link or targets do not exist, the method returns None.

        Example:
            >>> result = obj.get_link(
                    link_type='Similarity',
                    targets=['human', 'monkey'],
                    output_format=QueryOutputFormat.HANDLE
                )
            >>> print(result)
            '2931276cb5bb4fc0c2c48a6720fc9a84'
        """
        return self.query_engine.get_link(link_type, link_targets)

    def count_atoms(self) -> Tuple[int, int]:
        """
        This method is useful for returning the count of atoms in the database.
        It's also useful for ensuring that the knowledge base load went off without problems.

        Returns:
            Tuple[int, int]: (node_count, link_count)
        """
        return self.query_engine.count_atoms()

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """
        Perform a query on the knowledge base using a dict as input. Returns a
        list of dicts as result.

        The input dict is a link, used as a pattern to make the query.
        Variables can be used as link targets as well as nodes. Nested links are
        allowed as well.

        Args:
            query (Dict[str, Any]): A pattern described as a link (possibly with nested links) with
            nodes and variables used to query the knoeledge base.
            extra_paramaters (Dict[str, Any], optional): query optional parameters

        Returns:
            List[Dict[str, Any]]: a list of dicts with the matching subgraphs

        Raises:
            UnexpectedQueryFormat: If query resolution lead to an invalid state

        Notes:
            - No logical connectors (AND, OR, NOT) are allowed
            - If no match is found for the query, an empty list is returned.

        Example:

            >>> das.add_link({
                "type": "Expression",
                "targets": [
                    {"type": "Symbol", "name": "Test"},
                    {
                        "type": "Expression",
                        "targets": [
                            {"type": "Symbol", "name": "Test"},
                            {"type": "Symbol", "name": "2"}
                        ]
                    }
                ]
            })
            >>> query_params = {
                "toplevel_only": False,
                "return_type": QueryOutputFormat.ATOM_INFO,
            }
            >>> q1 = {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {
                        "atom_type": "link",
                        "type": "Expression",
                        "targets": [
                            {"atom_type": "variable", "name": "v2"},
                            {"atom_type": "node", "type": "Symbol", "name": "2"},
                        ]
                    }
                ]
            }
            >>> result = das.query(q1, query_params)
            >>> print(result)
            [{'handle': 'dbcf1c7b610a5adea335bf08f6509978', 'type': 'Expression', 'template': ['Expression', 'Symbol', ['Expression', 'Symbol', 'Symbol']], 'targets': [{'handle': '963d66edfb77236054125e3eb866c8b5', 'type': 'Symbol', 'name': 'Test'}, {'handle': '233d9a6da7d49d4164d863569e9ab7b6', 'type': 'Expression', 'template': ['Expression', 'Symbol', 'Symbol'], 'targets': [{'handle': '963d66edfb77236054125e3eb866c8b5', 'type': 'Symbol', 'name': 'Test'}, {'handle': '9f27a331633c8bc3c49435ffabb9110e', 'type': 'Symbol', 'name': '2'}]}]}]
        """
        return self.query_engine.query(query, parameters)

    def commit_changes(self):
        self.query_engine.commit()

    def get_node_handle(self, node_type: str, node_name: str) -> str:
        return self.backend.node_handle(node_type, node_name)

    def get_link_handle(self, link_type: str, link_targets: List[str]) -> str:
        return self.backend.link_handle(link_type, link_targets)

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.backend.add_node(node_params)

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.backend.add_link(link_params)

    def clear(self) -> None:
        """Clear all data"""
        ret = self.backend.clear_database()
        logger().debug('The database has been cleaned.')
        return ret


class QueryEngine(ABC):
    @abstractmethod
    def get_atom(
        self,
        handle: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        ...

    @abstractmethod
    def get_node(
        self,
        node_type: str,
        node_name: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        ...

    @abstractmethod
    def get_link(
        self,
        link_type: str,
        targets: List[str],
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        ...

    @abstractmethod
    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        ...

    @abstractmethod
    def count_atoms(self) -> Tuple[int, int]:
        ...


class LocalQueryEngine(QueryEngine):
    def __init__(self, backend, kwargs: Optional[dict] = None) -> None:
        self.local_backend = backend

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
                atom_handle = self.db.get_node_handle(query["type"], query["name"])
                return ListIterator([QueryAnswer(self.db.get_atom_as_dict(atom_handle), None)])
            except NodeDoesNotExistException:
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

    def get_atom(self, handle: str) -> Union[Dict[str, Any], None]:
        try:
            return self.local_backend.get_atom(handle)
        except AtomDoesNotExist:
            return None

    def get_node(self, node_type: str, node_name: str) -> Union[Dict[str, Any], None]:
        try:
            return self.local_backend.get_node(node_type, node_name)
        except NodeDoesNotExist:
            return None

    def get_link(self, link_type: str, link_targets: List[str]) -> Union[Dict[str, Any], None]:
        try:
            return self.local_backend.get_link(link_type, link_targets)
        except LinkDoesNotExist:
            return None

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = None,
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
        local = self.local_query_engine.get_atom(handle)
        if not local:
            return self.remote_das.get_atom(handle)

    def get_node(self, node_type: str, node_name: str) -> Dict[str, Any]:
        local = self.local_query_engine.get_node(node_type, node_name)
        if not local:
            return self.remote_das.get_node(node_type, node_name)

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        local = self.local_query_engine.get_link(link_type, link_targets)
        if not local:
            return self.remote_das.get_link(link_type, link_targets)

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = None,
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
