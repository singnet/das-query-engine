import json
from abc import ABC
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import requests
from hyperon_das_atomdb import WILDCARD
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB

from hyperon_das.cache import AndEvaluator, LazyQueryEvaluator, ListIterator, QueryAnswerIterator
from hyperon_das.client import FunctionsClient
from hyperon_das.constants import QueryOutputFormat
from hyperon_das.decorators import retry
from hyperon_das.exceptions import (
    ConnectionServerException,
    MethodNotAllowed,
    QueryParametersException,
    UnexpectedQueryFormat,
)
from hyperon_das.logger import logger
from hyperon_das.pattern_matcher.pattern_matcher import LogicalExpression, PatternMatchingAnswer
from hyperon_das.utils import Assignment, QueryAnswer, QueryParameters, config


class BaseDas(ABC):
    def _to_handle_list(
        self, atom_list: Union[List[str], List[Dict]]
    ) -> List[str]:
        if not atom_list:
            return []
        if isinstance(atom_list[0], str):
            return atom_list
        else:
            return [handle for handle, _ in atom_list]

    def _to_link_dict_list(
        self, db_answer: Union[List[str], List[Dict]]
    ) -> List[Dict]:
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
            answer.append(self.db.get_atom_as_dict(handle, arity))
        return answer

    def _to_json(self, db_answer: Union[List[str], List[Dict]]) -> List[Dict]:
        answer = []
        if db_answer:
            flat_handle = isinstance(db_answer[0], str)
            for atom in db_answer:
                if flat_handle:
                    handle = atom
                    arity = -1
                else:
                    handle, targets = atom
                    arity = len(targets)
                answer.append(
                    self.db.get_atom_as_deep_representation(handle, arity)
                )
        return json.dumps(answer, sort_keys=False, indent=4)

    def _turn_into_deep_representation(self, assignments) -> list:
        results = []
        for assignment in assignments:
            result = {}
            for variable, handle in assignment.mapping.items():
                deep_representation = self.db.get_atom_as_deep_representation(handle)
                is_link = 'targets' in deep_representation
                result[variable] = {
                    **deep_representation,
                    'atom_type': 'link' if is_link else 'node',
                }
            results.append(result)
        return results

    def _error(self, exception: Exception):
        logger().error(str(exception))
        raise exception

    def _recursive_query(
        self,
        query: Union[Dict[str, Any], List[Dict[str, Any]]],
        mappings: Set[Assignment] = None,
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> QueryAnswerIterator:
        if isinstance(query, list):
            sub_expression_results = [
                self._recursive_query(expression, mappings, extra_parameters)
                for expression in query
            ]
            return AndEvaluator(sub_expression_results)
        elif query["atom_type"] == "node":
            atom_handle = self.db.get_node_handle(query["type"], query["name"])
            return ListIterator([QueryAnswer(subgraph=self.db.get_atom_as_dict(atom_handle))])
        elif query["atom_type"] == "link":
            matched_targets = []
            for target in query["targets"]:
                if target["atom_type"] == "node" or target["atom_type"] == "link":
                    matched = self._recursive_query(target, mappings, extra_parameters)
                    if matched:
                        matched_targets.append(matched)
                elif target["atom_type"] == "variable":
                    matched_targets.append(ListIterator([QueryAnswer(subgraph=target)]))
                else:
                    self._error(
                        UnexpectedQueryFormat(
                            message="Query processing reached an unexpected state",
                            details=f'link: {str(query)} link target: {str(query)}',
                        )
                    )
            return LazyQueryEvaluator(query["type"], matched_targets, self, extra_parameters)
        else:
            self._error(
                UnexpectedQueryFormat(
                    message="Query processing reached an unexpected state",
                    details=f'query: {str(query)}',
                )
            )

    def clear_database(self) -> None:
        ret = self.db.clear_database()
        logger().debug('The database has been cleaned.')
        return ret

    def count_atoms(self) -> Tuple[int, int]:
        return self.db.count_atoms()

    def get_atom(
        self,
        handle: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        if output_format == QueryOutputFormat.HANDLE or not handle:
            atom = self.db.get_atom_as_dict(handle)
            return atom["handle"] if atom else ""
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            self._error(ValueError(f"Invalid output format: '{output_format}'"))

    def get_node(
        self,
        node_type: str,
        node_name: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        node_handle = None

        try:
            node_handle = self.db.get_node_handle(node_type, node_name)
        except ValueError:
            logger().warning(f"Attempt to access an invalid Node '{node_type}:{node_name}'")
            return None

        if output_format == QueryOutputFormat.HANDLE or not node_handle:
            return node_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(node_handle)
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(node_handle)
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            self._error(ValueError(f"Invalid output format: '{output_format}'"))

    def get_nodes(
        self,
        node_type: str,
        node_name: str = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[List[str], List[Dict]]:
        if node_name is not None:
            answer = self.db.get_node_handle(node_type, node_name)
            if answer is not None:
                answer = [answer]
        else:
            answer = self.db.get_all_nodes(node_type)

        if output_format == QueryOutputFormat.HANDLE or not answer:
            return answer
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return [self.db.get_atom_as_dict(handle) for handle in answer]
        elif output_format == QueryOutputFormat.JSON:
            answer = [self.db.get_atom_as_deep_representation(handle) for handle in answer]
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            self._error(ValueError(f"Invalid output format: '{output_format}'"))

    def get_link(
        self,
        link_type: str,
        targets: List[str],
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        link_handle = None

        # TODO: Is there any exception action?
        try:
            link_handle = self.db.get_link_handle(link_type, targets)
        except Exception as e:
            self._error(e)

        if output_format == QueryOutputFormat.HANDLE or link_handle is None:
            return link_handle
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self.db.get_atom_as_dict(link_handle, len(targets))
        elif output_format == QueryOutputFormat.JSON:
            answer = self.db.get_atom_as_deep_representation(link_handle, len(targets))
            return json.dumps(answer, sort_keys=False, indent=4)
        else:
            self._error(ValueError(f"Invalid output format: '{output_format}'"))

    def get_links(
        self,
        link_type: str,
        target_types: str = None,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[List[str], List[Dict]]:
        # TODO: Delete this If. This conditional will never happen
        if link_type is None:
            link_type = WILDCARD

        if target_types is not None and link_type != WILDCARD:
            db_answer = self.db.get_matched_type_template([link_type, *target_types])
        elif targets is not None:
            db_answer = self.db.get_matched_links(link_type, targets)
        elif link_type != WILDCARD:
            db_answer = self.db.get_matched_type(link_type)
        else:
            # TODO: Improve this message error. What is invalid?
            self._error(ValueError("Invalid parameters"))

        if output_format == QueryOutputFormat.HANDLE:
            return self._to_handle_list(db_answer)
        elif output_format == QueryOutputFormat.ATOM_INFO:
            return self._to_link_dict_list(db_answer)
        elif output_format == QueryOutputFormat.JSON:
            return self._to_json(db_answer)
        else:
            self.error(ValueError(f"Invalid output format: '{output_format}'"))

    def get_link_type(self, link_handle: str) -> str:
        try:
            resp = self.db.get_link_type(link_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            self._error(e)

    def get_link_targets(self, link_handle: str) -> List[str]:
        try:
            resp = self.db.get_link_targets(link_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            self._error(e)

    def get_node_type(self, node_handle: str) -> str:
        try:
            resp = self.db.get_node_type(node_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            self._error(e)

    def get_node_name(self, node_handle: str) -> str:
        try:
            resp = self.db.get_node_name(node_handle)
            return resp
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            self._error(e)

    def query(
        self,
        query: Dict[str, Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        logger().debug(
            {
                'message': '[DistributedAtomSpace][query] - Start',
                'data': {'query': query, 'extra_parameters': extra_parameters},
            }
        )
        try:
            return self.das.query(query, extra_parameters)
        # TODO: Find out what specific exceptions might happen
        except Exception as e:
            self._error(e)

    def pattern_matcher_query(
        self,
        query: LogicalExpression,
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> dict | list | None:
        logger().debug(
            {
                'message': '[DistributedAtomSpace][pattern_matcher_query] - Start',
                'data': {'query': query, 'extra_parameters': extra_parameters},
            }
        )

        if extra_parameters is not None:
            try:
                extra_parameters = QueryParameters(**extra_parameters)
            except TypeError as e:
                raise QueryParametersException(
                    message=str(e),
                    details=f'possible values {QueryParameters.values()}',
                )
        else:
            extra_parameters = QueryParameters()

        query_answer = PatternMatchingAnswer()

        matched = query.matched(self.db, query_answer, extra_parameters.__dict__)

        if not matched:
            return None

        if extra_parameters.return_type == QueryOutputFormat.HANDLE:
            result = list(query_answer.assignments)
        elif extra_parameters.return_type == QueryOutputFormat.ATOM_INFO:
            result = self._turn_into_deep_representation(query_answer.assignments)
        elif extra_parameters.return_type == QueryOutputFormat.JSON:
            objs = self._turn_into_deep_representation(query_answer.assignments)
            result = json.dumps(
                objs,
                sort_keys=False,
                indent=4,
            )
        else:
            raise ValueError(f"Invalid output format: '{extra_parameters.return_type}'")

        if query_answer.negation:
            return {'negation': True, 'mapping': result}
        else:
            return {'negation': False, 'mapping': result}


class DistributedAtomSpaceClient(BaseDas):
    def __init__(self, kwargs):
        self._remote_das = []
        self.db = InMemoryDB()
        self.das_origin = None

        params = {}
        if 'host' in kwargs:
            params = {'host': kwargs['host']}
        if 'port' in kwargs:
            params.update({'port': kwargs['port']})
        if params:
            try:
                url = self._connect_server(**params)
                # MARCO: Change this name. Using for tests
                self.das_origin = FunctionsClient(url)
            except Exception as e:
                raise ConnectionServerException(
                    message="An error occurs while connecting to the server",
                    details=str(e),
                )

    @property
    def remote_das(self):
        return self._remote_das

    @retry(attempts=5, timeout_seconds=120)
    def _connect_server(self, host: str, port: Optional[str] = None):
        port = port or config.get("DEFAULT_PORT_OPENFAAS", '8081')
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

    def attach_remote(
        self, host: str, port: Optional[str] = None, name: Optional[str] = None
    ) -> bool:
        try:
            url = self._connect_server(host, port)
            existing_servers = len(self.remote_das)
            das = FunctionsClient(url, existing_servers, name)
            self._remote_das.append(das)
            return True
        except Exception:
            return False

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.db.add_node(node_params)

    def add_link(self, link_params: Dict[str, Any]) -> Dict[str, Any]:
        return self.db.add_link(link_params)

    def query(
        self,
        query: Dict[str, Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        query_results = self._recursive_query(query, extra_parameters)
        logger().debug(f"query-client: {query} result: {str(query_results)}")

        answer = []
        
        for result in query_results:
            print('\n***********Handles************\n')
            print(result.handles)
            print('\n***********Graph************\n')
            print(result.subgraph)
            answer.append(result.subgraph)

        if self.das_origin:
            remote_answer = self.das_origin.query(query, extra_parameters)
        
        return answer


class DistributedAtomSpaceServer(BaseDas):
    def __init__(self, kwargs):
        self.db = RedisMongoDB()

    def add_node(self, node_params: Dict[str, Any]) -> Dict[str, Any]:
        self._error(
            MethodNotAllowed(
                message='This method is permited only in memory database',
                details='Instantiate the class sent the database type as `ram_only`',
            )
        )

    def add_link(self):
        self._error(
            MethodNotAllowed(
                message='This method is permited only in memory database',
                details='Instantiate the class sent the database type as `ram_only`',
            )
        )

    def query(
        self,
        das,
        query: Dict[str, Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        query_results = das._recursive_query(query, extra_parameters)
        logger().debug(f"query-server: {query} result: {str(query_results)}")
        answer = [result.subgraph for result in query_results]
        return answer
