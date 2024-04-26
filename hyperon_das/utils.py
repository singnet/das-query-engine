import pickle
from dataclasses import dataclass
from http import HTTPStatus  # noqa: F401
from importlib import import_module
from typing import Any, Dict, FrozenSet, List, Optional, Set, Tuple, Union

from requests import sessions
from requests.exceptions import (  # noqa: F401
    ConnectionError,
    HTTPError,
    JSONDecodeError,
    RequestException,
    Timeout,
)

from hyperon_das.decorators import retry
from hyperon_das.exceptions import InvalidAssignment
from hyperon_das.logger import logger


def das_error(exception: Exception):
    logger().error(str(exception))
    raise exception


class Assignment:
    @staticmethod
    def compose(components: List["Assignment"]) -> Optional["Assignment"]:
        answer = Assignment()
        for component in components:
            if not answer.merge(component):
                return None
        answer.freeze()
        return answer

    def __init__(self, other: Optional["Assignment"] = None):
        self.labels: Union[Set[str], FrozenSet] = set()
        self.values: Union[Set[str], FrozenSet] = set()
        self.hashcode: int = 0
        if other:
            self.mapping: Dict[str, str] = dict(other.mapping)
        else:
            self.mapping: Dict[str, str] = {}

    def __hash__(self) -> int:
        assert self.hashcode
        return self.hashcode

    def __eq__(self, other) -> bool:
        assert self.hashcode and other.hashcode
        return self.hashcode == other.hashcode

    def __lt__(self, other) -> bool:
        assert self.hashcode and other.hashcode
        return self.hashcode < other.hashcode

    def __repr__(self) -> str:
        return str([tuple([label, self.mapping[label]]) for label in sorted(self.labels)])

    def __str__(self) -> str:
        return self.__repr__()

    def frozen(self):
        return self.hashcode != 0

    def freeze(self) -> bool:
        if self.frozen():
            return False
        else:
            self.labels = frozenset(self.labels)
            self.values = frozenset(self.values)
            self.hashcode = hash(frozenset(self.mapping.items()))
            return True

    def assign(
        self,
        label: str,
        value: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if label is None or value is None or self.frozen():
            raise InvalidAssignment(
                message="Invalid assignment",
                details=f"label = {label} value = {value} hashcode = {self.hashcode}",
            )
        if label in self.labels:
            return self.mapping[label] == value
        else:
            if parameters and parameters['no_overload'] and value in self.values:
                return False
            self.labels.add(label)
            self.values.add(value)
            self.mapping[label] = value
            return True

    def merge(self, other: "Assignment", in_place: bool = True) -> Optional[bool]:
        if in_place:
            assert not self.frozen()
            if other:
                for label, value in other.mapping.items():
                    if not self.assign(label, value):
                        return False
            return True
        else:
            new_assignment = Assignment(self)
            if new_assignment.merge(other):
                new_assignment.freeze()
                return new_assignment
            else:
                return None


@dataclass
class QueryAnswer:
    subgraph: Optional[Dict] = None
    assignment: Optional[Assignment] = None

    def _recursive_get_handle_set(self, atom, handle_set):
        handle_set.add(atom['handle'])
        targets = atom.get('targets', None)
        if targets is not None:
            for target_atom in targets:
                self._recursive_get_handle_set(target_atom, handle_set)

    def _recursive_get_handle_count(self, atom, handle_count):
        key = atom['handle']
        total = handle_count.get(key, 0) + 1
        handle_count[key] = total
        targets = atom.get('targets', None)
        if targets is not None:
            for target_atom in targets:
                self._recursive_get_handle_count(target_atom, handle_count)

    def get_handle_set(self):
        handle_set = set()
        if self.subgraph is not None:
            self._recursive_get_handle_set(self.subgraph, handle_set)
        return handle_set

    def get_handle_count(self):
        handle_count = {}
        if self.subgraph is not None:
            self._recursive_get_handle_count(self.subgraph, handle_count)
        return handle_count


def get_package_version(package_name: str) -> str:
    package_module = import_module(package_name)
    return getattr(package_module, '__version__', None)


def serialize(payload: Any) -> bytes:
    return pickle.dumps(payload)


def deserialize(payload: bytes) -> Any:
    return pickle.loads(payload)


@retry(attempts=5, timeout_seconds=120)
def connect_to_server(host: str, port: int) -> Tuple[int, str]:
    """Connect to the server and return the status connection and the url server"""
    port = port or '8081'
    openfaas_uri = f'http://{host}:{port}/function/query-engine'
    aws_lambda_uri = f'http://{host}/prod/query-engine'

    for uri in [openfaas_uri, aws_lambda_uri]:
        status_code, message = check_server_connection(uri)
        if status_code == HTTPStatus.OK:
            break
        elif status_code == HTTPStatus.INTERNAL_SERVER_ERROR:
            raise Exception(message)

    return status_code, uri


def check_server_connection(url: str) -> Tuple[int, str]:
    logger().debug(f'connecting to remote Das {url}')
    error_msg = None
    try:
        das_version = get_package_version('hyperon_das')
        atom_db_version = get_package_version('hyperon_das_atomdb')

        with sessions.Session() as session:
            payload = {
                'action': 'handshake',
                'input': {
                    'das_version': das_version,
                    'atomdb_version': atom_db_version,
                },
            }
            response = session.request(
                method='POST',
                url=url,
                data=serialize(payload),
                headers={'Content-Type': 'application/octet-stream'},
                timeout=10,
            )
        if response.status_code == HTTPStatus.CONFLICT:
            try:
                response = deserialize(response.content)
                remote_das_version = response.get('das').get('version')
                remote_atomdb_version = response.get('atom_db').get('version')
            except JSONDecodeError as e:
                raise Exception(str(e))
            logger().error(
                f"Package version conflict error when connecting to remote DAS. Local DAS: 'das: {das_version} - atom_db: {atom_db_version}' -- Remote DAS: 'das: {remote_das_version} - atom_db: {remote_atomdb_version}'"
            )
            raise Exception(
                f"Local DAS version 'das: {das_version} and atom_db: {atom_db_version}', DAS server is expecting 'das: {remote_das_version} and atom_db: {remote_atomdb_version}'"
            )
        if response.status_code == HTTPStatus.OK:
            return response.status_code, "Successful connection"
        else:
            try:
                error_msg = deserialize(response.content).get('error')
                response.raise_for_status()
            except pickle.UnpicklingError:
                raise Exception("Error unpickling objects in peer's response")
    except (ConnectionError, Timeout, HTTPError, RequestException) as e:
        msg = f"{error_msg} - {str(e)}" if error_msg else str(e)
        return 400, msg
    except Exception as e:
        return 500, str(e)
