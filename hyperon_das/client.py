import contextlib
import pickle
from typing import Any

from hyperon_das_atomdb import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist
from requests import exceptions, sessions

from hyperon_das.exceptions import (
    FunctionsConnectionError,
    FunctionsTimeoutError,
    HTTPError,
    RequestError,
)
from hyperon_das.logger import logger
from hyperon_das.type_alias import Query
from hyperon_das.utils import connect_to_server, das_error, deserialize, serialize


class FunctionsClient:
    def __init__(self, host: str, port: int, name: str | None = None) -> None:
        if not host and not port:
            das_error(ValueError("'host' and 'port' are mandatory parameters"))
        self.name = name if name else f'client_{host}:{port}'
        self.url = connect_to_server(host, port)

    def _send_request(self, payload) -> Any:
        try:
            if payload.get('input'):
                normalized_input = {k: v for k, v in payload['input'].items() if v is not None}
                payload['input'] = normalized_input

            payload_serialized = serialize(payload)

            with sessions.Session() as session:
                response = session.request(
                    method='POST',
                    url=self.url,
                    data=payload_serialized,
                    headers={'Content-Type': 'application/octet-stream'},
                )

            response.raise_for_status()

            try:
                response_data = deserialize(response.content)
            except pickle.UnpicklingError as e:
                das_error(Exception(f"Unpickling error: {str(e)}"))

            if response.status_code == 200:
                return response_data
            else:
                return response_data.get(
                    'error', f'Unknown error with status code {response.status_code}'
                )
        except exceptions.ConnectionError as e:
            das_error(
                FunctionsConnectionError(
                    message=f"Connection error for URL: '{self.url}' with payload: '{payload}'",
                    details=str(e),
                )
            )
        except exceptions.Timeout as e:
            das_error(
                FunctionsTimeoutError(
                    message=f"Request timed out for URL: '{self.url}' with payload: '{payload}'",
                    details=str(e),
                )
            )
        except exceptions.HTTPError as e:
            with contextlib.suppress(pickle.UnpicklingError):
                message = deserialize(response.content)
                das_error(
                    HTTPError(
                        message="Please, check if your request payload is correctly formatted.",
                        details=message,
                        status_code=e.response.status_code,
                    )
                )
        except exceptions.RequestException as e:
            das_error(
                RequestError(
                    message=f"Request exception for URL: '{self.url}' with payload: '{payload}'.",
                    details=str(e),
                )
            )

    def get_atom(self, handle: str, **kwargs) -> str | dict:
        payload = {
            'action': 'get_atom',
            'input': {'handle': handle},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 404:
                raise AtomDoesNotExist(message='Atom Nonexistent')
            else:
                raise e

    def get_node(self, node_type: str, node_name: str) -> str | dict:
        payload = {
            'action': 'get_node',
            'input': {'node_type': node_type, 'node_name': node_name},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 404:
                raise NodeDoesNotExist(message='Node Nonexistent')
            else:
                raise e

    def get_link(self, link_type: str, link_targets: list[str]) -> dict[str, Any]:
        payload = {
            'action': 'get_link',
            'input': {'link_type': link_type, 'link_targets': link_targets},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 404:
                raise LinkDoesNotExist(message='Link Nonexistent')
            else:
                raise e

    def get_links(
        self,
        link_type: str,
        target_types: list[str] | None = None,
        link_targets: list[str] | None = None,
        **kwargs,
    ) -> list[str] | list[dict]:
        payload = {
            'action': 'get_links',
            'input': {'link_type': link_type, 'kwargs': kwargs},
        }
        if target_types:
            payload['input']['target_types'] = target_types

        if link_targets:
            payload['input']['link_targets'] = link_targets
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 404:
                raise AtomDoesNotExist(message='Atom Nonexistent')
            elif e.status_code == 400:
                raise ValueError(str(e))
            else:
                raise e

    def query(
        self,
        query: dict[str, Any],
        parameters: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        try:
            payload = {
                'action': 'query',
                'input': {'query': query, 'parameters': parameters},
            }
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 400:
                raise ValueError(
                    "Your query couldn't be processed due to an invalid format. Review the way the query "
                    "is written and try again.",
                    str(e),
                )
            elif e.status_code == 404:
                raise Exception("Your query couldn't be processed because Atom nonexistent", str(e))
            raise e

    def count_atoms(self) -> tuple[int, int]:
        payload = {
            'action': 'count_atoms',
            'input': {},
        }
        return self._send_request(payload)

    def commit_changes(self, **kwargs) -> tuple[int, int]:
        payload = {
            'action': 'commit_changes',
            'input': {'kwargs': kwargs},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 403:
                raise ValueError(str(e))
            else:
                raise e

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> list[dict | str | tuple[dict, list[dict]]]:
        payload = {
            'action': 'get_incoming_links',
            'input': {'atom_handle': atom_handle, 'kwargs': kwargs},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            logger().debug(f'Error during `get_incoming_links` request on remote Das: {str(e)}')
            return None, [] if kwargs.get('cursor') is not None else []  # TODO:(angelo) check this

    def create_field_index(
        self,
        atom_type: str,
        fields: list[str],
        named_type: str | None = None,
        composite_type: list[Any] | None = None,
        index_type: str | None = None,
    ) -> str:
        payload = {
            'action': 'create_field_index',
            'input': {
                'atom_type': atom_type,
                'fields': fields,
                'named_type': named_type,
                'composite_type': composite_type,
                'index_type': index_type,
            },
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 400:
                raise ValueError(str(e))
            else:
                raise e

    def custom_query(self, index_id: str, query: Query, **kwargs) -> list[dict[str, Any]]:
        payload = {
            'action': 'custom_query',
            'input': {'index_id': index_id, 'query': query, 'kwargs': kwargs},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            raise e

    def fetch(
        self,
        query: list[dict] | dict,
        host: str | None = None,
        port: int | None = None,
        **kwargs,
    ) -> Any:
        payload = {
            'action': 'fetch',
            'input': {'query': query, 'host': host, 'port': port, 'kwargs': kwargs},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            raise e

    def create_context(self, name: str, queries: list[Query] | None) -> Any:
        payload = {
            'action': 'create_context',
            'input': {'name': name, 'queries': queries},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 404:
                raise AtomDoesNotExist('Atom nonexistent')
            elif e.status_code == 400:
                raise ValueError(str(e))
            else:
                raise e

    def get_atoms_by_field(self, query: Query) -> list[str]:
        payload = {
            'action': 'get_atoms_by_field',
            'input': {'query': {v['field']: v['value'] for v in query}},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 400:
                raise ValueError(str(e))
            else:
                raise e

    def get_atoms_by_text_field(
        self, text_value: str, field: str | None = None, text_index_id: str | None = None
    ) -> list[str]:
        payload = {
            'action': 'get_atoms_by_text_field',
            'input': {'text_value': text_value, 'field': field, 'text_index_id': text_index_id},
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 400:
                raise ValueError(str(e))
            else:
                raise e

    def get_node_by_name_starting_with(self, node_type: str, startswith: str) -> list[str]:
        payload = {
            'action': 'get_node_by_name_starting_with',
            'input': {
                'node_type': node_type,
                'startswith': startswith,
            },
        }
        try:
            return self._send_request(payload)
        except HTTPError as e:
            if e.status_code == 400:
                raise ValueError(str(e))
            else:
                raise e
