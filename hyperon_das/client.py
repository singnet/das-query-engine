import contextlib
import pickle
from typing import Any, Dict, List, Optional, Tuple, Union

from hyperon_das_atomdb import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist
from requests import exceptions, sessions

from hyperon_das.exceptions import ConnectionError, HTTPError, RequestError, TimeoutError
from hyperon_das.logger import logger
from hyperon_das.utils import deserialize, serialize


class FunctionsClient:
    def __init__(self, url: str, server_count: int = 0, name: Optional[str] = None):
        if not name:
            self.name = f'server-{server_count}'
        self.url = url

    def _send_request(self, payload) -> Any:
        try:
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
                raise Exception(f"Unpickling error: {str(e)}")

            if response.status_code == 200:
                return response_data
            else:
                return response_data.get(
                    'error', f'Unknown error with status code {response.status_code}'
                )
        except exceptions.ConnectionError as e:
            raise ConnectionError(
                message=f"Connection error for URL: '{self.url}' with payload: '{payload}'",
                details=str(e),
            )
        except exceptions.Timeout as e:
            raise TimeoutError(
                message=f"Request timed out for URL: '{self.url}' with payload: '{payload}'",
                details=str(e),
            )
        except exceptions.HTTPError as e:
            with contextlib.suppress(pickle.UnpicklingError):
                return deserialize(response.content).get('error')
            raise HTTPError(
                message=f"HTTP error occurred for URL: '{self.url}' with payload: '{payload}'",
                details=str(e),
            )
        except exceptions.RequestException as e:
            raise RequestError(
                message=f"Request exception occurred for URL: '{self.url}' with payload: '{payload}'.",
                details=str(e),
            )

    def get_atom(self, handle: str, **kwargs) -> Union[str, Dict]:
        payload = {
            'action': 'get_atom',
            'input': {'handle': handle},
        }
        response = self._send_request(payload)
        if 'not exist' in response:
            raise AtomDoesNotExist('error')
        return response

    def get_node(self, node_type: str, node_name: str) -> Union[str, Dict]:
        payload = {
            'action': 'get_node',
            'input': {'node_type': node_type, 'node_name': node_name},
        }
        response = self._send_request(payload)
        if 'not exist' in response:
            raise NodeDoesNotExist('error')
        return response

    def get_link(self, link_type: str, link_targets: List[str]) -> Dict[str, Any]:
        payload = {
            'action': 'get_link',
            'input': {'link_type': link_type, 'link_targets': link_targets},
        }
        response = self._send_request(payload)
        if 'not exist' in response:
            raise LinkDoesNotExist('error')
        return response

    def get_links(
        self,
        link_type: str,
        target_types: List[str] = None,
        link_targets: List[str] = None,
        **kwargs,
    ) -> Union[List[str], List[Dict]]:
        payload = {
            'action': 'get_links',
            'input': {'link_type': link_type, 'kwargs': kwargs},
        }
        if target_types:
            payload['input']['target_types'] = target_types

        if link_targets:
            payload['input']['link_targets'] = link_targets

        return self._send_request(payload)

    def query(
        self,
        query: Dict[str, Any],
        parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        payload = {
            'action': 'query',
            'input': {'query': query, 'parameters': parameters},
        }
        return self._send_request(payload)

    def count_atoms(self) -> Tuple[int, int]:
        payload = {
            'action': 'count_atoms',
            'input': {},
        }
        return self._send_request(payload)

    def commit_changes(self) -> Tuple[int, int]:
        payload = {
            'action': 'commit_changes',
            'input': {},
        }
        return self._send_request(payload)

    def get_incoming_links(
        self, atom_handle: str, **kwargs
    ) -> List[Union[dict, str, Tuple[dict, List[dict]]]]:
        payload = {
            'action': 'get_incoming_links',
            'input': {'atom_handle': atom_handle, 'kwargs': kwargs},
        }
        response = self._send_request(payload)
        if response and 'error' in response:
            logger().debug(
                f'Error during `get_incoming_links` request on remote Das: {response["error"]}'
            )
            return None, [] if kwargs.get('cursor') is not None else []
        return response

    def create_field_index(
        self,
        atom_type: str,
        field: str,
        type: Optional[str] = None,
        composite_type: Optional[List[Any]] = None,
    ) -> str:
        payload = {
            'action': 'create_field_index',
            'input': {
                'atom_type': atom_type,
                'field': field,
                'type': type,
                'composite_type': composite_type,
            },
        }
        return self._send_request(payload)

    def custom_query(self, index_id: str, **kwargs) -> List[Dict[str, Any]]:
        payload = {
            'action': 'custom_query',
            'input': {'index_id': index_id, 'kwargs': kwargs},
        }
        return self._send_request(payload)
