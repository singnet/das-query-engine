import json
from typing import Any, Dict, List, Optional, Tuple, Union

import requests

from hyperon_das.constants import QueryOutputFormat
from hyperon_das.pattern_matcher.pattern_matcher import LogicalExpression


class FunctionsClient:
    def __init__(
        self, url: str, numbers_servers: int = 0, name: Optional[str] = None
    ):
        if not name:
            self.name = f'server-{numbers_servers}'
        self.url = url

    def _send_request(self, payload) -> str | dict | int:
        try:
            response = requests.request(
                'POST', url=self.url, data=json.dumps(payload)
            )
            if response.status_code == 200:
                text = response.text.rstrip('\n')
                try:
                    ret = eval(text)
                except Exception:
                    ret = text
                return ret
            else:
                return response.text
        except requests.exceptions.RequestException as e:
            raise e

    def get_node(
        self,
        node_type: str,
        node_name: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        payload = {
            'action': 'get_node',
            'input': {
                'node_type': node_type,
                'node_name': node_name,
                'output_format': output_format.name,
            },
        }
        return self._send_request(payload)

    def get_nodes(
        self,
        node_type: str,
        node_name: str = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[List[str], List[Dict]]:
        payload = {
            'action': 'get_nodes',
            'input': {
                'node_type': node_type,
                'node_name': node_name,
                'output_format': output_format.name,
            },
        }
        return self._send_request(payload)

    def get_node_type(self, node_handle: str) -> str:
        payload = {
            'action': 'get_node_type',
            'input': {'node_handle': node_handle},
        }
        return self._send_request(payload)

    def get_node_name(self, node_handle: str) -> str:
        payload = {
            'action': 'get_node_name',
            'input': {'node_handle': node_handle},
        }
        return self._send_request(payload)

    def get_link(
        self,
        link_type: str,
        targets: List[str],
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        payload = {
            'action': 'get_link',
            'input': {
                'link_type': link_type,
                'targets': targets,
                'output_format': output_format.name,
            },
        }
        return self._send_request(payload)

    def get_links(
        self,
        link_type: str,
        target_types: str = None,
        targets: List[str] = None,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[List[str], List[Dict]]:
        payload = {
            'action': 'get_links',
            'input': {
                'link_type': link_type,
                'output_format': output_format.name,
            },
        }
        if target_types is not None:
            payload['input']['target_types'] = target_types
        if targets is not None:
            payload['input']['targets'] = targets
        return self._send_request(payload)

    def get_link_type(self, link_handle: str) -> str:
        payload = {
            'action': 'get_link_type',
            'input': {'link_handle': link_handle},
        }
        return self._send_request(payload)

    def get_link_targets(self, link_handle: str) -> List[str]:
        payload = {
            'action': 'get_link_targets',
            'input': {'link_handle': link_handle},
        }
        return self._send_request(payload)

    def get_atom(
        self,
        handle: str,
        output_format: QueryOutputFormat = QueryOutputFormat.HANDLE,
    ) -> Union[str, Dict]:
        payload = {
            'action': 'get_atom',
            'input': {'handle': handle, 'output_format': output_format.name},
        }
        return self._send_request(payload)

    def count_atoms(self) -> Tuple[int, int]:
        payload = {
            'action': 'count_atoms',
            'input': {},
        }
        return self._send_request(payload)

    def clear_database(self) -> None:
        payload = {
            'action': 'clear_database',
            'input': {},
        }
        return self._send_request(payload)

    def query(
        self,
        query: Dict[str, Any],
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        """"""
        payload = {
            'action': 'query',
            'input': {'query': query, 'extra_parameters': extra_parameters},
        }
        return self._send_request(payload)

    def pattern_matcher_query(
        self,
        query: LogicalExpression,
        extra_parameters: Optional[Dict[str, Any]] = None,
    ) -> dict | list | None:
        payload = {
            'action': 'pattern_matcher_query',
            'input': {'query': query, 'extra_parameters': extra_parameters},
        }
        return self._send_request(payload)
