import json
from unittest.mock import patch

import pytest
from requests import exceptions

from hyperon_das.client import FunctionsClient
from hyperon_das.exceptions import ConnectionError, RequestError, TimeoutError
from hyperon_das.utils import deserializer, serializer


class TestFunctionsClient:
    @pytest.fixture
    def mock_request(self):
        with patch('requests.sessions.Session.request') as mock_request:
            yield mock_request

    def test_get_atom_success(self, mock_request):
        expected_request_data = {"action": "get_atom", "input": {"handle": "123"}}
        expected_response = {
            "handle": "af12f10f9ae2002a1607ba0b47ba8407",
            "composite_type_hash": "d99a604c79ce3c2e76a2f43488d5d4c3",
            "name": "human",
            "named_type": "Concept",
        }

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        result = client.get_atom(handle='123')

        mock_request.assert_called_with(
            method='POST',
            url='http://example.com',
            data=serializer(expected_request_data),
        )

        assert result == expected_response

    def test_get_node_success(self, mock_request):
        expected_request_data = {
            "action": "get_node",
            "input": {"node_type": "Concept", "node_name": "human"},
        }
        expected_response = {
            "handle": "af12f10f9ae2002a1607ba0b47ba8407",
            "composite_type_hash": "d99a604c79ce3c2e76a2f43488d5d4c3",
            "name": "human",
            "named_type": "Concept",
        }

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        result = client.get_node(node_type='Concept', node_name='human')

        mock_request.assert_called_with(
            method='POST',
            url='http://example.com',
            data=serializer(expected_request_data),
        )

        assert result == expected_response

    def test_get_link_success(self, mock_request):
        expected_request_data = {
            "action": "get_link",
            "input": {
                "link_type": "Similarity",
                "link_targets": [
                    "af12f10f9ae2002a1607ba0b47ba8407",
                    "1cdffc6b0b89ff41d68bec237481d1e1",
                ],
            },
        }
        expected_response = {
            "handle": "bad7472f41a0e7d601ca294eb4607c3a",
            "composite_type_hash": "ed73ea081d170e1d89fc950820ce1cee",
            "is_toplevel": True,
            "composite_type": [
                "a9dea78180588431ec64d6bc4872fdbc",
                "d99a604c79ce3c2e76a2f43488d5d4c3",
                "d99a604c79ce3c2e76a2f43488d5d4c3",
            ],
            "named_type": "Similarity",
            "named_type_hash": "a9dea78180588431ec64d6bc4872fdbc",
            "targets": ["af12f10f9ae2002a1607ba0b47ba8407", "1cdffc6b0b89ff41d68bec237481d1e1"],
        }

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        result = client.get_link(
            link_type='Similarity',
            link_targets=['af12f10f9ae2002a1607ba0b47ba8407', '1cdffc6b0b89ff41d68bec237481d1e1'],
        )

        mock_request.assert_called_with(
            method='POST',
            url='http://example.com',
            data=serializer(expected_request_data),
        )

        assert result == expected_response

    def test_get_links_success(self, mock_request):
        expected_request_data = {
            "action": "get_links",
            "input": {
                "link_type": "Inheritance",
                "kwargs": {},
                "link_targets": [
                    "4e8e26e3276af8a5c2ac2cc2dc95c6d2",
                    "80aff30094874e75028033a38ce677bb",
                ],
            },
        }
        expected_response = [
            {
                "handle": "ee1c03e6d1f104ccd811cfbba018451a",
                "type": "Inheritance",
                "template": ["Inheritance", "Concept", "Concept"],
                "targets": ["4e8e26e3276af8a5c2ac2cc2dc95c6d2", "80aff30094874e75028033a38ce677bb"],
            }
        ]

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        result = client.get_links(
            link_type='Inheritance',
            link_targets=['4e8e26e3276af8a5c2ac2cc2dc95c6d2', '80aff30094874e75028033a38ce677bb'],
        )

        mock_request.assert_called_with(
            method='POST',
            url='http://example.com',
            data=serializer(expected_request_data),
        )

        assert result == expected_response

    def test_query_success(self, mock_request):
        expected_request_data = {
            "action": "query",
            "input": {
                "query": {
                    "atom_type": "link",
                    "type": "Similarity",
                    "targets": [
                        {"atom_type": "node", "type": "Concept", "name": "human"},
                        {"atom_type": "node", "type": "Concept", "name": "monkey"},
                    ],
                },
                "parameters": [],
            },
        }
        expected_response = [
            {
                "handle": "bad7472f41a0e7d601ca294eb4607c3a",
                "type": "Similarity",
                "template": ["Similarity", "Concept", "Concept"],
                "targets": [
                    {
                        "handle": "af12f10f9ae2002a1607ba0b47ba8407",
                        "type": "Concept",
                        "name": "human",
                    },
                    {
                        "handle": "1cdffc6b0b89ff41d68bec237481d1e1",
                        "type": "Concept",
                        "name": "monkey",
                    },
                ],
            }
        ]

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        query = {
            "atom_type": "link",
            "type": "Similarity",
            "targets": [
                {"atom_type": "node", "type": "Concept", "name": "human"},
                {"atom_type": "node", "type": "Concept", "name": "monkey"},
            ],
        }

        result = client.query(query, parameters=[])

        mock_request.assert_called_with(
            method='POST',
            url='http://example.com',
            data=serializer(expected_request_data),
        )

        assert result == expected_response

    def test_count_atoms_success(self, mock_request):
        expected_request_data = {"action": "count_atoms", "input": {}}
        expected_response = (14, 26)

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        result = client.count_atoms()

        mock_request.assert_called_once_with(
            method='POST',
            url='http://example.com',
            data=serializer(expected_request_data),
        )

        assert result == expected_response

    def test_send_request_success(self, mock_request):
        payload = {"action": "get_atom", "input": {"handle": "123"}}
        expected_response = {
            "handle": "af12f10f9ae2002a1607ba0b47ba8407",
            "composite_type_hash": "d99a604c79ce3c2e76a2f43488d5d4c3",
            "name": "human",
            "named_type": "Concept",
        }

        mock_request.return_value.status_code = 200
        mock_request.return_value.content = serializer(expected_response)

        client = FunctionsClient(url='http://example.com')
        result = client._send_request(payload)

        mock_request.assert_called_with(
            method='POST',
            url='http://example.com',
            data=serializer(payload),
        )

        assert result == expected_response

    def test_send_request_connection_error(self, mock_request):
        mock_request.side_effect = exceptions.ConnectionError()

        client = FunctionsClient(url='http://example.com')
        payload = {"action": "get_atom", "input": {"handle": "123"}}

        with pytest.raises(ConnectionError):
            client._send_request(payload)

    def test_send_request_timeout_error(self, mock_request):
        mock_request.side_effect = exceptions.Timeout()

        client = FunctionsClient(url='http://example.com')
        payload = {"action": "get_atom", "input": {"handle": "123"}}

        with pytest.raises(TimeoutError):
            client._send_request(payload)

    def test_send_request_request_exception(self, mock_request):
        mock_request.side_effect = exceptions.RequestException()

        client = FunctionsClient(url='http://example.com')
        payload = {"action": "get_atom", "input": {"handle": "123"}}

        with pytest.raises(RequestError):
            client._send_request(payload)
