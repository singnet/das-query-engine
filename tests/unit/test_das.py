from unittest import mock

import pytest
from hyperon_das_atomdb.adapters import InMemoryDB
from hyperon_das_atomdb.exceptions import InvalidAtomDB
import copy

from hyperon_das.cache import RemoteIncomingLinks
from hyperon_das.das import DistributedAtomSpace, LocalQueryEngine, RemoteQueryEngine
from hyperon_das.exceptions import GetTraversalCursorException, InvalidQueryEngine
from hyperon_das.traverse_engines import TraverseEngine

from .mock import DistributedAtomSpaceMock


class TestDistributedAtomSpace:
    def test_create_das(self):
        das = DistributedAtomSpace()
        assert isinstance(das.backend, InMemoryDB)
        assert isinstance(das.query_engine, LocalQueryEngine)

        with mock.patch('hyperon_das.utils.check_server_connection', return_value=(200, 'OK')):
            das = DistributedAtomSpace(query_engine='remote', host='0.0.0.0', port=1234)
        assert isinstance(das.backend, InMemoryDB)
        assert isinstance(das.query_engine, RemoteQueryEngine)

        with pytest.raises(InvalidAtomDB):
            das = DistributedAtomSpace(atomdb='snet')

        with pytest.raises(InvalidQueryEngine) as exc:
            das = DistributedAtomSpace(query_engine='snet')

        assert exc.value.message == "Use either 'local' or 'remote'"
        assert exc.value.details == 'query_engine=snet'


    def test_create_das_redis_mongo_local(self):
        with pytest.raises(Exception):
            DistributedAtomSpaceMock(atomdb="redis_mongo", query_engine="remote")

    # Example
    @pytest.mark.parametrize("params,expected", [
        ({"name": "A", "type": "A"}, {"name": "A", "type": "A"})
    ])
    def test_add_nodes(self, params, expected):
        das = DistributedAtomSpaceMock()
        das.set_backend_return("add_node", copy.deepcopy(expected))
        node = das.add_node(params)
        assert isinstance(node, dict)
        assert node == expected

    def test_get_incoming_links(self):
        das = DistributedAtomSpaceMock()
        cursor, links = das.get_incoming_links('<Concept: human>', handles_only=True)
        assert cursor is None
        assert len(links) == 7

        cursor, links = das.get_incoming_links('<Concept: human>')
        assert cursor is None
        assert len(links) == 7

        with mock.patch('hyperon_das.utils.check_server_connection', return_value=(200, 'OK')):
            das_remote = DistributedAtomSpaceMock('remote', host='test', port=8080)

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links', return_value=(0, [])
        ):
            cursor, links = das_remote.get_incoming_links('<Concept: human>')
        assert cursor == 0
        assert isinstance(links, RemoteIncomingLinks)
        assert len(links.source.source) == 7

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links', return_value=(0, [1, 2, 3, 4])
        ):
            cursor, links = das_remote.get_incoming_links('<Concept: snet>')
        assert cursor == 0
        assert isinstance(links, RemoteIncomingLinks)
        assert links.source.source == [1, 2, 3, 4]

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links',
            return_value=(0, ["['Inheritance', '<Concept: ent>', '<Concept: snet>']"]),
        ):
            cursor, links = das_remote.get_incoming_links('<Concept: ent>', handles_only=True)
        assert cursor == 0
        assert isinstance(links, RemoteIncomingLinks)
        assert set(links.source.source) == {
            "['Inheritance', '<Concept: ent>', '<Concept: plant>']",
            "['Similarity', '<Concept: ent>', '<Concept: human>']",
            "['Similarity', '<Concept: human>', '<Concept: ent>']",
            "['Inheritance', '<Concept: ent>', '<Concept: snet>']",
        }

    def test_get_traversal_cursor(self):
        das = DistributedAtomSpace()
        das.add_node({'type': 'Concept', 'name': 'human'})
        human = das.compute_node_handle('Concept', 'human')

        cursor = das.get_traversal_cursor(human)

        assert isinstance(cursor, TraverseEngine)

        with pytest.raises(GetTraversalCursorException) as exc:
            das.get_traversal_cursor(handle='snet')

        assert exc.value.message == 'Cannot start Traversal. Atom does not exist'

    def test_get_atom(self):
        das = DistributedAtomSpace()
        das.add_link(
            {
                'type': 'expression',
                'targets': [
                    {'type': 'symbol', 'name': 'a'},
                    {
                        'type': 'expression',
                        'targets': [
                            {'type': 'symbol', 'name': 'b'},
                            {'type': 'symbol', 'name': 'c'},
                        ],
                    },
                ],
            }
        )

        handle = {}
        for n in ['a', 'b', 'c']:
            handle[n] = das.compute_node_handle('symbol', n)

        handle['internal_link'] = das.compute_link_handle('expression', [handle['b'], handle['c']])
        handle['external_link'] = das.compute_link_handle(
            'expression', [handle['a'], handle['internal_link']]
        )

        for n in ['a', 'b', 'c']:
            document = das.get_atom(handle[n])
            assert document['type'] == 'symbol'
            assert document['name'] == n
            assert document['handle'] == handle[n]

        document = das.get_atom(handle['internal_link'])
        assert document['type'] == 'expression'
        assert document['handle'] == handle['internal_link']
        assert document['targets'] == [handle['b'], handle['c']]

        document = das.get_atom(handle['external_link'])
        assert document['type'] == 'expression'
        assert document['handle'] == handle['external_link']
        assert document['targets'] == [handle['a'], handle['internal_link']]

        assert das.get_atoms([handle['a'], handle['external_link'], handle['c']]) == [
            das.get_atom(handle['a']),
            das.get_atom(handle['external_link']),
            das.get_atom(handle['c']),
        ]

    def test_about(self):
        das = DistributedAtomSpace()
        assert isinstance(das.about(), dict)
        assert 'das' in das.about()
        assert 'atom_db' in das.about()
        assert {'name', 'version', 'summary'} == set(das.about().get('das').keys())
        assert {'name', 'version', 'summary'} == set(das.about().get('atom_db').keys())

    def test_create_context(self):
        das = DistributedAtomSpace()
        context = das.create_context("blah", {})
        assert context.name == "blah"

    def test_get_atoms_by_field(self):
        das = DistributedAtomSpaceMock()
        atom_field = das.get_atoms_by_field({'Concept': 'human'})
        assert atom_field

    def test_get_atoms_by_text_field(self):
        das = DistributedAtomSpaceMock()
        atom_text_field = das.get_atoms_by_text_field(text_value='human', field='name')
        assert atom_text_field

    def test_get_node_by_name_starting_with(self):
        das = DistributedAtomSpaceMock()
        atom_starting_with = das.get_node_by_name_starting_with('Concept', 'mon')
        assert atom_starting_with

    def test_count_atoms(self):
        das = DistributedAtomSpaceMock()
        atom_count = das.count_atoms()
        assert atom_count == {'link_count': 26, 'node_count': 14, 'atom_count': 40}

    def test_count_atoms_local(self):
        das = DistributedAtomSpaceMock()
        atom_count = das.count_atoms({'context': 'local'})
        assert atom_count == {'link_count': 26, 'node_count': 14, 'atom_count': 40}

    def test_count_atoms_local_remote(self):
        das = DistributedAtomSpaceMock()
        atom_count = das.count_atoms({'context': 'remote'})
        assert atom_count == {}

    def test_count_atoms_local_both(self):
        das = DistributedAtomSpaceMock()
        atom_count = das.count_atoms({'context': 'both'})
        assert atom_count == {'link_count': 26, 'node_count': 14, 'atom_count': 40}
        # assert atom_count == (14, 26)

    def test_count_atoms_remote(self):
        das = DistributedAtomSpaceMock('remote', host='localhost', port=123)
        with mock.patch(
            'hyperon_das.client.FunctionsClient.count_atoms',
            return_value=(10, 0),
        ):
            atom_count = das.count_atoms({'context': 'remote'})
        assert atom_count == (10, 0)

    def test_count_atoms_both(self):
        das = DistributedAtomSpaceMock('remote', host='localhost', port=123)
        with mock.patch(
            'hyperon_das.client.FunctionsClient.count_atoms',
            return_value={'link_count': 0, 'node_count': 10, 'atom_count': 0},
        ):
            atom_count = das.count_atoms({'context': 'both'})
        # assert atom_count == (24, 26)
        assert atom_count == {'link_count': 26, 'node_count': 24, 'atom_count': 40}
