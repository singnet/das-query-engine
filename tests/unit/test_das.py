from unittest import mock

import pytest
from hyperon_das_atomdb.adapters import InMemoryDB
from hyperon_das_atomdb.exceptions import InvalidAtomDB

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

    def test_get_incoming_links(self):
        das = DistributedAtomSpaceMock()
        links = das.get_incoming_links('<Concept: human>', handles_only=True)
        assert len(links) == 7

        links = das.get_incoming_links('<Concept: human>')
        assert len(links) == 7

        with mock.patch('hyperon_das.utils.check_server_connection', return_value=(200, 'OK')):
            das_remote = DistributedAtomSpaceMock('remote', host='test', port=8080)

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links', return_value=(0, [])
        ):
            links = das_remote.get_incoming_links('<Concept: human>')
        assert len(links.source.source) == 7

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links', return_value=(0, [1, 2, 3, 4])
        ):
            links = das_remote.get_incoming_links('<Concept: snet>')
        assert links.source.source == [1, 2, 3, 4]

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links',
            return_value=(0, ["['Inheritance', '<Concept: ent>', '<Concept: snet>']"]),
        ):
            links = das_remote.get_incoming_links('<Concept: ent>', handles_only=True)
        assert set(links.source.source) == {
            "['Inheritance', '<Concept: ent>', '<Concept: plant>']",
            "['Similarity', '<Concept: ent>', '<Concept: human>']",
            "['Similarity', '<Concept: human>', '<Concept: ent>']",
            "['Inheritance', '<Concept: ent>', '<Concept: snet>']",
        }

    def test_get_traversal_cursor(self):
        das = DistributedAtomSpace()
        das.add_node({'type': 'Concept', 'name': 'human'})
        human = das.get_node_handle('Concept', 'human')

        cursor = das.get_traversal_cursor(human)

        assert isinstance(cursor, TraverseEngine)

        with pytest.raises(GetTraversalCursorException) as exc:
            das.get_traversal_cursor(handle='snet')

        assert exc.value.message == 'Cannot start Traversal. Atom does not exist'

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
