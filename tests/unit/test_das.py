from unittest import mock

import pytest
from hyperon_das_atomdb.adapters import InMemoryDB
from hyperon_das_atomdb.exceptions import InvalidAtomDB

from hyperon_das.das import DistributedAtomSpace, LocalQueryEngine, RemoteQueryEngine
from hyperon_das.exceptions import GetTraversalCursorException, InvalidQueryEngine
from hyperon_das.traverse_engines import TraverseEngine


class TestDistributedAtomSpace:
    def test_create_das(self):
        das = DistributedAtomSpace()
        assert isinstance(das.backend, InMemoryDB)
        assert isinstance(das.query_engine, LocalQueryEngine)

        with mock.patch(
            'hyperon_das.das.RemoteQueryEngine._connect_server', return_value='url-test'
        ):
            das = DistributedAtomSpace(query_engine='remote', host='0.0.0.0', port=1234)
        assert isinstance(das.backend, InMemoryDB)
        assert isinstance(das.query_engine, RemoteQueryEngine)

        with pytest.raises(InvalidAtomDB):
            das = DistributedAtomSpace(atomdb='snet')

        with pytest.raises(InvalidQueryEngine) as exc:
            das = DistributedAtomSpace(query_engine='snet')

        assert exc.value.message == 'The possible values are: `local` or `remote`'
        assert exc.value.details == 'query_engine=snet'

    def test_get_traversal_cursor(self):
        das = DistributedAtomSpace()
        das.add_node({'type': 'Concept', 'name': 'human'})
        human = das.get_node_handle('Concept', 'human')

        cursor = das.get_traversal_cursor(human)

        assert isinstance(cursor, TraverseEngine)

        with pytest.raises(GetTraversalCursorException) as exc:
            das.get_traversal_cursor(handle='snet')

        assert exc.value.message == 'Cannot start Traversal. Atom does not exist'
