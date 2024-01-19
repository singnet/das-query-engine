from unittest import mock

import pytest
from hyperon_das_atomdb.adapters import InMemoryDB

from hyperon_das.das import DistributedAtomSpace, LocalQueryEngine, RemoteQueryEngine
from hyperon_das.exceptions import InvalidQueryEngine

from .mock import DistributedAtomSpaceMock


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

        with pytest.raises(ValueError):
            das = DistributedAtomSpace(atomdb='snet')

        with pytest.raises(InvalidQueryEngine) as exc:
            das = DistributedAtomSpace(query_engine='snet')

        assert exc.value.message == 'The possible values are: `local` or `remote`'
        assert exc.value.details == 'query_engine=snet'

    def test_get_incoming_links(self):
        das = DistributedAtomSpaceMock()
        links = das.get_incoming_links('<Concept: human>', handles_only=True)
        assert len(links) == 7

        links = das.get_incoming_links('<Concept: human>')
        assert len(links) == 7

        with mock.patch(
            'hyperon_das.query_engines.RemoteQueryEngine._connect_server', return_value='fake'
        ):
            das_remote = DistributedAtomSpaceMock('remote', host='test')

        with mock.patch('hyperon_das.client.FunctionsClient.get_incoming_links', return_value=[]):
            links = das_remote.get_incoming_links('<Concept: human>')
        assert len(links) == 7

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links', return_value=[1, 2, 3, 4]
        ):
            links = das_remote.get_incoming_links('<Concept: snet>')
        assert links == [1, 2, 3, 4]

        with mock.patch(
            'hyperon_das.client.FunctionsClient.get_incoming_links',
            return_value=["['Inheritance', '<Concept: ent>', '<Concept: snet>']"],
        ):
            links = das_remote.get_incoming_links('<Concept: ent>', handles_only=True)
        assert set(links) == {
            "['Inheritance', '<Concept: ent>', '<Concept: plant>']",
            "['Similarity', '<Concept: ent>', '<Concept: human>']",
            "['Similarity', '<Concept: human>', '<Concept: ent>']",
            "['Inheritance', '<Concept: ent>', '<Concept: snet>']",
        }
