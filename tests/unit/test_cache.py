from unittest import mock

import pytest

from hyperon_das import DistributedAtomSpace
from hyperon_das.cache import ListIterator, LocalIncomingLinks, ProductIterator
from hyperon_das.utils import Assignment
from tests.unit.mock import up_knowledge_base_animals, up_n_links_in_database


class TestCache:
    def test_list_iterator(self):
        iterator = ListIterator(None)
        for element in iterator:
            assert False

        iterator = ListIterator([])
        for element in iterator:
            assert False

        iterator = ListIterator(
            [
                ([{"id": 1}], Assignment()),
            ]
        )
        expected = [1]
        count = 0
        for element in iterator:
            assert element[0][0]["id"] == expected[count]
            count += 1
        assert count == len(expected)

        iterator = ListIterator(
            [
                ([{"id": 1}], Assignment()),
                ([{"id": 2}], Assignment()),
            ]
        )
        expected = [1, 2]
        count = 0
        for element in iterator:
            assert element[0][0]["id"] == expected[count]
            count += 1
        assert count == len(expected)

        iterator = ListIterator(
            [
                ([{"id": 1}], Assignment()),
                ([{"id": 2}], Assignment()),
                ([{"id": 2}], Assignment()),
            ]
        )
        expected = [1, 2, 2]
        count = 0
        for element in iterator:
            assert element[0][0]["id"] == expected[count]
            count += 1
        assert count == len(expected)

        iterator = ListIterator(
            [
                ([{"id": 1}], Assignment()),
            ]
        )
        assert not iterator.is_empty()
        iterator = ListIterator([None])
        assert not iterator.is_empty()
        iterator = ListIterator([])
        assert iterator.is_empty()
        iterator = ListIterator(None)
        assert iterator.is_empty()

    def test_product_iterator(self):
        ln = None
        l0 = []
        l1 = [1, 2, 3]
        l2 = [4]
        l3 = [5, 6]
        l4 = [7, 8]

        li1 = ListIterator(l1)
        li3 = ListIterator(l3)
        iterator = ProductIterator([li1, li3])
        assert not iterator.is_empty()
        assert iterator.get() == (1, 5)
        expected = [(1, 5), (1, 6), (2, 5), (2, 6), (3, 5), (3, 6)]
        count = 0
        for element in iterator:
            assert element == expected[count]
            assert iterator.get() == expected[count]
            count += 1
        assert not iterator.is_empty()
        with pytest.raises(StopIteration):
            assert iterator.get()

        li3 = ListIterator(l3)
        li4 = ListIterator(l4)
        iterator = ProductIterator([li3, li4])
        expected = [(5, 7), (5, 8), (6, 7), (6, 8)]
        count = 0
        for element in iterator:
            assert element == expected[count]
            count += 1
        assert not iterator.is_empty()

        li1 = ListIterator(l1)
        li2 = ListIterator(l2)
        li3 = ListIterator(l3)
        iterator = ProductIterator([li1, li2, li3])
        expected = [
            (1, 4, 5),
            (1, 4, 6),
            (2, 4, 5),
            (2, 4, 6),
            (3, 4, 5),
            (3, 4, 6),
        ]
        count = 0
        for element in iterator:
            assert element == expected[count]
            count += 1
        with pytest.raises(StopIteration):
            assert iterator.get()

        for arg in [[ln, l1], [ln, l1, l2], [ln]]:
            iterator = ProductIterator([ListIterator(v) for v in arg])
            assert iterator.is_empty()
            for element in iterator:
                assert False
            assert iterator.is_empty()

        for arg in [[l0, l1], [l0, l1, l2], [l0]]:
            iterator = ProductIterator([ListIterator(v) for v in arg])
            assert iterator.is_empty()
            for element in iterator:
                assert False
            assert iterator.is_empty()

    def test_local_incoming_links_ram_only_iterator(self):
        def mock_get_incoming_links_ram_only(atom_handle: str, **kwargs):
            links = self.backend.db.incoming_set.get(atom_handle, [])
            if kwargs.get('handles_only', False):
                return links
            else:
                return [self.backend.get_atom(handle, **kwargs) for handle in links]

        das = DistributedAtomSpace()
        self.backend = das.backend
        up_knowledge_base_animals(das)
        atom_handle = das.get_node_handle('Concept', 'human')
        with mock.patch(
            'hyperon_das_atomdb.adapters.ram_only.InMemoryDB.get_incoming_links',
            side_effect=mock_get_incoming_links_ram_only,
        ):
            links = das.backend.get_incoming_links(atom_handle, handles_only=True)
            it = LocalIncomingLinks(
                ListIterator(links), backend=das.backend, atom_handle=atom_handle
            )
            current_value = it.get()
            assert isinstance(current_value, dict)
            assert current_value == next(it)
            assert it.is_empty() is False
            [i for i in it]
            assert it.is_empty() is True
            with pytest.raises(StopIteration):
                it.get()

    def test_local_incoming_links_redis_mongo_iterator(self):
        from tests.integration.test_local_redis_mongo import (
            _db_down,
            _db_up,
            mongo_port,
            redis_port,
        )

        def mock_get_incoming_links_redis_mongo(atom_handle: str, **kwargs):
            cursor, links = self.backend._retrieve_incoming_set(atom_handle, **kwargs)
            if kwargs.get('cursor') is not None:
                if kwargs.get('handles_only', False):
                    return cursor, links
                else:
                    return cursor, [self.backend.get_atom(handle, **kwargs) for handle in links]
            else:
                if kwargs.get('handles_only', False):
                    return links
                else:
                    return [self.backend.get_atom(handle, **kwargs) for handle in links]

        _db_up()
        das = DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )
        up_n_links_in_database(das, 2000)
        das.commit_changes()

        self.backend = das.backend

        atom_handle = das.get_node_handle('Concept', 'human')
        chunk_size = 500
        with mock.patch(
            'hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB.get_incoming_links',
            side_effect=mock_get_incoming_links_redis_mongo,
        ):
            cursor, links = das.backend.get_incoming_links(
                atom_handle, handles_only=True, cursor=0, chunk_size=chunk_size
            )
            it = LocalIncomingLinks(
                ListIterator(links),
                backend=das.backend,
                atom_handle=atom_handle,
                cursor=cursor,
                chunk_size=chunk_size,
            )
            current_value = it.get()
            assert isinstance(current_value, dict)
            assert current_value == next(it)
            assert it.is_empty() is False
            [i for i in it]
            assert it.is_empty() is True
            with pytest.raises(StopIteration):
                it.get()
        _db_down()
