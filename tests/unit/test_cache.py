from unittest import mock

import pytest
from hyperon_das_atomdb.adapters import InMemoryDB

from hyperon_das import DistributedAtomSpace
from hyperon_das.cache import (
    ListIterator,
    LocalGetLinks,
    LocalIncomingLinks,
    ProductIterator,
    RemoteIncomingLinks,
)
from hyperon_das.utils import Assignment
from tests.unit.helpers import load_n_random_links_by_type
from tests.utils import animal_base_handles, load_animals_base


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

    # def test_local_incoming_links_ram_only_iterator(self):
    #     db = InMemoryDB()
    #     load_animals_base(db)
    #     links = [
    #         animal_base_handles.similarity_human_chimp,
    #         animal_base_handles.similarity_human_ent,
    #         animal_base_handles.similarity_human_monkey,
    #         animal_base_handles.similarity_chimp_human,
    #         animal_base_handles.similarity_ent_human,
    #         animal_base_handles.similarity_monkey_human,
    #         animal_base_handles.inheritance_human_mammal,
    #     ]
    #     it = LocalIncomingLinks(
    #         ListIterator(links), backend=db, atom_handle=animal_base_handles.human
    #     )
    #     current_value = it.get()
    #     assert isinstance(current_value, dict)
    #     assert current_value == next(it)
    #     assert it.is_empty() is False
    #     [i for i in it]
    #     assert it.is_empty() is True
    #     with pytest.raises(StopIteration):
    #         it.get()

    # def test_local_incoming_links_redis_mongo_iterator(self):
    #     from tests.integration.test_local_redis_mongo import (
    #         _db_down,
    #         _db_up,
    #         mongo_port,
    #         redis_port,
    #     )

    #     _db_up()
    #     das = DistributedAtomSpace(
    #         query_engine='local',
    #         atomdb='redis_mongo',
    #         mongo_port=mongo_port,
    #         mongo_username='dbadmin',
    #         mongo_password='dassecret',
    #         redis_port=redis_port,
    #         redis_cluster=False,
    #         redis_ssl=False,
    #     )
    #     load_n_random_links_by_type(das=das, n=2000)
    #     das.commit_changes()

    #     self.backend = das.backend

    #     atom_handle = das.get_node_handle('Concept', 'human')
    #     chunk_size = 500
    #     cursor, links = das.backend.get_incoming_links(
    #         atom_handle, handles_only=True, cursor=0, chunk_size=chunk_size
    #     )
    #     it = LocalIncomingLinks(
    #         ListIterator(links),
    #         backend=das.backend,
    #         atom_handle=atom_handle,
    #         cursor=cursor,
    #         chunk_size=chunk_size,
    #     )
    #     current_value = it.get()
    #     assert isinstance(current_value, dict)
    #     assert current_value == next(it)
    #     assert it.is_empty() is False
    #     [i for i in it]
    #     assert it.is_empty() is True
    #     with pytest.raises(StopIteration):
    #         it.get()
    #     _db_down()

    # def test_local_get_links_ram_only_iterator(self):
    #     das = DistributedAtomSpace()
    #     load_animals_base(das)
    #     link_type = 'Inheritance'
    #     answer = das.query_engine._get_related_links(link_type)

    #     it = LocalGetLinks(ListIterator(answer), backend=das.query_engine, link_type=link_type)
    #     current_value = it.get()
    #     assert isinstance(current_value, dict)
    #     assert current_value == next(it)
    #     assert it.is_empty() is False
    #     [i for i in it]
    #     assert it.is_empty() is True
    #     with pytest.raises(StopIteration):
    #         it.get()
    #     answer = das.query_engine._get_related_links('Fake')
    #     it = LocalGetLinks(ListIterator(answer), backend=das.query_engine, link_type=link_type)
    #     assert it.is_empty() is True
    #     with pytest.raises(StopIteration):
    #         it.get()
    #     assert [i for i in it] == []

    # def test_local_get_links_redis_mongo_iterator(self):
    #     from tests.integration.test_local_redis_mongo import (
    #         _db_down,
    #         _db_up,
    #         mongo_port,
    #         redis_port,
    #     )

    #     _db_up()

    #     das = DistributedAtomSpace(
    #         query_engine='local',
    #         atomdb='redis_mongo',
    #         mongo_port=mongo_port,
    #         mongo_username='dbadmin',
    #         mongo_password='dassecret',
    #         redis_port=redis_port,
    #         redis_cluster=False,
    #         redis_ssl=False,
    #     )
    #     load_animals_base(das)
    #     das.commit_changes()

    #     link_type = 'Similarity'
    #     cursor, answer = das.query_engine._get_related_links(link_type, cursor=0)
    #     it = LocalGetLinks(
    #         ListIterator(answer), backend=das.query_engine, link_type=link_type, cursor=cursor
    #     )
    #     current_value = it.get()
    #     assert isinstance(current_value, dict)
    #     assert current_value == next(it)
    #     assert it.is_empty() is False
    #     [i for i in it]
    #     assert it.is_empty() is True
    #     with pytest.raises(StopIteration):
    #         it.get()

    #     load_n_random_links_by_type(das=das, n=2000)
    #     das.commit_changes()
    #     link_type = 'Inheritance'
    #     cursor, answer = das.query_engine._get_related_links(link_type, cursor=0, chunk_size=500)
    #     it = LocalGetLinks(
    #         ListIterator(answer), backend=das.query_engine, link_type=link_type, cursor=cursor
    #     )
    #     assert it.is_empty() is False
    #     result = [i for i in it]
    #     assert it.is_empty() is True
    #     with pytest.raises(StopIteration):
    #         it.get()
    #     assert len(result) >= 1000

    #     _db_down()


class TestLocalIncomingLinks:
    @pytest.fixture
    def backend(self):
        backend = mock.MagicMock()
        backend.get_atom.side_effect = lambda x, targets_document=None: {'handle': x}
        return backend

    def test_get_next_value(self, backend):
        iterator = LocalIncomingLinks(ListIterator([1, 2, 3]), backend=backend)

        iterator.get_next_value()
        assert iterator.current_value == backend.get_atom(1)

        iterator.get_next_value()
        assert iterator.current_value == backend.get_atom(2)

        iterator.get_next_value()
        assert iterator.current_value == backend.get_atom(3)

        with pytest.raises(StopIteration):
            iterator.get_next_value()

    def test_get_current_value(self, backend):
        iterator = LocalIncomingLinks(ListIterator([1, 2, 3]), backend=backend)

        assert iterator.get_current_value() == backend.get_atom(1)

        iterator.get_next_value()
        assert iterator.get_current_value() == backend.get_atom(1)

        iterator.get_next_value()
        assert iterator.get_current_value() == backend.get_atom(2)

        iterator.get_next_value()
        assert iterator.get_current_value() == backend.get_atom(3)

        iterator = LocalIncomingLinks(ListIterator([]), backend=backend)
        assert iterator.is_empty() is True

    def test_get_fetch_data_kwargs(self, backend):
        iterator = LocalIncomingLinks(ListIterator([1, 2, 3]), backend=backend)
        assert iterator.get_fetch_data_kwargs() == {
            'handles_only': True,
            'cursor': iterator.cursor,
            'chunk_size': iterator.chunk_size,
        }

    def test_get_fetch_data(self, backend):
        iterator = LocalIncomingLinks(ListIterator([1, 2, 3]), backend=backend)
        kwargs = {'param1': 'value1', 'param2': 'value2'}
        result = iterator.get_fetch_data(**kwargs)
        assert result == backend.get_incoming_links(iterator.atom_handle, **kwargs)


class TestRemoteIncomingLinks:
    def test_get_next_value(self):
        source = ListIterator(
            [{'handle': 'link1'}, {'handle': 'link2'}, {'handle': 'link3'}, {'handle': 'link4'}]
        )
        iterator = RemoteIncomingLinks(source)
        iterator.returned_handles = set(['link2', 'link3'])

        iterator.get_next_value()
        assert iterator.current_value == {'handle': 'link1'}

        iterator.get_next_value()
        assert iterator.current_value == {'handle': 'link4'}

    def test_get_current_value(self):
        source = ListIterator(
            [
                {'handle': 'link1'},
                {'handle': 'link2'},
            ]
        )
        iterator = RemoteIncomingLinks(source)

        iterator.get_next_value()
        assert iterator.get_current_value() == {'handle': 'link1'}

        iterator.get_next_value()
        assert iterator.get_current_value() == {'handle': 'link2'}

        iterator = RemoteIncomingLinks(ListIterator([]))
        assert iterator.is_empty() is True

    def test_get_fetch_data_kwargs(self):
        source = ListIterator(
            [
                {'handle': 'link1'},
                {'handle': 'link2'},
            ]
        )
        iterator = RemoteIncomingLinks(source, atom_handle='atom1', targets_document=True)
        kwargs = iterator.get_fetch_data_kwargs()
        assert kwargs == {
            'cursor': iterator.cursor,
            'chunk_size': iterator.chunk_size,
            'targets_document': iterator.targets_document,
        }

    def test_get_fetch_data(self):
        backend = mock.MagicMock()
        backend.get_incoming_links.return_value = (123, [{'handle': 'link1'}, {'handle': 'link2'}])
        source = ListIterator([])
        iterator = RemoteIncomingLinks(source, atom_handle='atom1')
        iterator.backend = backend
        result = iterator.get_fetch_data(cursor=0, chunk_size=100)
        assert result == (123, [{'handle': 'link1'}, {'handle': 'link2'}])
        backend.get_incoming_links.assert_called_once_with('atom1', cursor=0, chunk_size=100)


class TestLocalGetLinks:
    @pytest.fixture
    def iterator(self):
        link_type = 'Similarity'
        cursor = 0
        chunk_size = 500
        target_types = ['Type1', 'Type2']
        link_targets = ['Target1', 'Target2']
        toplevel_only = True

        source = ListIterator([1, 2, 3])

        backend = mock.Mock()
        backend._to_link_dict_list.side_effect = lambda x: [{'handle': x[0], 'name': f'Link{x[0]}'}]
        backend._get_related_links.return_value = (
            cursor,
            [{'handle': 1, 'name': 'Link1'}, {'handle': 2, 'name': 'Link2'}],
        )

        return LocalGetLinks(
            source,
            backend=backend,
            link_type=link_type,
            target_types=target_types,
            link_targets=link_targets,
            toplevel_only=toplevel_only,
            cursor=cursor,
            chunk_size=chunk_size,
        )

    def test_get_next_value(self, iterator):
        iterator.get_next_value()
        assert iterator.current_value == {'handle': 1, 'name': 'Link1'}

        iterator.get_next_value()
        assert iterator.current_value == {'handle': 2, 'name': 'Link2'}

        iterator.get_next_value()
        assert iterator.current_value == {'handle': 3, 'name': 'Link3'}

    def test_get_current_value(self, iterator):
        current_value = iterator.get_current_value()
        assert current_value == {'handle': 1, 'name': 'Link1'}

    def test_get_fetch_data_kwargs(self, iterator):
        fetch_data_kwargs = iterator.get_fetch_data_kwargs()
        expected_kwargs = {'cursor': 0, 'chunk_size': 500, 'toplevel_only': True}
        assert fetch_data_kwargs == expected_kwargs

    def test_get_fetch_data(self, iterator):
        fetch_data = iterator.get_fetch_data()
        expected_fetch_data = (0, [{'handle': 1, 'name': 'Link1'}, {'handle': 2, 'name': 'Link2'}])
        assert fetch_data == expected_fetch_data
