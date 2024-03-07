from typing import Union

import pytest
from hyperon_das_atomdb.adapters import InMemoryDB

from hyperon_das import DistributedAtomSpace
from hyperon_das.cache import ListIterator, LocalGetLinks, LocalIncomingLinks, RemoteIncomingLinks
from tests.integration.test_local_redis_mongo import _db_down, _db_up, mongo_port, redis_port
from tests.unit.helpers import load_n_random_links_by_type
from tests.utils import animal_base_handles, load_animals_base


class TestIncomingLinks:
    @pytest.fixture
    def human_incoming_links(self):
        return [
            animal_base_handles.similarity_human_chimp,
            animal_base_handles.similarity_human_ent,
            animal_base_handles.similarity_human_monkey,
            animal_base_handles.similarity_chimp_human,
            animal_base_handles.similarity_ent_human,
            animal_base_handles.similarity_monkey_human,
            animal_base_handles.inheritance_human_mammal,
        ]

    def _run_asserts(self, das, iterator: Union[LocalIncomingLinks, RemoteIncomingLinks]):
        current_value = iterator.get()
        assert current_value == das.get_atom(iterator.get()['handle'])
        assert isinstance(current_value, dict)
        assert current_value == next(iterator)
        assert iterator.is_empty() is False
        [item for item in iterator]
        assert iterator.is_empty() is True
        with pytest.raises(StopIteration):
            iterator.get()

    def test_local_incoming_links_ram_only_iterator(self):
        db = InMemoryDB()
        load_animals_base(db)
        links = [
            animal_base_handles.similarity_human_chimp,
            animal_base_handles.similarity_human_ent,
            animal_base_handles.similarity_human_monkey,
            animal_base_handles.similarity_chimp_human,
            animal_base_handles.similarity_ent_human,
            animal_base_handles.similarity_monkey_human,
            animal_base_handles.inheritance_human_mammal,
        ]
        it = LocalIncomingLinks(
            ListIterator(links), backend=db, atom_handle=animal_base_handles.human
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
        load_n_random_links_by_type(das=das, n=2000)
        das.commit_changes()

        self.backend = das.backend

        atom_handle = das.get_node_handle('Concept', 'human')
        chunk_size = 500
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

    def test_incoming_links_with_ram_only(self, human_incoming_links):
        das = DistributedAtomSpace()
        load_animals_base(das)
        iterator = das.get_incoming_links(animal_base_handles.human, no_iterator=False)
        self._run_asserts(das, iterator)

    def test_incoming_links_with_redis_mongo(self):
        das = DistributedAtomSpace()
        load_animals_base(das)
        iterator = das.get_incoming_links(animal_base_handles.human, no_iterator=False)
        self._run_asserts(das, iterator)

    def test_incoming_links_with_remote_das(self):
        das = DistributedAtomSpace()
        load_animals_base(das)
        iterator = das.get_incoming_links(animal_base_handles.human, no_iterator=False)
        self._run_asserts(das, iterator)


class TestGetLinks:
    def test_local_get_links_ram_only_iterator(self):
        das = DistributedAtomSpace()
        load_animals_base(das)
        link_type = 'Inheritance'
        answer = das.query_engine._get_related_links(link_type)

        it = LocalGetLinks(ListIterator(answer), backend=das.query_engine, link_type=link_type)
        current_value = it.get()
        assert isinstance(current_value, dict)
        assert current_value == next(it)
        assert it.is_empty() is False
        [i for i in it]
        assert it.is_empty() is True
        with pytest.raises(StopIteration):
            it.get()
        answer = das.query_engine._get_related_links('Fake')
        it = LocalGetLinks(ListIterator(answer), backend=das.query_engine, link_type=link_type)
        assert it.is_empty() is True
        with pytest.raises(StopIteration):
            it.get()
        assert [i for i in it] == []

    def test_local_get_links_redis_mongo_iterator(self):
        from tests.integration.test_local_redis_mongo import (
            _db_down,
            _db_up,
            mongo_port,
            redis_port,
        )

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
        load_animals_base(das)
        das.commit_changes()

        link_type = 'Similarity'
        cursor, answer = das.query_engine._get_related_links(link_type, cursor=0)
        it = LocalGetLinks(
            ListIterator(answer), backend=das.query_engine, link_type=link_type, cursor=cursor
        )
        current_value = it.get()
        assert isinstance(current_value, dict)
        assert current_value == next(it)
        assert it.is_empty() is False
        [i for i in it]
        assert it.is_empty() is True
        with pytest.raises(StopIteration):
            it.get()

        load_n_random_links_by_type(das=das, n=2000)
        das.commit_changes()
        link_type = 'Inheritance'
        cursor, answer = das.query_engine._get_related_links(link_type, cursor=0, chunk_size=500)
        it = LocalGetLinks(
            ListIterator(answer), backend=das.query_engine, link_type=link_type, cursor=cursor
        )
        assert it.is_empty() is False
        result = [i for i in it]
        assert it.is_empty() is True
        with pytest.raises(StopIteration):
            it.get()
        assert len(result) >= 1000

        _db_down()

    def test_get_links_with_ram_only_no_iterator_true(self):
        pass

    def test_get_links_with_ram_only_no_iterator_false(self):
        pass

    def test_get_links_with_redis_mongo_no_iterator_true(self):
        pass

    def test_get_links_with_redis_mongo_no_iterator_false(self):
        pass

    def test_get_links_with_remote_das(self):
        pass
