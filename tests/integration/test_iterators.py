from typing import Union

import pytest
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher as hasher

from hyperon_das import DistributedAtomSpace
from hyperon_das.cache import (
    LocalGetLinks,
    LocalIncomingLinks,
    RemoteGetLinks,
    RemoteIncomingLinks,
    TraverseLinksIterator,
    TraverseNeighborsIterator,
)
from tests.integration.helpers import (
    _db_down,
    _db_up,
    cleanup,
    load_metta_animals_base,
    metta_animal_base_handles,
    mongo_port,
    redis_port,
)
from tests.integration.remote_das_info import remote_das_host, remote_das_port


class TestIncomingLinks:
    @pytest.fixture(scope="session")
    def _cleanup(self, request):
        return cleanup(request)

    @pytest.fixture
    def human_handle(self):
        return metta_animal_base_handles.human

    def _human_incoming_links(self):
        return sorted(
            [
                metta_animal_base_handles.similarity_human_monkey,
                metta_animal_base_handles.similarity_monkey_human,
                metta_animal_base_handles.similarity_human_chimp,
                metta_animal_base_handles.similarity_chimp_human,
                metta_animal_base_handles.similarity_human_ent,
                metta_animal_base_handles.similarity_ent_human,
                metta_animal_base_handles.inheritance_human_mammal,
                hasher.expression_hash(
                    hasher.named_type_hash('MettaType'),
                    [
                        hasher.terminal_hash('Symbol', '"human"'),
                        hasher.terminal_hash('Symbol', 'Concept'),
                    ],
                ),
            ]
        )

    def _check_asserts(
        self, das: DistributedAtomSpace, iterator: Union[LocalIncomingLinks, RemoteIncomingLinks]
    ):
        current_value = iterator.get()
        assert current_value == das.get_atom(iterator.get()['handle'])
        assert isinstance(current_value, dict)
        assert iterator.is_empty() is False
        link_handles = sorted([item['handle'] for item in iterator])
        assert len(link_handles) == 8
        assert link_handles == self._human_incoming_links()
        assert iterator.is_empty() is True
        with pytest.raises(StopIteration):
            iterator.get()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_incoming_links_with_das_ram_only(self, human_handle):
        das = DistributedAtomSpace()
        load_metta_animals_base(das)
        iterator = das.get_incoming_links(human_handle, no_iterator=False)
        self._check_asserts(das, iterator)

    def test_incoming_links_with_das_redis_mongo(self, human_handle, _cleanup):
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
        load_metta_animals_base(das)
        das.commit_changes()
        iterator = das.get_incoming_links(human_handle, no_iterator=False, cursor=0)
        self._check_asserts(das, iterator)
        _db_down()

    def test_incoming_links_with_remote_das(self, human_handle):
        das = DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )
        iterator = das.get_incoming_links(human_handle)
        self._check_asserts(das, iterator)


class TestGetLinks:
    @pytest.fixture(scope="session")
    def _cleanup(self, request):
        return cleanup(request)

    def _expression_links(self) -> list:
        return sorted(
            [
                metta_animal_base_handles.similarity_human_monkey,
                metta_animal_base_handles.similarity_human_chimp,
                metta_animal_base_handles.similarity_chimp_monkey,
                metta_animal_base_handles.similarity_snake_earthworm,
                metta_animal_base_handles.similarity_rhino_triceratops,
                metta_animal_base_handles.similarity_snake_vine,
                metta_animal_base_handles.similarity_human_ent,
                metta_animal_base_handles.inheritance_human_mammal,
                metta_animal_base_handles.inheritance_monkey_mammal,
                metta_animal_base_handles.inheritance_chimp_mammal,
                metta_animal_base_handles.inheritance_mammal_animal,
                metta_animal_base_handles.inheritance_reptile_animal,
                metta_animal_base_handles.inheritance_snake_reptile,
                metta_animal_base_handles.inheritance_dinosaur_reptile,
                metta_animal_base_handles.inheritance_triceratops_dinosaur,
                metta_animal_base_handles.inheritance_earthworm_animal,
                metta_animal_base_handles.inheritance_rhino_mammal,
                metta_animal_base_handles.inheritance_vine_plant,
                metta_animal_base_handles.inheritance_ent_plant,
                metta_animal_base_handles.similarity_monkey_human,
                metta_animal_base_handles.similarity_chimp_human,
                metta_animal_base_handles.similarity_monkey_chimp,
                metta_animal_base_handles.similarity_earthworm_snake,
                metta_animal_base_handles.similarity_triceratops_rhino,
                metta_animal_base_handles.similarity_vine_snake,
                metta_animal_base_handles.similarity_ent_human,
            ]
        )

    def _check_asserts(
        self, das: DistributedAtomSpace, iterator: Union[LocalGetLinks, RemoteGetLinks]
    ):
        current_value = iterator.get()
        assert current_value['handle'] == das.get_atom(iterator.get()['handle'])['handle']
        assert isinstance(current_value, dict)
        assert iterator.is_empty() is False
        link_handles = sorted([item['handle'] for item in iterator])
        assert len(link_handles) == 26
        assert link_handles == self._expression_links()
        assert iterator.is_empty() is True
        with pytest.raises(StopIteration):
            iterator.get()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_get_links_with_das_ram_only(self):
        das = DistributedAtomSpace()
        load_metta_animals_base(das)
        iterator = das.get_links('Expression', no_iterator=False)
        self._check_asserts(das, iterator)

    def test_get_links_with_das_redis_mongo(self, _cleanup):
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
        load_metta_animals_base(das)
        das.commit_changes()
        iterator = das.get_links('Expression', no_iterator=False, cursor=0)
        self._check_asserts(das, iterator)
        _db_down()

    # TODO: Uncomment this test when the load with MeTTa parse is creating the template keys correctly
    # def test_get_links_with_remote_das(self, _cleanup):
    #     das = DistributedAtomSpace(query_engine='remote', host=remote_das_host, port=remote_das_port)
    #     iterator = das.get_links('Expression')
    #     self._check_asserts(das, iterator)


class TestTraverseLinks:
    @pytest.fixture(scope="session")
    def _cleanup(self, request):
        return cleanup(request)

    @pytest.fixture
    def human_handle(self):
        return metta_animal_base_handles.human

    def _is_expression_atom(self, atom: dict) -> bool:
        if atom['named_type'] != 'Expression':
            return False
        return True

    def _human_incoming_links(self):
        return sorted(
            [
                metta_animal_base_handles.similarity_human_monkey,
                metta_animal_base_handles.similarity_monkey_human,
                metta_animal_base_handles.similarity_human_chimp,
                metta_animal_base_handles.similarity_chimp_human,
                metta_animal_base_handles.similarity_human_ent,
                metta_animal_base_handles.similarity_ent_human,
                metta_animal_base_handles.inheritance_human_mammal,
            ]
        )

    def _check_asserts(self, das: DistributedAtomSpace, iterator: TraverseLinksIterator):
        current_value = iterator.get()
        assert current_value == das.get_atom(iterator.get()['handle'])
        assert isinstance(current_value, dict)
        assert iterator.is_empty() is False
        link_handles = sorted([item['handle'] for item in iterator])
        assert len(link_handles) == 7
        assert link_handles == self._human_incoming_links()
        assert iterator.is_empty() is True
        with pytest.raises(StopIteration):
            iterator.get()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_traverse_links_with_das_ram_only(self, human_handle):
        das = DistributedAtomSpace()
        load_metta_animals_base(das)
        traverse = das.get_traversal_cursor(human_handle)
        iterator = traverse.get_links(filter=self._is_expression_atom)
        self._check_asserts(das, iterator)

    def test_traverse_links_with_das_redis_mongo(self, human_handle, _cleanup):
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
        load_metta_animals_base(das)
        das.commit_changes()
        traverse = das.get_traversal_cursor(human_handle)
        iterator = traverse.get_links(filter=self._is_expression_atom)
        self._check_asserts(das, iterator)
        _db_down()

    def test_traverse_links_with_remote_das(self, human_handle):
        das = DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )
        traverse = das.get_traversal_cursor(human_handle)
        iterator = traverse.get_links(filter=self._is_expression_atom)
        self._check_asserts(das, iterator)


class TestTraverseNeighbors:
    @pytest.fixture(scope="session")
    def _cleanup(self, request):
        return cleanup(request)

    @pytest.fixture
    def human_handle(self):
        return metta_animal_base_handles.human

    def _human_neighbors(self):
        return sorted(
            [
                metta_animal_base_handles.mammal,
                metta_animal_base_handles.monkey,
                metta_animal_base_handles.chimp,
                metta_animal_base_handles.ent,
            ]
        )

    def _is_literal_atom(self, atom: dict) -> bool:
        if atom['is_literal'] is False:
            return False
        return True

    def _check_asserts(self, das: DistributedAtomSpace, iterator: TraverseNeighborsIterator):
        current_value = iterator.get()
        assert current_value == das.get_atom(iterator.get()['handle'])
        assert isinstance(current_value, dict)
        assert iterator.is_empty() is False
        node_handles = sorted([item['handle'] for item in iterator])
        assert len(node_handles) == 4
        assert node_handles == self._human_neighbors()
        assert iterator.is_empty() is True
        with pytest.raises(StopIteration):
            iterator.get()
        with pytest.raises(StopIteration):
            next(iterator)

    def test_traverse_neighbors_with_das_ram_only(self, human_handle):
        das = DistributedAtomSpace()
        load_metta_animals_base(das)
        traverse = das.get_traversal_cursor(human_handle)
        iterator = traverse.get_neighbors(filter=self._is_literal_atom)
        self._check_asserts(das, iterator)

    def test_traverse_neighbors_with_das_redis_mongo(self, human_handle, _cleanup):
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
        load_metta_animals_base(das)
        das.commit_changes()
        traverse = das.get_traversal_cursor(human_handle)
        iterator = traverse.get_neighbors(filter=self._is_literal_atom)
        self._check_asserts(das, iterator)
        _db_down()

    def test_traverse_neighbors_with_remote_das(self, human_handle):
        das = DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )
        traverse = das.get_traversal_cursor(human_handle)
        iterator = traverse.get_neighbors(filter=self._is_literal_atom)
        self._check_asserts(das, iterator)
