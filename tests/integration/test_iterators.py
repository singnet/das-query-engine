from typing import Union

import pytest
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher as hasher

from hyperon_das import DistributedAtomSpace
from hyperon_das.cache.iterators import (
    CustomQuery,
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
                metta_animal_base_handles.human_typedef,
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
        assert len(link_handles) == 9
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
                metta_animal_base_handles.human_typedef,
                metta_animal_base_handles.monkey_typedef,
                metta_animal_base_handles.chimp_typedef,
                metta_animal_base_handles.snake_typedef,
                metta_animal_base_handles.earthworm_typedef,
                metta_animal_base_handles.rhino_typedef,
                metta_animal_base_handles.triceratops_typedef,
                metta_animal_base_handles.vine_typedef,
                metta_animal_base_handles.ent_typedef,
                metta_animal_base_handles.mammal_typedef,
                metta_animal_base_handles.animal_typedef,
                metta_animal_base_handles.reptile_typedef,
                metta_animal_base_handles.dinosaur_typedef,
                metta_animal_base_handles.plant_typedef,
                metta_animal_base_handles.similarity_typedef,
                metta_animal_base_handles.inheritance_typedef,
                metta_animal_base_handles.concept_typedef,
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
        assert len(link_handles) == 43
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

    def test_get_links_with_remote_das(self, _cleanup):
        das = DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )
        iterator = das.get_links('Expression')
        self._check_asserts(das, iterator)


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
                metta_animal_base_handles.human_typedef,
            ]
        )

    def _check_asserts(self, das: DistributedAtomSpace, iterator: TraverseLinksIterator):
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


class TestCustomQuery:
    @pytest.fixture(scope="session")
    def _cleanup(self, request):
        return cleanup(request)

    def _all_nodes(self):
        return sorted(
            [
                metta_animal_base_handles.human,
                metta_animal_base_handles.monkey,
                metta_animal_base_handles.chimp,
                metta_animal_base_handles.mammal,
                metta_animal_base_handles.animal,
                metta_animal_base_handles.reptile,
                metta_animal_base_handles.snake,
                metta_animal_base_handles.dinosaur,
                metta_animal_base_handles.earthworm,
                metta_animal_base_handles.rhino,
                metta_animal_base_handles.plant,
                metta_animal_base_handles.vine,
                metta_animal_base_handles.ent,
                metta_animal_base_handles.triceratops,
            ]
        )

    def _all_links(self):
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
                metta_animal_base_handles.human_typedef,
                metta_animal_base_handles.monkey_typedef,
                metta_animal_base_handles.chimp_typedef,
                metta_animal_base_handles.snake_typedef,
                metta_animal_base_handles.earthworm_typedef,
                metta_animal_base_handles.rhino_typedef,
                metta_animal_base_handles.triceratops_typedef,
                metta_animal_base_handles.vine_typedef,
                metta_animal_base_handles.ent_typedef,
                metta_animal_base_handles.mammal_typedef,
                metta_animal_base_handles.animal_typedef,
                metta_animal_base_handles.reptile_typedef,
                metta_animal_base_handles.dinosaur_typedef,
                metta_animal_base_handles.plant_typedef,
                metta_animal_base_handles.similarity_typedef,
                metta_animal_base_handles.inheritance_typedef,
                metta_animal_base_handles.concept_typedef,
            ]
        )

    def _asserts(self, das: DistributedAtomSpace):
        node_index = das.create_field_index(atom_type='node', field='is_literal', type='Symbol')
        link_index_type = das.create_field_index(
            atom_type='link', field='is_toplevel', type='Expression'
        )
        link_index_composite_type = das.create_field_index(
            atom_type='link',
            field='is_toplevel',
            composite_type=['Expression', 'Symbol', 'Symbol', 'Symbol'],
        )

        node_iterator = das.custom_query(node_index, is_literal=True, no_iterator=False)
        link_iterator_type = das.custom_query(
            link_index_type, is_toplevel=True, chunk_size=10, no_iterator=False
        )
        link_iterator_composite_type = das.custom_query(
            link_index_composite_type, is_toplevel=True, chunk_size=5, no_iterator=False
        )

        nodes = self._check_asserts(das, node_iterator)
        links_type = self._check_asserts(das, link_iterator_type)
        links_composite_type = self._check_asserts(das, link_iterator_composite_type)

        assert sorted(nodes) == self._all_nodes()
        assert sorted(links_type) == self._all_links()
        assert sorted(links_composite_type) == self._all_links()

    def _check_asserts(self, das: DistributedAtomSpace, iterator: CustomQuery):
        current_value = iterator.get()
        assert current_value == das.get_atom(iterator.get()['handle'])
        assert isinstance(current_value, dict)
        assert iterator.is_empty() is False
        handles = sorted([item['handle'] for item in iterator])
        assert iterator.is_empty() is True
        with pytest.raises(StopIteration):
            iterator.get()
        with pytest.raises(StopIteration):
            next(iterator)
        return handles

    def test_custom_query_with_local_das_redis_mongo(self, _cleanup):
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
        self._asserts(das)
        _db_down()

    def test_custom_query_with_remote_das(self):
        das = DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )
        self._asserts(das)
