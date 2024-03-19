import pytest
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher as hasher

from hyperon_das.das import DistributedAtomSpace

from .helpers import (
    _db_down,
    _db_up,
    cleanup,
    load_metta_animals_base,
    metta_animal_base_handles,
    mongo_port,
    redis_port,
)
from .remote_das_info import remote_das_host, remote_das_port


class TestTraverseEngine:
    @pytest.fixture(scope="session")
    def _cleanup(self, request):
        return cleanup(request)

    def _check_asserts(self, das: DistributedAtomSpace):
        traverse_1 = das.get_traversal_cursor(metta_animal_base_handles.similarity_chimp_human)
        links_1 = traverse_1.get_links()
        traverse_2 = das.get_traversal_cursor(
            metta_animal_base_handles.inheritance_dinosaur_reptile
        )
        links_2 = traverse_2.get_links()
        assert len([i for i in links_1]) == 0
        assert len([i for i in links_2]) == 0

        # Initialize Traverse
        traverse = das.get_traversal_cursor(handle=metta_animal_base_handles.human)

        # Get
        current_cursor = traverse.get()
        assert current_cursor['handle'] == metta_animal_base_handles.human
        assert current_cursor['name'] == '"human"'
        assert current_cursor['named_type'] == 'Symbol'

        # Get links
        links = traverse.get_links()
        link_handles = sorted([link['handle'] for link in links])
        assert link_handles == sorted(
            [
                metta_animal_base_handles.similarity_human_chimp,
                metta_animal_base_handles.similarity_human_monkey,
                metta_animal_base_handles.similarity_human_ent,
                metta_animal_base_handles.similarity_ent_human,
                metta_animal_base_handles.similarity_monkey_human,
                metta_animal_base_handles.similarity_chimp_human,
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

        # Get links with filters
        links = traverse.get_links(link_type='Expression', cursor_position=1, target_type='Symbol')
        link_handles = sorted([link['handle'] for link in links])
        assert link_handles == sorted(
            [
                metta_animal_base_handles.similarity_human_chimp,
                metta_animal_base_handles.similarity_human_monkey,
                metta_animal_base_handles.similarity_human_ent,
                metta_animal_base_handles.inheritance_human_mammal,
            ]
        )

        # Get neighbors
        neighbors = traverse.get_neighbors()
        neighbors_handles = sorted([neighbor['handle'] for neighbor in neighbors])
        assert neighbors_handles == sorted(
            [
                metta_animal_base_handles.chimp,
                metta_animal_base_handles.monkey,
                metta_animal_base_handles.ent,
                metta_animal_base_handles.mammal,
                metta_animal_base_handles.Concept,
                metta_animal_base_handles.Similarity,
                metta_animal_base_handles.Inheritance,
            ]
        )

        # Get neighbors with filters
        def is_literal(atom: dict) -> bool:
            return atom['is_literal'] is True

        neighbors = traverse.get_neighbors(
            link_type='Expression', cursor_position=2, target_type='Symbol', filter=is_literal
        )
        neighbors_handles = sorted([neighbor['handle'] for neighbor in neighbors])
        assert neighbors_handles == sorted(
            [
                metta_animal_base_handles.chimp,
                metta_animal_base_handles.monkey,
                metta_animal_base_handles.ent,
            ]
        )

        # Follow link
        traverse.follow_link()
        expected_neighbors = [
            metta_animal_base_handles.chimp,
            metta_animal_base_handles.monkey,
            metta_animal_base_handles.ent,
            metta_animal_base_handles.mammal,
            metta_animal_base_handles.Concept,
            metta_animal_base_handles.Similarity,
            metta_animal_base_handles.Inheritance,
        ]
        assert traverse.get()['handle'] in expected_neighbors

        # Follow link with filters
        def is_ent(atom: dict) -> bool:
            return atom['name'] == '"ent"'

        traverse.goto(metta_animal_base_handles.human)
        traverse.follow_link(
            link_type='Expression', cursor_position=2, target_type='Symbol', filter=is_ent
        )
        assert traverse.get()['name'] == '"ent"'

    def test_traverse_engine_with_das_ram_only(self):
        das = DistributedAtomSpace()
        load_metta_animals_base(das)
        self._check_asserts(das)

    def test_traverse_engine_with_das_redis_mongo(self, _cleanup):
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
        self._check_asserts(das)
        _db_down()

    def test_traverse_engine_with_remote_das(self):
        das = DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )
        self._check_asserts(das)
