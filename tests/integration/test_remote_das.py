import pytest
from hyperon_das_atomdb import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

from hyperon_das import DistributedAtomSpace
from hyperon_das.exceptions import GetTraversalCursorException
from hyperon_das.traverse_engines import TraverseEngine

from .remote_das_info import remote_das_host, remote_das_port

human = ExpressionHasher.terminal_hash('Symbol', '"human"')
monkey = ExpressionHasher.terminal_hash('Symbol', '"monkey"')
chimp = ExpressionHasher.terminal_hash('Symbol', '"chimp"')
mammal = ExpressionHasher.terminal_hash('Symbol', '"mammal"')
ent = ExpressionHasher.terminal_hash('Symbol', '"ent"')
animal = ExpressionHasher.terminal_hash('Symbol', '"animal"')
reptile = ExpressionHasher.terminal_hash('Symbol', '"reptile"')
dinosaur = ExpressionHasher.terminal_hash('Symbol', '"dinosaur"')
triceratops = ExpressionHasher.terminal_hash('Symbol', '"triceratops"')
rhino = ExpressionHasher.terminal_hash('Symbol', '"rhino"')
earthworm = ExpressionHasher.terminal_hash('Symbol', '"earthworm"')
snake = ExpressionHasher.terminal_hash('Symbol', '"snake"')
vine = ExpressionHasher.terminal_hash('Symbol', '"vine"')
plant = ExpressionHasher.terminal_hash('Symbol', '"plant"')

expression = ExpressionHasher.named_type_hash('Expression')
similarity = ExpressionHasher.terminal_hash('Symbol', 'Similarity')
inheritance = ExpressionHasher.terminal_hash('Symbol', 'Inheritance')
concept = ExpressionHasher.terminal_hash('Symbol', 'Concept')

similarity_human_monkey = ExpressionHasher.expression_hash(expression, [similarity, human, monkey])
similarity_human_chimp = ExpressionHasher.expression_hash(expression, [similarity, human, chimp])
similarity_chimp_monkey = ExpressionHasher.expression_hash(expression, [similarity, chimp, monkey])
similarity_snake_earthworm = ExpressionHasher.expression_hash(
    expression, [similarity, snake, earthworm]
)
similarity_rhino_triceratops = ExpressionHasher.expression_hash(
    expression, [similarity, rhino, triceratops]
)
similarity_snake_vine = ExpressionHasher.expression_hash(expression, [similarity, snake, vine])
similarity_human_ent = ExpressionHasher.expression_hash(expression, [similarity, human, ent])
inheritance_human_mammal = ExpressionHasher.expression_hash(
    expression, [inheritance, human, mammal]
)
inheritance_monkey_mammal = ExpressionHasher.expression_hash(
    expression, [inheritance, monkey, mammal]
)
inheritance_chimp_mammal = ExpressionHasher.expression_hash(
    expression, [inheritance, chimp, mammal]
)
inheritance_mammal_animal = ExpressionHasher.expression_hash(
    expression, [inheritance, mammal, animal]
)
inheritance_reptile_animal = ExpressionHasher.expression_hash(
    expression, [inheritance, reptile, animal]
)
inheritance_snake_reptile = ExpressionHasher.expression_hash(
    expression, [inheritance, snake, reptile]
)
inheritance_dinosaur_reptile = ExpressionHasher.expression_hash(
    expression, [inheritance, dinosaur, reptile]
)
inheritance_triceratops_dinosaur = ExpressionHasher.expression_hash(
    expression, [inheritance, triceratops, dinosaur]
)
inheritance_earthworm_animal = ExpressionHasher.expression_hash(
    expression, [inheritance, earthworm, animal]
)
inheritance_rhino_mammal = ExpressionHasher.expression_hash(
    expression, [inheritance, rhino, mammal]
)
inheritance_vine_plant = ExpressionHasher.expression_hash(expression, [inheritance, vine, plant])
inheritance_ent_plant = ExpressionHasher.expression_hash(expression, [inheritance, ent, plant])
similarity_monkey_human = ExpressionHasher.expression_hash(expression, [similarity, monkey, human])
similarity_chimp_human = ExpressionHasher.expression_hash(expression, [similarity, chimp, human])
similarity_monkey_chimp = ExpressionHasher.expression_hash(expression, [similarity, monkey, chimp])
similarity_earthworm_snake = ExpressionHasher.expression_hash(
    expression, [similarity, earthworm, snake]
)
similarity_triceratops_rhino = ExpressionHasher.expression_hash(
    expression, [similarity, triceratops, rhino]
)
similarity_vine_snake = ExpressionHasher.expression_hash(expression, [similarity, vine, snake])
similarity_ent_human = ExpressionHasher.expression_hash(expression, [similarity, ent, human])


class TestRemoteDistributedAtomSpace:
    """Integration tests with OpenFaas function on the Vultr server. Using the Animal Knowledge Base"""

    @pytest.fixture
    def remote_das(self):
        return DistributedAtomSpace(
            query_engine='remote', host=remote_das_host, port=remote_das_port
        )  # vultr

    def traversal(self, das: DistributedAtomSpace, handle: str):
        return das.get_traversal_cursor(handle)

    def test_server_connection(self):
        try:
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )
        except Exception as e:
            pytest.fail(f'Connection with OpenFaaS server fail, Details: {str(e)}')
        if not das.query_engine.remote_das.url:
            pytest.fail('Connection with server fail')
        assert (
            das.query_engine.remote_das.url
            == f'http://{remote_das_host}:{remote_das_port}/function/query-engine'
        )

    def test_get_atom(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_atom(handle=human)
        assert result['handle'] == human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        result = remote_das.get_atom(handle=inheritance_dinosaur_reptile)
        assert result['handle'] == inheritance_dinosaur_reptile
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [inheritance, dinosaur, reptile]

        with pytest.raises(AtomDoesNotExist):
            remote_das.get_atom(handle='fake')

    def test_get_node(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_node(node_type='Symbol', node_name='"human"')
        assert result['handle'] == human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        with pytest.raises(NodeDoesNotExist):
            remote_das.get_node(node_type='Fake', node_name='fake')

    def test_get_link(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_link(
            link_type='Expression', link_targets=[inheritance, earthworm, animal]
        )
        assert result['handle'] == inheritance_earthworm_animal
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [inheritance, earthworm, animal]

        with pytest.raises(LinkDoesNotExist):
            remote_das.get_link(link_type='Fake', link_targets=['fake1', 'fake2'])

    # TODO: uncomment test after update package version
    # def test_get_links(self, remote_das: DistributedAtomSpace):
    #     all_inheritance = [
    #         inheritance_human_mammal,
    #         inheritance_monkey_mammal,
    #         inheritance_chimp_mammal,
    #         inheritance_mammal_animal,
    #         inheritance_reptile_animal,
    #         inheritance_snake_reptile,
    #         inheritance_dinosaur_reptile,
    #         inheritance_triceratops_dinosaur,
    #         inheritance_earthworm_animal,
    #         inheritance_rhino_mammal,
    #         inheritance_vine_plant,
    #         inheritance_ent_plant,
    #     ]

    #     links = remote_das.get_links(link_type='Inheritance')
    #     assert len(links) == 12
    #     assert set([link['handle'] for link in links]) == set(all_inheritance)

    #     links = remote_das.get_links(link_type='Inheritance', target_types=['Concept', 'Concept'])
    #     assert len(links) == 12
    #     assert set([link['handle'] for link in links]) == set(all_inheritance)

    #     links = remote_das.get_links(link_type='Inheritance', link_targets=[earthworm, animal])
    #     assert links[0]['handle'] == inheritance_earthworm_animal

    def test_get_incoming_links(self, remote_das: DistributedAtomSpace):
        expected_handles = [inheritance_vine_plant, similarity_snake_vine, similarity_vine_snake]

        expected_atoms = [remote_das.get_atom(handle) for handle in expected_handles]

        response_atoms = remote_das.get_incoming_links(vine, handles_only=False)
        for atom in response_atoms:
            if len(atom["targets"]) == 3:
                assert atom in expected_atoms

    def test_count_atoms(self, remote_das: DistributedAtomSpace):
        nodes = 21
        links = 43
        response = remote_das.count_atoms()
        assert response[0] == nodes
        assert response[1] == links

    def test_query(self, remote_das: DistributedAtomSpace):
        all_inheritance_mammal = [
            inheritance_chimp_mammal,
            inheritance_human_mammal,
            inheritance_monkey_mammal,
            inheritance_rhino_mammal,
        ]

        answer = remote_das.query(
            {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "Inheritance"},
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "node", "type": "Symbol", "name": '"mammal"'},
                ],
            },
            {'no_iterator': True},
        )

        assert len(answer) == 4

        for _, link in answer:
            assert link['handle'] in all_inheritance_mammal
            if link['handle'] == inheritance_chimp_mammal:
                assert link['targets'] == [
                    {'handle': inheritance, 'type': 'Symbol', 'name': "Inheritance"},
                    {'handle': chimp, 'type': 'Symbol', 'name': '"chimp"'},
                    {'handle': mammal, 'type': 'Symbol', 'name': '"mammal"'},
                ]
            elif link['handle'] == inheritance_human_mammal:
                assert link['targets'] == [
                    {'handle': inheritance, 'type': 'Symbol', 'name': "Inheritance"},
                    {'handle': human, 'type': 'Symbol', 'name': '"human"'},
                    {'handle': mammal, 'type': 'Symbol', 'name': '"mammal"'},
                ]
            elif link['handle'] == inheritance_monkey_mammal:
                assert link['targets'] == [
                    {'handle': inheritance, 'type': 'Symbol', 'name': "Inheritance"},
                    {'handle': monkey, 'type': 'Symbol', 'name': '"monkey"'},
                    {'handle': mammal, 'type': 'Symbol', 'name': '"mammal"'},
                ]
            elif link['handle'] == inheritance_rhino_mammal:
                assert link['targets'] == [
                    {'handle': inheritance, 'type': 'Symbol', 'name': "Inheritance"},
                    {'handle': rhino, 'type': 'Symbol', 'name': '"rhino"'},
                    {'handle': mammal, 'type': 'Symbol', 'name': '"mammal"'},
                ]

    def test_get_traversal_cursor(self, remote_das: DistributedAtomSpace):
        cursor = remote_das.get_traversal_cursor(human)
        assert cursor.get()['handle'] == human
        with pytest.raises(GetTraversalCursorException):
            remote_das.get_traversal_cursor('fake_handle')

    def test_traverse_engine_methods(self, remote_das: DistributedAtomSpace):
        cursor: TraverseEngine = self.traversal(remote_das, dinosaur)
        assert cursor.get()['handle'] == dinosaur

        links_iter = cursor.get_links()
        expected_links = [
            remote_das.get_atom(handle)
            for handle in [inheritance_dinosaur_reptile, inheritance_triceratops_dinosaur]
        ]
        count = len(expected_links)
        for link in links_iter:
            if len(link["targets"]) == 3:
                assert link in expected_links
                count -= 1
        assert count == 0

        # TODO: fix this test

        # neighbors_iter = cursor.get_neighbors(cursor_position=1)
        # assert neighbors_iter.get()['handle'] == reptile
        # atom = cursor.follow_link(cursor_position=1)
        # assert atom['handle'] == triceratops

        cursor.goto(human)
        assert cursor.get()['handle'] == human
