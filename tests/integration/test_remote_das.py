import pytest
from hyperon_das_atomdb import AtomDB, AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist

from hyperon_das import DistributedAtomSpace
from hyperon_das.exceptions import GetTraversalCursorException
from hyperon_das.traverse_engines import TraverseEngine

human = AtomDB.node_handle('Concept', 'human')
monkey = AtomDB.node_handle('Concept', 'monkey')
chimp = AtomDB.node_handle('Concept', 'chimp')
mammal = AtomDB.node_handle('Concept', 'mammal')
ent = AtomDB.node_handle('Concept', 'ent')
animal = AtomDB.node_handle('Concept', 'animal')
reptile = AtomDB.node_handle('Concept', 'reptile')
dinosaur = AtomDB.node_handle('Concept', 'dinosaur')
triceratops = AtomDB.node_handle('Concept', 'triceratops')
rhino = AtomDB.node_handle('Concept', 'rhino')
earthworm = AtomDB.node_handle('Concept', 'earthworm')
snake = AtomDB.node_handle('Concept', 'snake')
vine = AtomDB.node_handle('Concept', 'vine')
plant = AtomDB.node_handle('Concept', 'plant')

similarity_human_monkey = AtomDB.link_handle('Similarity', [human, monkey])
similarity_human_chimp = AtomDB.link_handle('Similarity', [human, chimp])
similarity_chimp_monkey = AtomDB.link_handle('Similarity', [chimp, monkey])
similarity_snake_earthworm = AtomDB.link_handle('Similarity', [snake, earthworm])
similarity_rhino_triceratops = AtomDB.link_handle('Similarity', [rhino, triceratops])
similarity_snake_vine = AtomDB.link_handle('Similarity', [snake, vine])
similarity_human_ent = AtomDB.link_handle('Similarity', [human, ent])
inheritance_human_mammal = AtomDB.link_handle('Inheritance', [human, mammal])
inheritance_monkey_mammal = AtomDB.link_handle('Inheritance', [monkey, mammal])
inheritance_chimp_mammal = AtomDB.link_handle('Inheritance', [chimp, mammal])
inheritance_mammal_animal = AtomDB.link_handle('Inheritance', [mammal, animal])
inheritance_reptile_animal = AtomDB.link_handle('Inheritance', [reptile, animal])
inheritance_snake_reptile = AtomDB.link_handle('Inheritance', [snake, reptile])
inheritance_dinosaur_reptile = AtomDB.link_handle('Inheritance', [dinosaur, reptile])
inheritance_triceratops_dinosaur = AtomDB.link_handle('Inheritance', [triceratops, dinosaur])
inheritance_earthworm_animal = AtomDB.link_handle('Inheritance', [earthworm, animal])
inheritance_rhino_mammal = AtomDB.link_handle('Inheritance', [rhino, mammal])
inheritance_vine_plant = AtomDB.link_handle('Inheritance', [vine, plant])
inheritance_ent_plant = AtomDB.link_handle('Inheritance', [ent, plant])
similarity_monkey_human = AtomDB.link_handle('Similarity', [monkey, human])
similarity_chimp_human = AtomDB.link_handle('Similarity', [chimp, human])
similarity_monkey_chimp = AtomDB.link_handle('Similarity', [monkey, chimp])
similarity_earthworm_snake = AtomDB.link_handle('Similarity', [earthworm, snake])
similarity_triceratops_rhino = AtomDB.link_handle('Similarity', [triceratops, rhino])
similarity_vine_snake = AtomDB.link_handle('Similarity', [vine, snake])
similarity_ent_human = AtomDB.link_handle('Similarity', [ent, human])


class TestRemoteDistributedAtomSpace:
    """Integration tests with OpenFaas function on the Vultr server. Using the Animal Knowledge Base"""

    @pytest.fixture
    def remote_das(self):
        return DistributedAtomSpace(query_engine='remote', host='45.63.85.59', port=8080)  # vultr

    def traversal(self, das: DistributedAtomSpace, handle: str):
        return das.get_traversal_cursor(handle)

    def test_server_connection(self):
        host = '45.63.85.59'
        port = 8080
        try:
            das = DistributedAtomSpace(query_engine='remote', host=host, port=port)
        except Exception as e:
            pytest.fail(f'Connection with OpenFaaS server fail, Details: {str(e)}')
        if not das.query_engine.remote_das.url:
            pytest.fail('Connection with server fail')
        assert das.query_engine.remote_das.url == f'http://{host}:{port}/function/query-engine'

    def test_get_atom(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_atom(handle=human)
        assert result['handle'] == human
        assert result['name'] == 'human'
        assert result['named_type'] == 'Concept'

        result = remote_das.get_atom(handle=inheritance_dinosaur_reptile)
        assert result['handle'] == inheritance_dinosaur_reptile
        assert result['named_type'] == 'Inheritance'
        assert result['targets'] == [dinosaur, reptile]

        with pytest.raises(AtomDoesNotExist):
            remote_das.get_atom(handle='fake')

    def test_get_node(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_node(node_type='Concept', node_name='human')
        assert result['handle'] == human
        assert result['name'] == 'human'
        assert result['named_type'] == 'Concept'

        with pytest.raises(NodeDoesNotExist):
            remote_das.get_node(node_type='Fake', node_name='fake')

    def test_get_link(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_link(link_type='Inheritance', link_targets=[earthworm, animal])
        assert result['handle'] == inheritance_earthworm_animal
        assert result['named_type'] == 'Inheritance'
        assert result['targets'] == [earthworm, animal]

        with pytest.raises(LinkDoesNotExist):
            remote_das.get_link(link_type='Fake', link_targets=['fake1', 'fake2'])

    def test_get_links(self, remote_das: DistributedAtomSpace):
        all_inheritance = [
            inheritance_human_mammal,
            inheritance_monkey_mammal,
            inheritance_chimp_mammal,
            inheritance_mammal_animal,
            inheritance_reptile_animal,
            inheritance_snake_reptile,
            inheritance_dinosaur_reptile,
            inheritance_triceratops_dinosaur,
            inheritance_earthworm_animal,
            inheritance_rhino_mammal,
            inheritance_vine_plant,
            inheritance_ent_plant,
        ]

        links = remote_das.get_links(link_type='Inheritance')
        assert len(links) == 12
        assert set([link['handle'] for link in links]) == set(all_inheritance)

        links = remote_das.get_links(link_type='Inheritance', target_types=['Concept', 'Concept'])
        assert len(links) == 12
        assert set([link['handle'] for link in links]) == set(all_inheritance)

        links = remote_das.get_links(link_type='Inheritance', link_targets=[earthworm, animal])
        assert links[0]['handle'] == inheritance_earthworm_animal

    # TODO: uncomment test after update package version
    # def test_get_incoming_links(self, remote_das: DistributedAtomSpace):
    #     expected_handles = [inheritance_vine_plant, similarity_snake_vine, similarity_vine_snake]

    #     expected_atoms = [remote_das.get_atom(handle) for handle in expected_handles]

    #     response_atoms = remote_das.get_incoming_links(vine, handles_only=False)
    #     for atom in response_atoms:
    #         assert atom in expected_atoms

    def test_count_atoms(self, remote_das: DistributedAtomSpace):
        nodes = 14
        links = 26
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
                "type": "Inheritance",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "node", "type": "Concept", "name": "mammal"},
                ],
            },
            {'no_iterator': True},
        )

        assert len(answer) == 4

        for _, link in answer:
            assert link['handle'] in all_inheritance_mammal
            if link['handle'] == inheritance_chimp_mammal:
                assert link['targets'] == [
                    {'handle': chimp, 'type': 'Concept', 'name': 'chimp'},
                    {'handle': mammal, 'type': 'Concept', 'name': 'mammal'},
                ]
            elif link['handle'] == inheritance_human_mammal:
                assert link['targets'] == [
                    {'handle': human, 'type': 'Concept', 'name': 'human'},
                    {'handle': mammal, 'type': 'Concept', 'name': 'mammal'},
                ]
            elif link['handle'] == inheritance_monkey_mammal:
                assert link['targets'] == [
                    {'handle': monkey, 'type': 'Concept', 'name': 'monkey'},
                    {'handle': mammal, 'type': 'Concept', 'name': 'mammal'},
                ]
            elif link['handle'] == inheritance_rhino_mammal:
                assert link['targets'] == [
                    {'handle': rhino, 'type': 'Concept', 'name': 'rhino'},
                    {'handle': mammal, 'type': 'Concept', 'name': 'mammal'},
                ]

    def test_get_traversal_cursor(self, remote_das: DistributedAtomSpace):
        cursor = remote_das.get_traversal_cursor(human)
        assert cursor.get()['handle'] == human
        with pytest.raises(GetTraversalCursorException):
            remote_das.get_traversal_cursor('fake_handle')

    # TODO: uncomment test after update package version
    # def test_traverse_engine_methods(self, remote_das: DistributedAtomSpace):
    #     cursor: TraverseEngine = self.traversal(remote_das, dinosaur)
    #     assert cursor.get()['handle'] == dinosaur

    #     links_iter = cursor.get_links()
    #     expected_links = [
    #         remote_das.get_atom(handle)
    #         for handle in [inheritance_dinosaur_reptile, inheritance_triceratops_dinosaur]
    #     ]
    #     for link in links_iter:
    #         assert link in expected_links

    #     neighbors_iter = cursor.get_neighbors(cursor_position=0)
    #     assert neighbors_iter.get()['handle'] == reptile

    #     atom = cursor.follow_link(cursor_position=1)
    #     assert atom['handle'] == triceratops

    #     cursor.goto(human)
    #     assert cursor.get()['handle'] == human
