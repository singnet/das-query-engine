import pytest
from hyperon_das_atomdb import AtomDoesNotExist
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

from hyperon_das.das import DistributedAtomSpace

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
metta_type = ExpressionHasher.terminal_hash('Symbol', 'MettaType')

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
metta_type_human_concept = ExpressionHasher.expression_hash(metta_type, [human, concept])
metta_type_mammal_concept = ExpressionHasher.expression_hash(metta_type, [mammal, concept])


class TestTraverseEngine:
    def test_traverse_engine_remote(self):
        """Test TraverseEngine methods with remote DAS in OpenFaas"""

        def get():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )
            cursor = das.get_traversal_cursor(human)
            current_cursor = cursor.get()

            assert current_cursor['handle'] == human
            assert current_cursor['name'] == '"human"'
            assert current_cursor['named_type'] == 'Symbol'

        def get_links():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )

            def _build_atom_answer(handle: str) -> dict:
                cursor = das.get_traversal_cursor(handle)
                links = cursor.get_links(link_type='Expression')
                return sorted([link['handle'] for link in links])

            def _human_links():
                answers = _build_atom_answer(human)
                assert answers == sorted(
                    [
                        similarity_human_chimp,
                        similarity_human_monkey,
                        similarity_human_ent,
                        similarity_ent_human,
                        similarity_monkey_human,
                        similarity_chimp_human,
                        inheritance_human_mammal,
                    ]
                )

            def _chimp_links():
                answers = _build_atom_answer(chimp)
                assert answers == sorted(
                    [
                        inheritance_chimp_mammal,
                        similarity_monkey_chimp,
                        similarity_chimp_monkey,
                        similarity_chimp_human,
                        similarity_human_chimp,
                    ]
                )

            def _ent_links():
                answers = _build_atom_answer(ent)
                assert answers == sorted(
                    [inheritance_ent_plant, similarity_human_ent, similarity_ent_human]
                )

            def _similarity_links():
                answers = _build_atom_answer(similarity_human_chimp)
                assert len(answers) == 0

                answers = _build_atom_answer(similarity_chimp_monkey)
                assert len(answers) == 0

                answers = _build_atom_answer(similarity_human_ent)
                assert len(answers) == 0

            _human_links()

            _chimp_links()

            _ent_links()

            _similarity_links()

        def get_links_with_filters():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )

            def _build_atom_answer(handle: str, **filters) -> dict:
                cursor = das.get_traversal_cursor(handle)
                links = cursor.get_links(**filters)
                return sorted([link['handle'] for link in links])

            def _human_links():
                answers = _build_atom_answer(human, link_type='Expression')
                assert answers == sorted(
                    [
                        similarity_human_chimp,
                        similarity_human_monkey,
                        similarity_human_ent,
                        similarity_ent_human,
                        similarity_monkey_human,
                        similarity_chimp_human,
                        inheritance_human_mammal,
                    ]
                )

                answers = _build_atom_answer(human, link_type='Expression', cursor_position=1)
                answers == sorted(
                    [
                        inheritance_human_mammal,
                        similarity_human_chimp,
                        similarity_human_monkey,
                        similarity_human_ent,
                    ]
                )

                answers = _build_atom_answer(human, link_type='Expression', cursor_position=2)
                assert answers == sorted(
                    [similarity_ent_human, similarity_monkey_human, similarity_chimp_human]
                )

                answers = _build_atom_answer(
                    human, link_type='Expression', cursor_position=1, target_type='Symbol'
                )
                assert answers == sorted(
                    [
                        inheritance_human_mammal,
                        similarity_human_chimp,
                        similarity_human_monkey,
                        similarity_human_ent,
                    ]
                )
                answers = _build_atom_answer(human, link_type='Fake')
                assert len(answers) == 0
                answers = _build_atom_answer(human, cursor_position=3)
                assert len(answers) == 0
                answers = _build_atom_answer(human, target_type='Fake')
                assert len(answers) == 0

                # das.add_link(
                #    {
                #        'type': 'Expression',
                #        'targets': [
                #            {'type': 'Symbol', 'name': 'Similarity'},
                #            {'type': 'Symbol', 'name': 'human'},
                #            {'type': 'Symbol', 'name': 'snet'},
                #        ],
                #        'weight': 0.5,
                #    }
                # )

                # def my_filter(link) -> bool:
                #    if 'weight' in link:
                #        return True
                #    return False

                # answers = _build_atom_answer(
                #    human,
                #    link_type='Expression',
                #    cursor_position=1,
                #    target_type='Symbol',
                #    filter=my_filter,
                # )
                # TODO Fix this test
                # assert answers == [
                #    AtomDB.link_handle('Expression', [similarity, human, AtomDB.node_handle('Symbol', 'snet')])
                # ]

                def my_second_filter(link):
                    if 'weight' in link and link['weight'] >= 0.5:
                        return link

                with pytest.raises(TypeError) as exc:
                    _build_atom_answer(human, filter=my_second_filter)
                assert exc.value.args[0] == 'The function must return a boolean'

            def _mammal_links():
                answers = _build_atom_answer(mammal, link_type='Expression')
                assert answers == sorted(
                    [
                        inheritance_mammal_animal,
                        inheritance_monkey_mammal,
                        inheritance_chimp_mammal,
                        inheritance_human_mammal,
                        inheritance_rhino_mammal,
                    ]
                )
                answers = _build_atom_answer(mammal, link_type='Expression', cursor_position=1)
                assert answers == sorted([inheritance_mammal_animal])
                answers = _build_atom_answer(mammal, link_type='Expression', cursor_position=2)
                assert answers == sorted(
                    [
                        inheritance_monkey_mammal,
                        inheritance_chimp_mammal,
                        inheritance_human_mammal,
                        inheritance_rhino_mammal,
                    ]
                )
                answers = _build_atom_answer(
                    mammal, link_type='Expression', cursor_position=2, target_type='Symbol'
                )
                assert answers == sorted(
                    [
                        inheritance_monkey_mammal,
                        inheritance_chimp_mammal,
                        inheritance_human_mammal,
                        inheritance_rhino_mammal,
                    ]
                )
                answers = _build_atom_answer(mammal, link_type='Similarity')
                assert len(answers) == 0
                answers = _build_atom_answer(mammal, cursor_position=5)
                assert len(answers) == 0
                answers = _build_atom_answer(mammal, target_type='Snet')
                assert len(answers) == 0

                # das.add_link(
                #    {
                #        'type': 'Inheritance',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake1'},
                #            {'type': 'Concept', 'name': 'mammal'},
                #        ],
                #        'weight': 0.4,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Inheritance',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake2'},
                #            {'type': 'Concept', 'name': 'mammal'},
                #        ],
                #        'weight': 0.5,
                #    }
                # )

                # def my_filter(link) -> bool:
                #    if 'weight' in link and link['weight'] >= 0.5:
                #        return True
                #    return False

                # TODO Fix this test
                # answers = _build_atom_answer(
                #    mammal,
                #    link_type='Inheritance',
                #    cursor_position=1,
                #    target_type='Fake',
                #    filter=my_filter,
                # )
                # assert answers == [
                #    AtomDB.link_handle('Inheritance', [AtomDB.node_handle('Fake', 'fake2'), mammal])
                # ]

            def _snake_links():
                answers = _build_atom_answer(snake, link_type='Expression')
                assert answers == sorted(
                    [
                        similarity_snake_earthworm,
                        similarity_earthworm_snake,
                        similarity_snake_vine,
                        similarity_vine_snake,
                        inheritance_snake_reptile,
                    ]
                )
                answers = _build_atom_answer(snake, link_type='Expression', cursor_position=1)
                assert answers == sorted(
                    [
                        similarity_snake_earthworm,
                        similarity_snake_vine,
                        inheritance_snake_reptile,
                    ]
                )
                answers = _build_atom_answer(snake, link_type='Expression', cursor_position=2)
                assert answers == sorted(
                    [
                        similarity_earthworm_snake,
                        similarity_vine_snake,
                    ]
                )
                answers = _build_atom_answer(
                    snake, link_type='Expression', cursor_position=1, target_type='Symbol'
                )
                assert answers == sorted(
                    [
                        similarity_snake_earthworm,
                        similarity_snake_vine,
                        inheritance_snake_reptile,
                    ]
                )
                answers = _build_atom_answer(snake, link_type='Evaluation')
                assert len(answers) == 0
                answers = _build_atom_answer(snake, cursor_position=5)
                assert len(answers) == 0
                answers = _build_atom_answer(
                    snake, link_type='Expression', cursor_position=0, target_type='Snet'
                )
                assert len(answers) == 0

                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake1'},
                #            {'type': 'Concept', 'name': 'snake'},
                #        ],
                #        'weight': 0.2,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Concept', 'name': 'snake'},
                #            {'type': 'Fake', 'name': 'fake1'},
                #        ],
                #        'weight': 0.5,
                #    }
                # )

                # def my_filter(link) -> bool:
                #    if 'weight' in link and link['weight'] >= 0.5:
                #        return True
                #    return False

                # answers = _build_atom_answer(
                #    snake, link_type='Similarity', target_type='Fake', filter=my_filter
                # )
                # TODO Fix this test
                # assert answers == [
                #    AtomDB.link_handle("Similarity", [snake, AtomDB.node_handle('Fake', 'fake1')])
                # ]

            def _similarity_human_monkey_links():
                answers = _build_atom_answer(similarity_human_monkey, link_type='Similarity')
                assert len(answers) == 0

                das.add_link(
                    {
                        'type': 'Inheritance',
                        'targets': [
                            {'type': 'Fake', 'name': 'fake'},
                            {
                                'type': 'Similarity',
                                'targets': [
                                    {'type': 'Concept', 'name': 'human'},
                                    {'type': 'Concept', 'name': 'monkey'},
                                ],
                            },
                        ],
                    }
                )

                cursor = das.get_traversal_cursor(similarity_human_monkey)
                links = cursor.get_links(link_type='Inheritance')
                answers = [link for link in links]
                assert len(answers) == 1

            _human_links()
            _mammal_links()
            _snake_links()
            # TODO Fix this test
            # _similarity_human_monkey_links()

        def get_neighbors():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )

            def _build_neighbors(handle: str) -> dict:
                cursor = das.get_traversal_cursor(handle)
                neighbors_iterator = cursor.get_neighbors()
                return [item for item in neighbors_iterator]

            def _monkey_neighbors():
                neighbors = _build_neighbors(monkey)
                assert das.get_atom(human) in neighbors
                assert das.get_atom(mammal) in neighbors
                assert das.get_atom(chimp) in neighbors
                assert len(neighbors) == 6

            def _dinosaur_neighbors():
                neighbors = _build_neighbors(dinosaur)
                assert das.get_atom(reptile) in neighbors
                assert das.get_atom(triceratops) in neighbors
                assert len(neighbors) == 4

            def _rhino_neighbors():
                neighbors = _build_neighbors(rhino)
                assert das.get_atom(mammal) in neighbors
                assert das.get_atom(triceratops) in neighbors
                assert len(neighbors) == 5

            def _inheritance_neighbors():
                answers = _build_neighbors(inheritance_monkey_mammal)
                assert len(answers) == 0

                answers = _build_neighbors(inheritance_dinosaur_reptile)
                assert len(answers) == 0

                answers = _build_neighbors(inheritance_rhino_mammal)
                assert len(answers) == 0

            _monkey_neighbors()

            _dinosaur_neighbors()

            _rhino_neighbors()

            _inheritance_neighbors()

        def get_neighbors_with_filters():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )

            def _build_neighbors(handle: str, **filters) -> dict:
                cursor = das.get_traversal_cursor(handle)
                neighbors_iterator = cursor.get_neighbors(**filters)
                ret = []
                for item in neighbors_iterator:
                    ret.append(item)
                return ret

            def _human_neighbors():
                neighbors = _build_neighbors(human, link_type='Expression', cursor_position=0)
                assert len(neighbors) == 0

                neighbors = _build_neighbors(human, link_type='Expression', cursor_position=1)
                assert das.get_atom(mammal) in neighbors
                assert das.get_atom(monkey) in neighbors
                assert das.get_atom(chimp) in neighbors
                assert das.get_atom(ent) in neighbors
                assert len(neighbors) == 6

                neighbors = _build_neighbors(human, link_type='Expression', cursor_position=2)
                assert das.get_atom(monkey) in neighbors
                assert das.get_atom(chimp) in neighbors
                assert das.get_atom(ent) in neighbors
                assert len(neighbors) == 4

                neighbors = _build_neighbors(human, link_type='Expression', cursor_position=3)
                assert len(neighbors) == 0

                neighbors = _build_neighbors(
                    human, link_type='Expression', cursor_position=2, target_type='Symbol'
                )
                assert das.get_atom(monkey) in neighbors
                assert das.get_atom(chimp) in neighbors
                assert das.get_atom(ent) in neighbors
                assert len(neighbors) == 4

                neighbors = _build_neighbors(
                    human, link_type='Expression', cursor_position=2, target_type='fake'
                )
                assert len(neighbors) == 0

                neighbors = _build_neighbors(human, link_type='Inheritance', target_type='Snet')
                assert len(neighbors) == 0

                neighbors = _build_neighbors(human, link_type='Similarity', target_type='Snet')
                assert len(neighbors) == 0

                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Concept', 'name': 'human'},
                #            {'type': 'Fake', 'name': 'fake-h'},
                #        ],
                #        'weight': 0.7,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake-h'},
                #            {'type': 'Concept', 'name': 'human'},
                #        ],
                #        'weight': 0.3,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Inheritance',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake-h2'},
                #            {'type': 'Fake', 'name': 'fake-h3'},
                #            {'type': 'Fake', 'name': 'fake-h4'},
                #            {'type': 'Concept', 'name': 'human'},
                #        ],
                #        'weight': 0.3,
                #    }
                # )

                # def my_filter(link) -> bool:
                #    if 'weight' in link and link['weight'] >= 1:
                #        return True
                #    return False

                # fake_h = AtomDB.node_handle('Fake', 'fake-h')
                # fake_h2 = AtomDB.node_handle('Fake', 'fake-h2')
                # fake_h3 = AtomDB.node_handle('Fake', 'fake-h3')
                # fake_h4 = AtomDB.node_handle('Fake', 'fake-h4')

                # TODO Fix this test
                # neighbors = _build_neighbors(human)
                # assert das.get_atom(mammal) in neighbors
                # assert das.get_atom(monkey) in neighbors
                # assert das.get_atom(chimp) in neighbors
                # assert das.get_atom(ent) in neighbors
                # assert das.get_atom(fake_h) in neighbors
                # assert das.get_atom(fake_h2) in neighbors
                # assert das.get_atom(fake_h3) in neighbors
                # assert das.get_atom(fake_h4) in neighbors
                # assert len(neighbors) == 8

                # TODO Fix this test
                # neighbors = _build_neighbors(human, link_type='Similarity', target_type='Fake')
                # assert das.get_atom(fake_h) in neighbors
                # assert len(neighbors) == 1

                # TODO Fix this test
                # neighbors = _build_neighbors(
                #    human, link_type='Similarity', target_type='Fake', filter=my_filter
                # )
                # assert len(neighbors) == 0

            def _vine_neighbors():
                neighbors = _build_neighbors(vine, link_type='Expression')
                assert das.get_atom(snake) in neighbors
                assert das.get_atom(plant) in neighbors
                assert len(neighbors) == 4

                neighbors = _build_neighbors(vine, link_type='Expression', cursor_position=1)
                assert das.get_atom(snake) in neighbors
                assert das.get_atom(plant) in neighbors
                assert len(neighbors) == 4

                neighbors = _build_neighbors(vine, link_type='Expression', cursor_position=2)
                assert das.get_atom(snake) in neighbors
                assert len(neighbors) == 2

                # fake_v1 = AtomDB.node_handle('Fake', 'fake-v1')
                # fake_v2 = AtomDB.node_handle('Fake', 'fake-v2')

                # das.add_link(
                #    {
                #        'type': 'Inheritance',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake-v1'},
                #            {'type': 'Concept', 'name': 'vine'},
                #        ],
                #        'weight': 1,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Concept', 'name': 'vine'},
                #            {'type': 'Fake', 'name': 'fake-v2'},
                #        ],
                #        'weight': 0.7,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake-v2'},
                #            {'type': 'Concept', 'name': 'vine'},
                #        ],
                #        'weight': 0.3,
                #    }
                # )

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Inheritance')
                # assert das.get_atom(plant) in neighbors
                # assert das.get_atom(fake_v1) in neighbors
                # assert len(neighbors) == 2

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Similarity')
                # assert das.get_atom(snake) in neighbors
                # assert das.get_atom(fake_v2) in neighbors
                # assert len(neighbors) == 2

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Similarity', target_type='Concept')
                # assert das.get_atom(snake) in neighbors
                # assert len(neighbors) == 1

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Inheritance', target_type='Fake')
                # assert das.get_atom(fake_v1) in neighbors
                # assert len(neighbors) == 1

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Similarity', target_type='Fake')
                # assert das.get_atom(fake_v2) in neighbors
                # assert len(neighbors) == 1

                # def my_filter(link) -> bool:
                #    if 'weight' in link and link['weight'] >= 1:
                #        return True
                #    return False

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Similarity', filter=my_filter)
                # assert len(neighbors) == 0

                # TODO Fix this test
                # neighbors = _build_neighbors(vine, link_type='Inheritance', filter=my_filter)
                # assert das.get_atom(fake_v1) in neighbors
                # assert len(neighbors) == 1

            def _inheritance_dinosaur_reptile():
                neighbors = _build_neighbors(inheritance_dinosaur_reptile)
                assert len(neighbors) == 0

                # fake_dr1 = AtomDB.node_handle('Fake', 'fake-dr1')
                # fake_dr2 = AtomDB.node_handle('Fake', 'fake-dr2')

                # das.add_link(
                #    {
                #        'type': 'Inheritance',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake-dr1'},
                #            {
                #                'type': 'Inheritance',
                #                'targets': [
                #                    {'type': 'Concept', 'name': 'dinosaur'},
                #                    {'type': 'Concept', 'name': 'reptile'},
                #                ],
                #            },
                #        ],
                #        'weight': 1,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {
                #                'type': 'Inheritance',
                #                'targets': [
                #                    {'type': 'Concept', 'name': 'dinosaur'},
                #                    {'type': 'Concept', 'name': 'reptile'},
                #                ],
                #            },
                #            {'type': 'Fake', 'name': 'fake-dr2'},
                #        ],
                #        'weight': 0.7,
                #    }
                # )
                # das.add_link(
                #    {
                #        'type': 'Similarity',
                #        'targets': [
                #            {'type': 'Fake', 'name': 'fake-dr2'},
                #            {
                #                'type': 'Inheritance',
                #                'targets': [
                #                    {'type': 'Concept', 'name': 'dinosaur'},
                #                    {'type': 'Concept', 'name': 'reptile'},
                #                ],
                #            },
                #        ],
                #        'weight': 0.3,
                #    }
                # )

                # TODO Fix this test
                # neighbors = _build_neighbors(inheritance_dinosaur_reptile)
                # assert das.get_atom(fake_dr1) in neighbors
                # assert das.get_atom(fake_dr2) in neighbors
                # assert len(neighbors) == 2

                # TODO Fix this test
                # neighbors = _build_neighbors(inheritance_dinosaur_reptile, link_type='Similarity')
                # assert das.get_atom(fake_dr2) in neighbors
                # assert len(neighbors) == 1

                # TODO Fix this test
                # neighbors = _build_neighbors(inheritance_dinosaur_reptile, link_type='Inheritance')
                # assert das.get_atom(fake_dr1) in neighbors
                # assert len(neighbors) == 1

                # TODO Fix this test
                # neighbors = _build_neighbors(
                #    inheritance_dinosaur_reptile, link_type='Inheritance', target_type='Fake'
                # )
                # assert das.get_atom(fake_dr1) in neighbors
                # assert len(neighbors) == 1

                # TODO Fix this test
                # neighbors = _build_neighbors(
                #    inheritance_dinosaur_reptile, link_type='Inheritance', target_type='Concept'
                # )
                # assert len(neighbors) == 0

                # TODO Fix this test
                # neighbors = _build_neighbors(inheritance_dinosaur_reptile, target_type='Concept')
                # assert len(neighbors) == 0

                # TODO Fix this test
                # neighbors = _build_neighbors(inheritance_dinosaur_reptile, target_type='Fake')
                # assert das.get_atom(fake_dr1) in neighbors
                # assert das.get_atom(fake_dr2) in neighbors
                # assert len(neighbors) == 2

            _human_neighbors()
            _vine_neighbors()
            _inheritance_dinosaur_reptile()

        def follow_link():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )

            def _mammal():
                cursor = das.get_traversal_cursor(mammal)
                assert cursor.get()['name'] == '"mammal"'

                cursor.follow_link(link_type='Expression')
                current_cursor = cursor.get()['name']
                assert current_cursor in ('"monkey"', '"chimp"', '"human"', '"animal"', '"rhino"')

                cursor.follow_link()
                previous_cursor = current_cursor
                current_cursor = cursor.get()['name']
                if previous_cursor == 'monkey':
                    assert current_cursor in ('mammal', 'chimp', 'human')
                elif previous_cursor == 'chimp':
                    assert current_cursor in ('mammal', 'monkey', 'human')
                elif previous_cursor == 'human':
                    assert current_cursor in ('mammal', 'monkey', 'chimp', 'ent')
                elif previous_cursor == 'animal':
                    assert current_cursor in ('mammal', 'reptile', 'earthworm')
                elif previous_cursor == 'rhino':
                    assert current_cursor in ('mammal', 'triceratops')

            def _earthworm():
                cursor = das.get_traversal_cursor(earthworm)
                assert cursor.get()['name'] == 'earthworm'

                cursor.follow_link()
                current_cursor = cursor.get()['name']
                assert current_cursor in ('animal', 'snake')

                cursor.follow_link()
                previous_cursor = current_cursor
                current_cursor = cursor.get()['name']

                if previous_cursor == 'animal':
                    assert current_cursor in ('mammal', 'reptile', 'earthworm')
                elif previous_cursor == 'snake':
                    assert current_cursor in ('earthworm', 'reptile', 'vine')

            _mammal()
            _earthworm()

        def follow_link_with_filters():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )

            def _mammal():
                cursor = das.get_traversal_cursor(mammal)
                assert cursor.get()['name'] == 'mammal'

                cursor.follow_link(link_type='Similarity')
                assert cursor.get()['name'] == 'mammal'

                cursor.follow_link(link_type='Inheritance')
                current_cursor = cursor.get()['name']
                assert current_cursor in ('monkey', 'chimp', 'human', 'animal', 'rhino')

                cursor.follow_link(link_type='Inheritance', target_type='Concept')
                previous_cursor = current_cursor
                current_cursor = cursor.get()['name']
                if previous_cursor == 'monkey':
                    assert current_cursor in ('mammal', 'chimp')
                elif previous_cursor == 'chimp':
                    assert current_cursor in ('mammal', 'monkey', 'human')
                elif previous_cursor == 'human':
                    assert current_cursor in ('mammal', 'monkey', 'chimp', 'ent')
                elif previous_cursor == 'animal':
                    assert current_cursor in ('mammal', 'reptile', 'earthworm')
                elif previous_cursor == 'rhino':
                    assert current_cursor in ('mammal', 'triceratops')

                cursor.follow_link(link_type='Inheritance', target_type='Fake')
                previous_cursor = current_cursor
                current_cursor = cursor.get()['name']
                assert previous_cursor == current_cursor

            _mammal()

        def goto():
            das = DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )
            cursor = das.get_traversal_cursor(human)
            cursor.get()['name'] == '"human"'

            cursor.goto(ent)
            assert cursor.get()['name'] == '"ent"'

            with pytest.raises(AtomDoesNotExist):
                cursor.goto('snet')

        get()
        get_links()
        get_links_with_filters()
        get_neighbors()
        get_neighbors_with_filters()
        # TODO Redesign these tests
        # follow_link()
        # follow_link_with_filters()
        goto()
