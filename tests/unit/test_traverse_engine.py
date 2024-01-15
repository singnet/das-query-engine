import pytest
from hyperon_das_atomdb import AtomDB

from hyperon_das.das import DistributedAtomSpace
from hyperon_das.exceptions import MultiplePathsError

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


def up_knowledge_base_animals(das: DistributedAtomSpace) -> None:
    all_links = [
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'chimp'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'chimp'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'snake'},
                {'type': 'Concept', 'name': 'earthworm'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'rhino'},
                {'type': 'Concept', 'name': 'triceratops'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'snake'},
                {'type': 'Concept', 'name': 'vine'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'ent'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'monkey'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'chimp'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'mammal'},
                {'type': 'Concept', 'name': 'animal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'reptile'},
                {'type': 'Concept', 'name': 'animal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'snake'},
                {'type': 'Concept', 'name': 'reptile'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'dinosaur'},
                {'type': 'Concept', 'name': 'reptile'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'triceratops'},
                {'type': 'Concept', 'name': 'dinosaur'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'earthworm'},
                {'type': 'Concept', 'name': 'animal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'rhino'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'vine'},
                {'type': 'Concept', 'name': 'plant'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'ent'},
                {'type': 'Concept', 'name': 'plant'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'monkey'},
                {'type': 'Concept', 'name': 'human'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'chimp'},
                {'type': 'Concept', 'name': 'human'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'monkey'},
                {'type': 'Concept', 'name': 'chimp'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'earthworm'},
                {'type': 'Concept', 'name': 'snake'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'triceratops'},
                {'type': 'Concept', 'name': 'rhino'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'vine'},
                {'type': 'Concept', 'name': 'snake'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'ent'},
                {'type': 'Concept', 'name': 'human'},
            ],
        },
    ]

    for link in all_links:
        das.add_link(link)


def simplify_links(links: list, das: DistributedAtomSpace) -> set:
    answers = set()
    for link in links:
        targets_name = []
        for target in link['targets']:
            targets_name.append(das.get_atom(target)['name'])
        answers.add(f"{link['named_type']} : {targets_name}")
    return answers


def get_names(handles: str, das: DistributedAtomSpace) -> set:
    answer = set()
    for handle in handles:
        answer.add(das.get_atom(handle)['name'])
    return answer


class TestDocumentTraverseEngine:
    @pytest.fixture
    def das(self):
        das = DistributedAtomSpace()
        up_knowledge_base_animals(das)
        return das

    def test_get(self, das):
        cursor = das.get_traversal_cursor(human)
        current_cursor = cursor.get()

        assert current_cursor['handle'] == human
        assert current_cursor['name'] == 'human'
        assert current_cursor['named_type'] == 'Concept'

    def test_get_links(self, das):
        def _build_atom_answer(handle: str) -> dict:
            cursor = das.get_traversal_cursor(handle)
            links = cursor.get_links()
            return simplify_links(links, das)

        def _human_links():
            answers = _build_atom_answer(human)
            assert len(answers) == 7
            assert answers == {
                "Similarity : ['human', 'chimp']",
                "Similarity : ['human', 'monkey']",
                "Similarity : ['human', 'ent']",
                "Similarity : ['ent', 'human']",
                "Similarity : ['monkey', 'human']",
                "Similarity : ['chimp', 'human']",
                "Inheritance : ['human', 'mammal']",
            }

        def _monkey_links():
            answers = _build_atom_answer(monkey)
            assert len(answers) == 5
            assert answers == {
                "Inheritance : ['monkey', 'mammal']",
                "Similarity : ['monkey', 'chimp']",
                "Similarity : ['chimp', 'monkey']",
                "Similarity : ['monkey', 'human']",
                "Similarity : ['human', 'monkey']",
            }

        def _chimp_links():
            answers = _build_atom_answer(chimp)
            assert len(answers) == 5
            assert answers == {
                "Inheritance : ['chimp', 'mammal']",
                "Similarity : ['monkey', 'chimp']",
                "Similarity : ['chimp', 'monkey']",
                "Similarity : ['chimp', 'human']",
                "Similarity : ['human', 'chimp']",
            }

        def _mammal_links():
            answers = _build_atom_answer(mammal)
            assert len(answers) == 5
            assert answers == {
                "Inheritance : ['mammal', 'animal']",
                "Inheritance : ['monkey', 'mammal']",
                "Inheritance : ['chimp', 'mammal']",
                "Inheritance : ['human', 'mammal']",
                "Inheritance : ['rhino', 'mammal']",
            }

        def _ent_links():
            answers = _build_atom_answer(ent)
            assert len(answers) == 3
            assert answers == {
                "Inheritance : ['ent', 'plant']",
                "Similarity : ['human', 'ent']",
                "Similarity : ['ent', 'human']",
            }

        def _animal_links():
            answers = _build_atom_answer(animal)
            assert len(answers) == 3
            assert answers == {
                "Inheritance : ['mammal', 'animal']",
                "Inheritance : ['reptile', 'animal']",
                "Inheritance : ['earthworm', 'animal']",
            }

        def _reptile_links():
            answers = _build_atom_answer(reptile)
            assert len(answers) == 3
            assert answers == {
                "Inheritance : ['dinosaur', 'reptile']",
                "Inheritance : ['snake', 'reptile']",
                "Inheritance : ['reptile', 'animal']",
            }

        def _dinosaur_links():
            answers = _build_atom_answer(dinosaur)
            assert len(answers) == 2
            assert answers == {
                "Inheritance : ['dinosaur', 'reptile']",
                "Inheritance : ['triceratops', 'dinosaur']",
            }

        def _triceratops_links():
            answers = _build_atom_answer(triceratops)
            assert len(answers) == 3
            assert answers == {
                "Similarity : ['rhino', 'triceratops']",
                "Similarity : ['triceratops', 'rhino']",
                "Inheritance : ['triceratops', 'dinosaur']",
            }

        def _rhino_links():
            answers = _build_atom_answer(rhino)
            assert len(answers) == 3
            assert answers == {
                "Similarity : ['rhino', 'triceratops']",
                "Similarity : ['triceratops', 'rhino']",
                "Inheritance : ['rhino', 'mammal']",
            }

        def _earthworm_links():
            answers = _build_atom_answer(earthworm)
            assert len(answers) == 3
            assert answers == {
                "Similarity : ['snake', 'earthworm']",
                "Similarity : ['earthworm', 'snake']",
                "Inheritance : ['earthworm', 'animal']",
            }

        def _snake_links():
            answers = _build_atom_answer(snake)
            assert len(answers) == 5
            assert answers == {
                "Similarity : ['snake', 'earthworm']",
                "Similarity : ['earthworm', 'snake']",
                "Inheritance : ['snake', 'reptile']",
                "Similarity : ['snake', 'vine']",
                "Similarity : ['vine', 'snake']",
            }

        def _vine_links():
            answers = _build_atom_answer(vine)
            assert len(answers) == 3
            assert answers == {
                "Inheritance : ['vine', 'plant']",
                "Similarity : ['snake', 'vine']",
                "Similarity : ['vine', 'snake']",
            }

        def _plant_links():
            answers = _build_atom_answer(plant)
            assert len(answers) == 2
            assert answers == {"Inheritance : ['vine', 'plant']", "Inheritance : ['ent', 'plant']"}

        def _similarity_inheritance_links():
            answers = _build_atom_answer(similarity_human_monkey)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_human_chimp)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_chimp_monkey)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_snake_earthworm)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_rhino_triceratops)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_snake_vine)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_human_ent)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_human_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_monkey_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_chimp_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_mammal_animal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_reptile_animal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_snake_reptile)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_dinosaur_reptile)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_triceratops_dinosaur)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_earthworm_animal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_rhino_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_vine_plant)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_ent_plant)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_monkey_human)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_chimp_human)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_monkey_chimp)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_earthworm_snake)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_triceratops_rhino)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_vine_snake)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_ent_human)
            assert len(answers) == 0

        _human_links()
        _monkey_links()
        _chimp_links()
        _mammal_links()
        _ent_links()
        _animal_links()
        _reptile_links()
        _dinosaur_links()
        _triceratops_links()
        _rhino_links()
        _earthworm_links()
        _snake_links()
        _vine_links()
        _plant_links()
        _similarity_inheritance_links()

    def test_get_links_with_filters(self, das):
        def _build_atom_answer(handle: str, **filters) -> dict:
            cursor = das.get_traversal_cursor(handle)
            links = cursor.get_links(**filters)
            return simplify_links(links, das)

        def _human_links():
            answers = _build_atom_answer(human, link_type='Similarity')
            assert answers == {
                "Similarity : ['human', 'chimp']",
                "Similarity : ['human', 'monkey']",
                "Similarity : ['human', 'ent']",
                "Similarity : ['ent', 'human']",
                "Similarity : ['monkey', 'human']",
                "Similarity : ['chimp', 'human']",
            }
            answers = _build_atom_answer(human, link_type='Similarity', cursor_position=0)
            assert answers == {
                "Similarity : ['human', 'chimp']",
                "Similarity : ['human', 'monkey']",
                "Similarity : ['human', 'ent']",
            }
            answers = _build_atom_answer(human, link_type='Similarity', cursor_position=1)
            assert answers == {
                "Similarity : ['ent', 'human']",
                "Similarity : ['monkey', 'human']",
                "Similarity : ['chimp', 'human']",
            }
            answers = _build_atom_answer(
                human, link_type='Similarity', cursor_position=0, target_type='Concept'
            )
            assert answers == {
                "Similarity : ['human', 'chimp']",
                "Similarity : ['human', 'monkey']",
                "Similarity : ['human', 'ent']",
            }
            answers = _build_atom_answer(human, link_type='Fake')
            assert len(answers) == 0
            answers = _build_atom_answer(human, cursor_position=2)
            assert len(answers) == 0
            answers = _build_atom_answer(human, target_type='Fake')
            assert len(answers) == 0

            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'human'},
                        {'type': 'Concept', 'name': 'snet'},
                    ],
                    'weight': 0.5,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link:
                    return True
                return False

            answers = _build_atom_answer(
                human,
                link_type='Similarity',
                cursor_position=0,
                target_type='Concept',
                filter=my_filter,
            )
            assert answers == {"Similarity : ['human', 'snet']"}

            def my_second_filter(link):
                if 'weight' in link and link['weight'] >= 0.5:
                    return link

            with pytest.raises(TypeError) as exc:
                _build_atom_answer(human, filter=my_second_filter)
            assert exc.value.args[0] == 'The function must return a boolean'

        def _mammal_links():
            answers = _build_atom_answer(mammal, link_type='Inheritance')
            assert answers == {
                "Inheritance : ['mammal', 'animal']",
                "Inheritance : ['monkey', 'mammal']",
                "Inheritance : ['chimp', 'mammal']",
                "Inheritance : ['human', 'mammal']",
                "Inheritance : ['rhino', 'mammal']",
            }
            answers = _build_atom_answer(mammal, link_type='Inheritance', cursor_position=0)
            assert answers == {"Inheritance : ['mammal', 'animal']"}
            answers = _build_atom_answer(mammal, link_type='Inheritance', cursor_position=1)
            assert answers == {
                "Inheritance : ['monkey', 'mammal']",
                "Inheritance : ['chimp', 'mammal']",
                "Inheritance : ['human', 'mammal']",
                "Inheritance : ['rhino', 'mammal']",
            }
            answers = _build_atom_answer(
                mammal, link_type='Inheritance', cursor_position=1, target_type='Concept'
            )
            assert answers == {
                "Inheritance : ['monkey', 'mammal']",
                "Inheritance : ['chimp', 'mammal']",
                "Inheritance : ['human', 'mammal']",
                "Inheritance : ['rhino', 'mammal']",
            }
            answers = _build_atom_answer(mammal, link_type='Similarity')
            assert len(answers) == 0
            answers = _build_atom_answer(mammal, cursor_position=5)
            assert len(answers) == 0
            answers = _build_atom_answer(mammal, target_type='Snet')
            assert len(answers) == 0

            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake1'},
                        {'type': 'Concept', 'name': 'mammal'},
                    ],
                    'weight': 0.4,
                }
            )
            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake2'},
                        {'type': 'Concept', 'name': 'mammal'},
                    ],
                    'weight': 0.5,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 0.5:
                    return True
                return False

            answers = _build_atom_answer(
                mammal,
                link_type='Inheritance',
                cursor_position=1,
                target_type='Fake',
                filter=my_filter,
            )
            assert answers == {"Inheritance : ['fake2', 'mammal']"}

        def _snake_links():
            answers = _build_atom_answer(snake, link_type='Similarity')
            assert answers == {
                "Similarity : ['snake', 'earthworm']",
                "Similarity : ['earthworm', 'snake']",
                "Similarity : ['snake', 'vine']",
                "Similarity : ['vine', 'snake']",
            }
            answers = _build_atom_answer(snake, link_type='Inheritance', cursor_position=0)
            assert answers == {
                "Inheritance : ['snake', 'reptile']",
            }
            answers = _build_atom_answer(snake, link_type='Inheritance', cursor_position=1)
            assert len(answers) == 0
            answers = _build_atom_answer(snake, link_type='Similarity', cursor_position=0)
            assert answers == {
                "Similarity : ['snake', 'earthworm']",
                "Similarity : ['snake', 'vine']",
            }
            answers = _build_atom_answer(snake, link_type='Similarity', cursor_position=1)
            assert answers == {
                "Similarity : ['earthworm', 'snake']",
                "Similarity : ['vine', 'snake']",
            }
            answers = _build_atom_answer(
                snake, link_type='Inheritance', cursor_position=0, target_type='Concept'
            )
            assert answers == {"Inheritance : ['snake', 'reptile']"}
            answers = _build_atom_answer(snake, link_type='Evaluation')
            assert len(answers) == 0
            answers = _build_atom_answer(snake, cursor_position=5)
            assert len(answers) == 0
            answers = _build_atom_answer(
                snake, link_type='Inheritance', cursor_position=0, target_type='Snet'
            )
            assert len(answers) == 0

            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake1'},
                        {'type': 'Concept', 'name': 'snake'},
                    ],
                    'weight': 0.2,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'snake'},
                        {'type': 'Fake', 'name': 'fake1'},
                    ],
                    'weight': 0.5,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 0.5:
                    return True
                return False

            answers = _build_atom_answer(
                snake, link_type='Similarity', target_type='Fake', filter=my_filter
            )
            assert answers == {"Similarity : ['snake', 'fake1']"}

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
        _similarity_human_monkey_links()

    def test_get_neighbors(self, das):
        def _build_neighbors(handle: str) -> dict:
            cursor = das.get_traversal_cursor(handle)
            neighbors_iterator = cursor.get_neighbors()
            return [item for item in neighbors_iterator]

        def _human_neighbors():
            neighbors = _build_neighbors(human)
            assert das.get_atom(monkey) in neighbors
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(chimp) in neighbors
            assert das.get_atom(ent) in neighbors
            assert len(neighbors) == 4

        def _monkey_neighbors():
            neighbors = _build_neighbors(monkey)
            assert das.get_atom(human) in neighbors
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(chimp) in neighbors
            assert len(neighbors) == 3

        def _chimp_neighbors():
            neighbors = _build_neighbors(chimp)
            assert das.get_atom(human) in neighbors
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(monkey) in neighbors
            assert len(neighbors) == 3

        def _mammal_neighbors():
            neighbors = _build_neighbors(mammal)
            assert das.get_atom(animal) in neighbors
            assert das.get_atom(monkey) in neighbors
            assert das.get_atom(chimp) in neighbors
            assert das.get_atom(human) in neighbors
            assert das.get_atom(rhino) in neighbors
            assert len(neighbors) == 5

        def _ent_neighbors():
            neighbors = _build_neighbors(ent)
            assert das.get_atom(human) in neighbors
            assert das.get_atom(plant) in neighbors
            assert len(neighbors) == 2

        def _animal_neighbors():
            neighbors = _build_neighbors(animal)
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(reptile) in neighbors
            assert das.get_atom(earthworm) in neighbors
            assert len(neighbors) == 3

        def _reptile_neighbors():
            neighbors = _build_neighbors(reptile)
            assert das.get_atom(animal) in neighbors
            assert das.get_atom(dinosaur) in neighbors
            assert das.get_atom(snake) in neighbors
            assert len(neighbors) == 3

        def _dinosaur_neighbors():
            neighbors = _build_neighbors(dinosaur)
            assert das.get_atom(reptile) in neighbors
            assert das.get_atom(triceratops) in neighbors
            assert len(neighbors) == 2

        def _triceratops_neighbors():
            neighbors = _build_neighbors(triceratops)
            assert das.get_atom(dinosaur) in neighbors
            assert das.get_atom(rhino) in neighbors
            assert len(neighbors) == 2

        def _rhino_neighbors():
            neighbors = _build_neighbors(rhino)
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(triceratops) in neighbors
            assert len(neighbors) == 2

        def _earthworm_neighbors():
            neighbors = _build_neighbors(earthworm)
            assert das.get_atom(animal) in neighbors
            assert das.get_atom(snake) in neighbors
            assert len(neighbors) == 2

        def _snake_neighbors():
            neighbors = _build_neighbors(snake)
            assert das.get_atom(earthworm) in neighbors
            assert das.get_atom(reptile) in neighbors
            assert das.get_atom(vine) in neighbors
            assert len(neighbors) == 3

        def _vine_neighbors():
            neighbors = _build_neighbors(vine)
            assert das.get_atom(snake) in neighbors
            assert das.get_atom(plant) in neighbors
            assert len(neighbors) == 2

        def _plant_neighbors():
            neighbors = _build_neighbors(plant)
            assert das.get_atom(vine) in neighbors
            assert das.get_atom(ent) in neighbors
            assert len(neighbors) == 2

        def _similarity_inheritance_neighbors():
            answers = _build_neighbors(similarity_human_monkey)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_human_chimp)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_chimp_monkey)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_snake_earthworm)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_rhino_triceratops)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_snake_vine)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_human_ent)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_human_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_monkey_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_chimp_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_mammal_animal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_reptile_animal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_snake_reptile)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_dinosaur_reptile)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_triceratops_dinosaur)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_earthworm_animal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_rhino_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_vine_plant)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_ent_plant)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_monkey_human)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_chimp_human)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_monkey_chimp)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_earthworm_snake)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_triceratops_rhino)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_vine_snake)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_ent_human)
            assert len(answers) == 0

        _human_neighbors()
        _monkey_neighbors()
        _chimp_neighbors()
        _mammal_neighbors()
        _ent_neighbors()
        _animal_neighbors()
        _reptile_neighbors()
        _dinosaur_neighbors()
        _triceratops_neighbors()
        _rhino_neighbors()
        _earthworm_neighbors()
        _snake_neighbors()
        _vine_neighbors()
        _plant_neighbors()
        _similarity_inheritance_neighbors()

    def test_get_neighbors_with_filters(self, das):
        def _build_neighbors(handle: str, **filters) -> dict:
            cursor = das.get_traversal_cursor(handle)
            neighbors_iterator = cursor.get_neighbors(**filters)
            return [item for item in neighbors_iterator]

        def _human_neighbors():
            neighbors = _build_neighbors(human, link_type='Inheritance')
            assert das.get_atom(mammal) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(human, link_type='Similarity')
            assert das.get_atom(monkey) in neighbors
            assert das.get_atom(chimp) in neighbors
            assert das.get_atom(ent) in neighbors
            assert len(neighbors) == 3

            neighbors = _build_neighbors(human, target_type='Concept')
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(monkey) in neighbors
            assert das.get_atom(chimp) in neighbors
            assert das.get_atom(ent) in neighbors
            assert len(neighbors) == 4

            neighbors = _build_neighbors(human, link_type='Inheritance', target_type='Snet')
            assert len(neighbors) == 0

            neighbors = _build_neighbors(human, link_type='Similarity', target_type='Snet')
            assert len(neighbors) == 0

            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'human'},
                        {'type': 'Fake', 'name': 'fake-h'},
                    ],
                    'weight': 0.7,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-h'},
                        {'type': 'Concept', 'name': 'human'},
                    ],
                    'weight': 0.3,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 1:
                    return True
                return False

            fake_h = AtomDB.node_handle('Fake', 'fake-h')

            neighbors = _build_neighbors(human)
            assert das.get_atom(mammal) in neighbors
            assert das.get_atom(monkey) in neighbors
            assert das.get_atom(chimp) in neighbors
            assert das.get_atom(ent) in neighbors
            assert das.get_atom(fake_h) in neighbors
            assert len(neighbors) == 5

            neighbors = _build_neighbors(human, link_type='Similarity', target_type='Fake')
            assert das.get_atom(fake_h) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(
                human, link_type='Similarity', target_type='Fake', filter=my_filter
            )
            assert len(neighbors) == 0

        def _vine_neighbors():
            neighbors = _build_neighbors(vine, link_type='Similarity')
            assert das.get_atom(snake) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(vine, link_type='Inheritance')
            assert das.get_atom(plant) in neighbors
            assert len(neighbors) == 1

            fake_v1 = AtomDB.node_handle('Fake', 'fake-v1')
            fake_v2 = AtomDB.node_handle('Fake', 'fake-v2')

            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-v1'},
                        {'type': 'Concept', 'name': 'vine'},
                    ],
                    'weight': 1,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'vine'},
                        {'type': 'Fake', 'name': 'fake-v2'},
                    ],
                    'weight': 0.7,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-v2'},
                        {'type': 'Concept', 'name': 'vine'},
                    ],
                    'weight': 0.3,
                }
            )

            neighbors = _build_neighbors(vine, link_type='Inheritance')
            assert das.get_atom(plant) in neighbors
            assert das.get_atom(fake_v1) in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(vine, link_type='Similarity')
            assert das.get_atom(snake) in neighbors
            assert das.get_atom(fake_v2) in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(vine, link_type='Similarity', target_type='Concept')
            assert das.get_atom(snake) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(vine, link_type='Inheritance', target_type='Fake')
            assert das.get_atom(fake_v1) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(vine, link_type='Similarity', target_type='Fake')
            assert das.get_atom(fake_v2) in neighbors
            assert len(neighbors) == 1

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 1:
                    return True
                return False

            neighbors = _build_neighbors(vine, link_type='Similarity', filter=my_filter)
            assert len(neighbors) == 0

            neighbors = _build_neighbors(vine, link_type='Inheritance', filter=my_filter)
            assert das.get_atom(fake_v1) in neighbors
            assert len(neighbors) == 1

        def _inheritance_dinosaur_reptile():
            neighbors = _build_neighbors(inheritance_dinosaur_reptile)
            assert len(neighbors) == 0

            fake_dr1 = AtomDB.node_handle('Fake', 'fake-dr1')
            fake_dr2 = AtomDB.node_handle('Fake', 'fake-dr2')

            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-dr1'},
                        {
                            'type': 'Inheritance',
                            'targets': [
                                {'type': 'Concept', 'name': 'dinosaur'},
                                {'type': 'Concept', 'name': 'reptile'},
                            ],
                        },
                    ],
                    'weight': 1,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {
                            'type': 'Inheritance',
                            'targets': [
                                {'type': 'Concept', 'name': 'dinosaur'},
                                {'type': 'Concept', 'name': 'reptile'},
                            ],
                        },
                        {'type': 'Fake', 'name': 'fake-dr2'},
                    ],
                    'weight': 0.7,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-dr2'},
                        {
                            'type': 'Inheritance',
                            'targets': [
                                {'type': 'Concept', 'name': 'dinosaur'},
                                {'type': 'Concept', 'name': 'reptile'},
                            ],
                        },
                    ],
                    'weight': 0.3,
                }
            )

            neighbors = _build_neighbors(inheritance_dinosaur_reptile)
            assert das.get_atom(fake_dr1) in neighbors
            assert das.get_atom(fake_dr2) in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, link_type='Similarity')
            assert das.get_atom(fake_dr2) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, link_type='Inheritance')
            assert das.get_atom(fake_dr1) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(
                inheritance_dinosaur_reptile, link_type='Inheritance', target_type='Fake'
            )
            assert das.get_atom(fake_dr1) in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(
                inheritance_dinosaur_reptile, link_type='Inheritance', target_type='Concept'
            )
            assert len(neighbors) == 0

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, target_type='Concept')
            assert len(neighbors) == 0

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, target_type='Fake')
            assert das.get_atom(fake_dr1) in neighbors
            assert das.get_atom(fake_dr2) in neighbors
            assert len(neighbors) == 2

        _human_neighbors()
        _vine_neighbors()
        _inheritance_dinosaur_reptile()

    @pytest.mark.parametrize("n", range(10))
    def test_follow_link(self, das, n):
        def _mammal():
            cursor = das.get_traversal_cursor(mammal)
            assert cursor.get()['name'] == 'mammal'

            cursor.follow_link()
            current_cursor = cursor.get()['name']
            assert current_cursor == 'monkey' or 'chimp' or 'human' or 'animal' or 'rhino'

            cursor.follow_link()
            previous_cursor = current_cursor
            current_cursor = cursor.get()['name']
            if previous_cursor == 'monkey':
                assert current_cursor == 'mammal' or 'chimp'
            elif previous_cursor == 'chimp':
                assert current_cursor == 'mammal' or 'monkey' or 'human'
            elif previous_cursor == 'human':
                assert current_cursor == 'mammal' or 'monkey' or 'chimp' or 'ent'
            elif previous_cursor == 'animal':
                assert current_cursor == 'mammal' or 'reptile' or 'earthworm'
            elif previous_cursor == 'rhino':
                assert current_cursor == 'mammal' or 'triceratops'

        def _earthworm():
            cursor = das.get_traversal_cursor(earthworm)
            assert cursor.get()['name'] == 'earthworm'

            cursor.follow_link()
            current_cursor = cursor.get()['name']
            assert current_cursor == 'animal' or 'snake'

            cursor.follow_link()
            previous_cursor = current_cursor
            current_cursor = cursor.get()['name']

            if previous_cursor == 'animal':
                assert current_cursor == 'mammal' or 'reptile' or 'earthworm'
            elif previous_cursor == 'snake':
                assert current_cursor == 'earthworm' or 'reptile' or 'vine'

        _mammal()
        _earthworm()

    def test_follow_link_with_filters(self, das):
        def _mammal():
            cursor = das.get_traversal_cursor(mammal)
            assert cursor.get()['name'] == 'mammal'

            cursor.follow_link(link_type='Similarity')
            assert cursor.get()['name'] == 'mammal'

            cursor.follow_link(link_type='Inheritance')
            current_cursor = cursor.get()['name']
            assert current_cursor == 'monkey' or 'chimp' or 'human' or 'animal' or 'rhino'

            cursor.follow_link(link_type='Inheritance', target_type='Concept')
            previous_cursor = current_cursor
            current_cursor = cursor.get()['name']
            if previous_cursor == 'monkey':
                assert current_cursor == 'mammal' or 'chimp'
            elif previous_cursor == 'chimp':
                assert current_cursor == 'mammal' or 'monkey' or 'human'
            elif previous_cursor == 'human':
                assert current_cursor == 'mammal' or 'monkey' or 'chimp' or 'ent'
            elif previous_cursor == 'animal':
                assert current_cursor == 'mammal' or 'reptile' or 'earthworm'
            elif previous_cursor == 'rhino':
                assert current_cursor == 'mammal' or 'triceratops'

            cursor.follow_link(link_type='Inheritance', target_type='Fake')
            previous_cursor = current_cursor
            current_cursor = cursor.get()['name']
            assert previous_cursor == current_cursor

            with pytest.raises(MultiplePathsError) as exc:
                cursor.follow_link(link_type='Inheritance', unique_path=True)
            assert exc.value.message == 'Unable to follow the link. More than one path found'

        _mammal()


class TestHandleTraverseEngine:
    @pytest.fixture
    def das(self):
        das = DistributedAtomSpace()
        up_knowledge_base_animals(das)
        return das

    def test_get(self, das: DistributedAtomSpace):
        human = das.get_node_handle('Concept', 'human')
        document_cursor = das.get_traversal_cursor(handle=human, handles_only=True)
        cursor = document_cursor.get()
        assert cursor == human

    def test_get_links(self, das):
        def _build_atom_answer(handle: str) -> dict:
            cursor = das.get_traversal_cursor(handle, handles_only=True)
            links = cursor.get_links()
            return set([link for link in links])

        def _human_links():
            answers = _build_atom_answer(human)
            assert len(answers) == 7
            assert answers == {
                similarity_human_monkey,
                similarity_monkey_human,
                similarity_chimp_human,
                similarity_human_chimp,
                similarity_human_ent,
                similarity_ent_human,
                inheritance_human_mammal,
            }

        def _monkey_links():
            answers = _build_atom_answer(monkey)
            assert len(answers) == 5
            assert answers == {
                inheritance_monkey_mammal,
                similarity_monkey_chimp,
                similarity_chimp_monkey,
                similarity_monkey_human,
                similarity_human_monkey,
            }

        def _chimp_links():
            answers = _build_atom_answer(chimp)
            assert len(answers) == 5
            assert answers == {
                inheritance_chimp_mammal,
                similarity_monkey_chimp,
                similarity_chimp_monkey,
                similarity_human_chimp,
                similarity_chimp_human,
            }

        def _mammal_links():
            answers = _build_atom_answer(mammal)
            assert len(answers) == 5
            assert answers == {
                inheritance_mammal_animal,
                inheritance_monkey_mammal,
                inheritance_chimp_mammal,
                inheritance_human_mammal,
                inheritance_rhino_mammal,
            }

        def _ent_links():
            answers = _build_atom_answer(ent)
            assert len(answers) == 3
            assert answers == {inheritance_ent_plant, similarity_human_ent, similarity_ent_human}

        def _animal_links():
            answers = _build_atom_answer(animal)
            assert len(answers) == 3
            assert answers == {
                inheritance_mammal_animal,
                inheritance_reptile_animal,
                inheritance_earthworm_animal,
            }

        def _reptile_links():
            answers = _build_atom_answer(reptile)
            assert len(answers) == 3
            assert answers == {
                inheritance_dinosaur_reptile,
                inheritance_snake_reptile,
                inheritance_reptile_animal,
            }

        def _dinosaur_links():
            answers = _build_atom_answer(dinosaur)
            assert len(answers) == 2
            assert answers == {inheritance_dinosaur_reptile, inheritance_triceratops_dinosaur}

        def _triceratops_links():
            answers = _build_atom_answer(triceratops)
            assert len(answers) == 3
            assert answers == {
                similarity_rhino_triceratops,
                similarity_triceratops_rhino,
                inheritance_triceratops_dinosaur,
            }

        def _rhino_links():
            answers = _build_atom_answer(rhino)
            assert len(answers) == 3
            assert answers == {
                similarity_rhino_triceratops,
                similarity_triceratops_rhino,
                inheritance_rhino_mammal,
            }

        def _earthworm_links():
            answers = _build_atom_answer(earthworm)
            assert len(answers) == 3
            assert answers == {
                similarity_snake_earthworm,
                similarity_earthworm_snake,
                inheritance_earthworm_animal,
            }

        def _snake_links():
            answers = _build_atom_answer(snake)
            assert len(answers) == 5
            assert answers == {
                similarity_snake_earthworm,
                similarity_earthworm_snake,
                similarity_snake_vine,
                similarity_vine_snake,
                inheritance_snake_reptile,
            }

        def _vine_links():
            answers = _build_atom_answer(vine)
            assert len(answers) == 3
            assert answers == {inheritance_vine_plant, similarity_snake_vine, similarity_vine_snake}

        def _plant_links():
            answers = _build_atom_answer(plant)
            assert len(answers) == 2
            assert answers == {inheritance_vine_plant, inheritance_ent_plant}

        def _similarity_inheritance_links():
            answers = _build_atom_answer(similarity_human_monkey)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_human_chimp)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_chimp_monkey)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_snake_earthworm)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_rhino_triceratops)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_snake_vine)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_human_ent)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_human_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_monkey_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_chimp_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_mammal_animal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_reptile_animal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_snake_reptile)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_dinosaur_reptile)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_triceratops_dinosaur)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_earthworm_animal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_rhino_mammal)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_vine_plant)
            assert len(answers) == 0

            answers = _build_atom_answer(inheritance_ent_plant)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_monkey_human)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_chimp_human)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_monkey_chimp)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_earthworm_snake)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_triceratops_rhino)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_vine_snake)
            assert len(answers) == 0

            answers = _build_atom_answer(similarity_ent_human)
            assert len(answers) == 0

        _human_links()
        _monkey_links()
        _chimp_links()
        _mammal_links()
        _ent_links()
        _animal_links()
        _reptile_links()
        _dinosaur_links()
        _triceratops_links()
        _rhino_links()
        _earthworm_links()
        _snake_links()
        _vine_links()
        _plant_links()
        _similarity_inheritance_links()

    def test_get_links_with_filters(self, das):
        def _build_atom_answer(handle: str, **filters) -> dict:
            cursor = das.get_traversal_cursor(handle, handles_only=True)
            links = cursor.get_links(**filters)
            return set([link for link in links])

        def _human_links():
            answers = _build_atom_answer(human, link_type='Similarity')
            assert answers == {
                similarity_human_monkey,
                similarity_monkey_human,
                similarity_chimp_human,
                similarity_human_chimp,
                similarity_human_ent,
                similarity_ent_human,
            }
            answers = _build_atom_answer(human, link_type='Similarity', cursor_position=0)
            assert answers == {
                similarity_human_chimp,
                similarity_human_monkey,
                similarity_human_ent,
            }
            answers = _build_atom_answer(human, link_type='Similarity', cursor_position=1)
            assert answers == {
                similarity_ent_human,
                similarity_monkey_human,
                similarity_chimp_human,
            }
            answers = _build_atom_answer(
                human, link_type='Similarity', cursor_position=0, target_type='Concept'
            )
            assert answers == {
                similarity_human_chimp,
                similarity_human_monkey,
                similarity_human_ent,
            }
            answers = _build_atom_answer(human, link_type='Fake')
            assert len(answers) == 0
            answers = _build_atom_answer(human, cursor_position=2)
            assert len(answers) == 0
            answers = _build_atom_answer(human, target_type='Fake')
            assert len(answers) == 0

            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'human'},
                        {'type': 'Concept', 'name': 'snet'},
                    ],
                    'weight': 0.5,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link:
                    return True
                return False

            answers = _build_atom_answer(
                human,
                link_type='Similarity',
                cursor_position=0,
                target_type='Concept',
                filter=my_filter,
            )

            snet = AtomDB.node_handle('Concept', 'snet')

            assert answers == {AtomDB.link_handle('Similarity', [human, snet])}

            def my_second_filter(link):
                if 'weight' in link and link['weight'] >= 0.5:
                    return link

            with pytest.raises(TypeError) as exc:
                _build_atom_answer(human, filter=my_second_filter)
            assert exc.value.args[0] == 'The function must return a boolean'

        def _mammal_links():
            answers = _build_atom_answer(mammal, link_type='Inheritance')
            assert answers == {
                inheritance_mammal_animal,
                inheritance_monkey_mammal,
                inheritance_chimp_mammal,
                inheritance_human_mammal,
                inheritance_rhino_mammal,
            }
            answers = _build_atom_answer(mammal, link_type='Inheritance', cursor_position=0)
            assert answers == {inheritance_mammal_animal}
            answers = _build_atom_answer(mammal, link_type='Inheritance', cursor_position=1)
            assert answers == {
                inheritance_monkey_mammal,
                inheritance_chimp_mammal,
                inheritance_human_mammal,
                inheritance_rhino_mammal,
            }
            answers = _build_atom_answer(
                mammal, link_type='Inheritance', cursor_position=1, target_type='Concept'
            )
            assert answers == {
                inheritance_monkey_mammal,
                inheritance_chimp_mammal,
                inheritance_human_mammal,
                inheritance_rhino_mammal,
            }
            answers = _build_atom_answer(mammal, link_type='Similarity')
            assert len(answers) == 0
            answers = _build_atom_answer(mammal, cursor_position=5)
            assert len(answers) == 0
            answers = _build_atom_answer(mammal, target_type='Snet')
            assert len(answers) == 0

            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake1'},
                        {'type': 'Concept', 'name': 'mammal'},
                    ],
                    'weight': 0.4,
                }
            )
            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake2'},
                        {'type': 'Concept', 'name': 'mammal'},
                    ],
                    'weight': 0.5,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 0.5:
                    return True
                return False

            answers = _build_atom_answer(
                mammal,
                link_type='Inheritance',
                cursor_position=1,
                target_type='Fake',
                filter=my_filter,
            )
            fake2 = AtomDB.node_handle('Fake', 'fake2')
            assert answers == {AtomDB.link_handle('Inheritance', [fake2, mammal])}

        def _snake_links():
            answers = _build_atom_answer(snake, link_type='Similarity')
            assert answers == {
                similarity_snake_earthworm,
                similarity_earthworm_snake,
                similarity_snake_vine,
                similarity_vine_snake,
            }
            answers = _build_atom_answer(snake, link_type='Inheritance', cursor_position=0)
            assert answers == {inheritance_snake_reptile}
            answers = _build_atom_answer(snake, link_type='Inheritance', cursor_position=1)
            assert len(answers) == 0
            answers = _build_atom_answer(snake, link_type='Similarity', cursor_position=0)
            assert answers == {
                similarity_snake_earthworm,
                similarity_snake_vine,
            }
            answers = _build_atom_answer(snake, link_type='Similarity', cursor_position=1)
            assert answers == {similarity_earthworm_snake, similarity_vine_snake}
            answers = _build_atom_answer(
                snake, link_type='Inheritance', cursor_position=0, target_type='Concept'
            )
            assert answers == {inheritance_snake_reptile}
            answers = _build_atom_answer(snake, link_type='Evaluation')
            assert len(answers) == 0
            answers = _build_atom_answer(snake, cursor_position=5)
            assert len(answers) == 0
            answers = _build_atom_answer(
                snake, link_type='Inheritance', cursor_position=0, target_type='Snet'
            )
            assert len(answers) == 0

            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake1'},
                        {'type': 'Concept', 'name': 'snake'},
                    ],
                    'weight': 0.2,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'snake'},
                        {'type': 'Fake', 'name': 'fake1'},
                    ],
                    'weight': 0.5,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 0.5:
                    return True
                return False

            answers = _build_atom_answer(
                snake, link_type='Similarity', target_type='Fake', filter=my_filter
            )
            fake1 = AtomDB.node_handle('Fake', 'fake1')
            assert answers == {AtomDB.link_handle('Similarity', [snake, fake1])}

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

            fake = AtomDB.node_handle('Fake', 'fake')
            nested_link_handle = AtomDB.link_handle('Inheritance', [fake, similarity_human_monkey])

            answers = _build_atom_answer(similarity_human_monkey)
            assert len(answers) == 1
            assert answers == {nested_link_handle}

        _human_links()
        _mammal_links()
        _snake_links()
        _similarity_human_monkey_links()

    def test_get_neighbors(self, das):
        def _build_neighbors(handle: str) -> dict:
            cursor = das.get_traversal_cursor(handle, handles_only=True)
            neighbors_iterator = cursor.get_neighbors()
            return [item for item in neighbors_iterator]

        def _human_neighbors():
            neighbors = _build_neighbors(human)
            assert monkey in neighbors
            assert mammal in neighbors
            assert chimp in neighbors
            assert ent in neighbors
            assert len(neighbors) == 4

        def _monkey_neighbors():
            neighbors = _build_neighbors(monkey)
            assert human in neighbors
            assert mammal in neighbors
            assert chimp in neighbors
            assert len(neighbors) == 3

        def _chimp_neighbors():
            neighbors = _build_neighbors(chimp)
            assert human in neighbors
            assert mammal in neighbors
            assert monkey in neighbors
            assert len(neighbors) == 3

        def _mammal_neighbors():
            neighbors = _build_neighbors(mammal)
            assert animal in neighbors
            assert monkey in neighbors
            assert chimp in neighbors
            assert human in neighbors
            assert rhino in neighbors
            assert len(neighbors) == 5

        def _ent_neighbors():
            neighbors = _build_neighbors(ent)
            assert human in neighbors
            assert plant in neighbors
            assert len(neighbors) == 2

        def _animal_neighbors():
            neighbors = _build_neighbors(animal)
            assert mammal in neighbors
            assert reptile in neighbors
            assert earthworm in neighbors
            assert len(neighbors) == 3

        def _reptile_neighbors():
            neighbors = _build_neighbors(reptile)
            assert animal in neighbors
            assert dinosaur in neighbors
            assert snake in neighbors
            assert len(neighbors) == 3

        def _dinosaur_neighbors():
            neighbors = _build_neighbors(dinosaur)
            assert reptile in neighbors
            assert triceratops in neighbors
            assert len(neighbors) == 2

        def _triceratops_neighbors():
            neighbors = _build_neighbors(triceratops)
            assert dinosaur in neighbors
            assert rhino in neighbors
            assert len(neighbors) == 2

        def _rhino_neighbors():
            neighbors = _build_neighbors(rhino)
            assert mammal in neighbors
            assert triceratops in neighbors
            assert len(neighbors) == 2

        def _earthworm_neighbors():
            neighbors = _build_neighbors(earthworm)
            assert animal in neighbors
            assert snake in neighbors
            assert len(neighbors) == 2

        def _snake_neighbors():
            neighbors = _build_neighbors(snake)
            assert earthworm in neighbors
            assert reptile in neighbors
            assert vine in neighbors
            assert len(neighbors) == 3

        def _vine_neighbors():
            neighbors = _build_neighbors(vine)
            assert snake in neighbors
            assert plant in neighbors
            assert len(neighbors) == 2

        def _plant_neighbors():
            neighbors = _build_neighbors(plant)
            assert vine in neighbors
            assert ent in neighbors
            assert len(neighbors) == 2

        def _similarity_inheritance_neighbors():
            answers = _build_neighbors(similarity_human_monkey)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_human_chimp)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_chimp_monkey)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_snake_earthworm)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_rhino_triceratops)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_snake_vine)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_human_ent)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_human_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_monkey_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_chimp_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_mammal_animal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_reptile_animal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_snake_reptile)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_dinosaur_reptile)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_triceratops_dinosaur)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_earthworm_animal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_rhino_mammal)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_vine_plant)
            assert len(answers) == 0

            answers = _build_neighbors(inheritance_ent_plant)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_monkey_human)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_chimp_human)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_monkey_chimp)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_earthworm_snake)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_triceratops_rhino)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_vine_snake)
            assert len(answers) == 0

            answers = _build_neighbors(similarity_ent_human)
            assert len(answers) == 0

        _human_neighbors()
        _monkey_neighbors()
        _chimp_neighbors()
        _mammal_neighbors()
        _ent_neighbors()
        _animal_neighbors()
        _reptile_neighbors()
        _dinosaur_neighbors()
        _triceratops_neighbors()
        _rhino_neighbors()
        _earthworm_neighbors()
        _snake_neighbors()
        _vine_neighbors()
        _plant_neighbors()
        _similarity_inheritance_neighbors()

    def test_get_neighbors_with_filters(self, das):
        def _build_neighbors(handle: str, **filters) -> dict:
            cursor = das.get_traversal_cursor(handle, handles_only=True)
            neighbors_iterator = cursor.get_neighbors(**filters)
            return [item for item in neighbors_iterator]

        def _human_neighbors():
            neighbors = _build_neighbors(human, link_type='Inheritance')
            assert mammal in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(human, link_type='Similarity')
            assert monkey in neighbors
            assert chimp in neighbors
            assert ent in neighbors
            assert len(neighbors) == 3

            neighbors = _build_neighbors(human, target_type='Concept')
            assert mammal in neighbors
            assert monkey in neighbors
            assert chimp in neighbors
            assert ent in neighbors
            assert len(neighbors) == 4

            neighbors = _build_neighbors(human, link_type='Inheritance', target_type='Snet')
            assert len(neighbors) == 0

            neighbors = _build_neighbors(human, link_type='Similarity', target_type='Snet')
            assert len(neighbors) == 0

            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'human'},
                        {'type': 'Fake', 'name': 'fake-h'},
                    ],
                    'weight': 0.7,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-h'},
                        {'type': 'Concept', 'name': 'human'},
                    ],
                    'weight': 0.3,
                }
            )

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 1:
                    return True
                return False

            fake_h = AtomDB.node_handle('Fake', 'fake-h')

            neighbors = _build_neighbors(human)
            assert mammal in neighbors
            assert monkey in neighbors
            assert chimp in neighbors
            assert ent in neighbors
            assert fake_h in neighbors
            assert len(neighbors) == 5

            neighbors = _build_neighbors(human, link_type='Similarity', target_type='Fake')
            assert fake_h in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(
                human, link_type='Similarity', target_type='Fake', filter=my_filter
            )
            assert fake_h in neighbors
            assert len(neighbors) == 1

        def _vine_neighbors():
            neighbors = _build_neighbors(vine, link_type='Similarity')
            assert snake in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(vine, link_type='Inheritance')
            assert plant in neighbors
            assert len(neighbors) == 1

            fake_v1 = AtomDB.node_handle('Fake', 'fake-v1')
            fake_v2 = AtomDB.node_handle('Fake', 'fake-v2')

            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-v1'},
                        {'type': 'Concept', 'name': 'vine'},
                    ],
                    'weight': 1,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Concept', 'name': 'vine'},
                        {'type': 'Fake', 'name': 'fake-v2'},
                    ],
                    'weight': 0.7,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-v2'},
                        {'type': 'Concept', 'name': 'vine'},
                    ],
                    'weight': 0.3,
                }
            )

            neighbors = _build_neighbors(vine, link_type='Inheritance')
            assert plant in neighbors
            assert fake_v1 in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(vine, link_type='Similarity')
            assert snake in neighbors
            assert fake_v2 in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(vine, link_type='Similarity', target_type='Concept')
            assert snake in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(vine, link_type='Inheritance', target_type='Fake')
            assert fake_v1 in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(vine, link_type='Similarity', target_type='Fake')
            assert fake_v2 in neighbors
            assert len(neighbors) == 1

            def my_filter(link) -> bool:
                if 'weight' in link and link['weight'] >= 1:
                    return True
                return False

            neighbors = _build_neighbors(vine, link_type='Similarity', filter=my_filter)
            assert snake in neighbors
            assert fake_v2 in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(vine, link_type='Inheritance', filter=my_filter)
            assert plant in neighbors
            assert fake_v1 in neighbors
            assert len(neighbors) == 2

        def _inheritance_dinosaur_reptile():
            neighbors = _build_neighbors(inheritance_dinosaur_reptile)
            assert len(neighbors) == 0

            fake_dr1 = AtomDB.node_handle('Fake', 'fake-dr1')
            fake_dr2 = AtomDB.node_handle('Fake', 'fake-dr2')

            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-dr1'},
                        {
                            'type': 'Inheritance',
                            'targets': [
                                {'type': 'Concept', 'name': 'dinosaur'},
                                {'type': 'Concept', 'name': 'reptile'},
                            ],
                        },
                    ],
                    'weight': 1,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {
                            'type': 'Inheritance',
                            'targets': [
                                {'type': 'Concept', 'name': 'dinosaur'},
                                {'type': 'Concept', 'name': 'reptile'},
                            ],
                        },
                        {'type': 'Fake', 'name': 'fake-dr2'},
                    ],
                    'weight': 0.7,
                }
            )
            das.add_link(
                {
                    'type': 'Similarity',
                    'targets': [
                        {'type': 'Fake', 'name': 'fake-dr2'},
                        {
                            'type': 'Inheritance',
                            'targets': [
                                {'type': 'Concept', 'name': 'dinosaur'},
                                {'type': 'Concept', 'name': 'reptile'},
                            ],
                        },
                    ],
                    'weight': 0.3,
                }
            )

            neighbors = _build_neighbors(inheritance_dinosaur_reptile)
            assert fake_dr1 in neighbors
            assert fake_dr2 in neighbors
            assert len(neighbors) == 2

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, link_type='Similarity')
            assert fake_dr2 in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, link_type='Inheritance')
            assert fake_dr1 in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(
                inheritance_dinosaur_reptile, link_type='Inheritance', target_type='Fake'
            )
            assert fake_dr1 in neighbors
            assert len(neighbors) == 1

            neighbors = _build_neighbors(
                inheritance_dinosaur_reptile, link_type='Inheritance', target_type='Concept'
            )
            assert len(neighbors) == 0

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, target_type='Concept')
            assert len(neighbors) == 0

            neighbors = _build_neighbors(inheritance_dinosaur_reptile, target_type='Fake')
            assert fake_dr1 in neighbors
            assert fake_dr2 in neighbors
            assert len(neighbors) == 2

        _human_neighbors()
        _vine_neighbors()
        _inheritance_dinosaur_reptile()

    @pytest.mark.parametrize("n", range(10))
    def test_follow_link(self, das, n):
        def _mammal():
            cursor = das.get_traversal_cursor(mammal, handles_only=True)
            assert cursor.get() == mammal

            cursor.follow_link()
            current_cursor = cursor.get()
            assert current_cursor == monkey or chimp or human or animal or rhino

            cursor.follow_link()
            previous_cursor = current_cursor
            current_cursor = cursor.get()
            if previous_cursor == monkey:
                assert current_cursor == mammal or chimp
            elif previous_cursor == chimp:
                assert current_cursor == mammal or monkey or human
            elif previous_cursor == human:
                assert current_cursor == mammal or monkey or chimp or ent
            elif previous_cursor == animal:
                assert current_cursor == mammal or reptile or earthworm
            elif previous_cursor == rhino:
                assert current_cursor == mammal or triceratops

        def _earthworm():
            cursor = das.get_traversal_cursor(earthworm, handles_only=True)
            assert cursor.get() == earthworm

            cursor.follow_link()
            current_cursor = cursor.get()
            assert current_cursor == animal or snake

            cursor.follow_link()
            previous_cursor = current_cursor
            current_cursor = cursor.get()

            if previous_cursor == animal:
                assert current_cursor == mammal or reptile or earthworm
            elif previous_cursor == snake:
                assert current_cursor == earthworm or reptile or vine

        _mammal()
        _earthworm()

    def test_follow_link_with_filters(self, das):
        def _mammal():
            cursor = das.get_traversal_cursor(mammal, handles_only=True)
            assert cursor.get() == mammal

            cursor.follow_link(link_type='Similarity')
            assert cursor.get() == mammal

            cursor.follow_link(link_type='Inheritance')
            current_cursor = cursor.get()
            assert current_cursor == monkey or chimp or human or animal or rhino

            cursor.follow_link(link_type='Inheritance', target_type='Concept')
            previous_cursor = current_cursor
            current_cursor = cursor.get()
            if previous_cursor == monkey:
                assert current_cursor == mammal or chimp
            elif previous_cursor == chimp:
                assert current_cursor == mammal or monkey or human
            elif previous_cursor == human:
                assert current_cursor == mammal or monkey or chimp or ent
            elif previous_cursor == animal:
                assert current_cursor == mammal or reptile or earthworm
            elif previous_cursor == rhino:
                assert current_cursor == mammal or triceratops

            cursor.follow_link(link_type='Inheritance', target_type='Fake')
            previous_cursor = current_cursor
            current_cursor = cursor.get()
            assert previous_cursor == current_cursor

            with pytest.raises(MultiplePathsError) as exc:
                cursor.follow_link(link_type='Inheritance', unique_path=True)
            assert exc.value.message == 'Unable to follow the link. More than one path found'

        _mammal()
