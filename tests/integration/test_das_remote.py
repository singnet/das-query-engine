from unittest import mock

import pytest
from hyperon_das_atomdb import AtomDB

from hyperon_das import DistributedAtomSpace

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


class TestDasRemote:
    @pytest.fixture
    def das(self):
        url = 'https://de93-128-201-159-151.ngrok-free.app/v1/function'
        with mock.patch(
            'hyperon_das.query_engines.RemoteQueryEngine._connect_server', return_value=url
        ):
            return DistributedAtomSpace(query_engine='remote', host='test')

    def test_get_links(self, das: DistributedAtomSpace):
        def _build_atom_answer(handle: str) -> dict:
            cursor = das.get_traversal_cursor(handle)
            links = cursor.get_links()
            return simplify_links(links, das)

        def _human_links():
            das.add_link(
                {
                    'type': 'Inheritance',
                    'targets': [
                        {'type': 'Concept', 'name': 'human'},
                        {'type': 'Concept', 'name': 'capozzoli'},
                    ],
                }
            )
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
        # _monkey_links()
        # _chimp_links()
        # _mammal_links()
        # _ent_links()
        # _animal_links()
        # _reptile_links()
        # _dinosaur_links()
        # _triceratops_links()
        # _rhino_links()
        # _earthworm_links()
        # _snake_links()
        # _vine_links()
        # _plant_links()
        # _similarity_inheritance_links()
