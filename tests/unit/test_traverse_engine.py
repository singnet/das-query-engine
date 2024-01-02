import pytest

from hyperon_das.das import DistributedAtomSpace
from hyperon_das.exceptions import GetTraversalCursorException, MultiplePathsError
from hyperon_das.traverse_engines import DocumentTraverseEngine, HandleOnlyTraverseEngine


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


class TestTraverseEngine:
    @pytest.fixture
    def das(self):
        das = DistributedAtomSpace()

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

        return das

    def test_get_traversal_cursor(self, das):
        human = das.get_node_handle('Concept', 'human')

        document_cursor = das.get_traversal_cursor(human)

        assert isinstance(document_cursor, DocumentTraverseEngine)

        handle_only_cursor = das.get_traversal_cursor(human, handles_only=True)

        assert isinstance(handle_only_cursor, HandleOnlyTraverseEngine)

        with pytest.raises(GetTraversalCursorException) as exc:
            das.get_traversal_cursor(handle='snet')

        assert exc.value.message == 'Cannot start Traversal. Atom does not exist'

    def test_get(self, das):
        human = das.get_node_handle('Concept', 'human')
        document_cursor = das.get_traversal_cursor(human)
        cursor = document_cursor.get()

        assert cursor['handle'] == human
        assert cursor['name'] == 'human'
        assert cursor['named_type'] == 'Concept'

    def test_get_links(self, das):
        human = das.get_node_handle('Concept', 'human')
        monkey = das.get_node_handle('Concept', 'monkey')
        dinosaur = das.get_node_handle('Concept', 'dinosaur')
        plant = das.get_node_handle('Concept', 'plant')
        similarity_human_monkey = das.get_link_handle('Similarity', [human, monkey])

        document_cursor = das.get_traversal_cursor(human)
        links = document_cursor.get_links()
        answers = simplify_links(links, das)
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

        document_cursor = das.get_traversal_cursor(dinosaur)
        links = document_cursor.get_links()
        answers = simplify_links(links, das)
        assert len(answers) == 2
        assert answers == {
            "Inheritance : ['dinosaur', 'reptile']",
            "Inheritance : ['triceratops', 'dinosaur']",
        }

        document_cursor = das.get_traversal_cursor(plant)
        links = document_cursor.get_links()
        answers = simplify_links(links, das)
        assert len(answers) == 2
        assert answers == {
            "Inheritance : ['vine', 'plant']",
            "Inheritance : ['ent', 'plant']",
        }

        document_cursor = das.get_traversal_cursor(similarity_human_monkey)
        links = document_cursor.get_links()
        answers = simplify_links(links, das)
        assert len(answers) == 0
        assert answers == set()

    def test_get_links_with_filters(self, das):
        human = das.get_node_handle('Concept', 'human')

        cursor = das.get_traversal_cursor(human)
        links = cursor.get_links(link_type='Similarity')
        answers = simplify_links(links, das)
        assert answers == {
            "Similarity : ['human', 'chimp']",
            "Similarity : ['human', 'monkey']",
            "Similarity : ['human', 'ent']",
            "Similarity : ['ent', 'human']",
            "Similarity : ['monkey', 'human']",
            "Similarity : ['chimp', 'human']",
        }
        links = cursor.get_links(link_type='Similarity', cursor_position=0)
        answers = simplify_links(links, das)
        assert answers == {
            "Similarity : ['human', 'chimp']",
            "Similarity : ['human', 'monkey']",
            "Similarity : ['human', 'ent']",
        }
        links = cursor.get_links(link_type='Similarity', cursor_position=0, target_type='Concept')
        answers = simplify_links(links, das)
        assert answers == {
            "Similarity : ['human', 'chimp']",
            "Similarity : ['human', 'monkey']",
            "Similarity : ['human', 'ent']",
        }
        links = cursor.get_links(link_type='Fake')
        answers = simplify_links(links, das)
        assert answers == set()
        links = cursor.get_links(cursor_position=2)
        answers = simplify_links(links, das)
        assert answers == set()
        links = cursor.get_links(target_type='Fake')
        answers = simplify_links(links, das)
        assert answers == set()

        das.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'human'},
                    {'type': 'Concept', 'name': 'X'},
                ],
                'weight': 0.5,
            }
        )

        def my_filter(link) -> bool:
            if 'weight' in link:
                return True
            return False

        links = cursor.get_links(link_type='Inheritance', filter=my_filter)
        answers = simplify_links(links, das)
        assert answers == {"Inheritance : ['human', 'X']"}

    def test_get_neighbors(self, das):
        mammal = das.get_node_handle('Concept', 'mammal')
        monkey = das.get_node_handle('Concept', 'monkey')
        chimp = das.get_node_handle('Concept', 'chimp')
        human = das.get_node_handle('Concept', 'human')
        animal = das.get_node_handle('Concept', 'animal')
        rhino = das.get_node_handle('Concept', 'rhino')

        cursor = das.get_traversal_cursor(mammal)
        neighbors = cursor.get_neighbors()

        assert das.get_atom(monkey) in neighbors
        assert das.get_atom(chimp) in neighbors
        assert das.get_atom(human) in neighbors
        assert das.get_atom(animal) in neighbors
        assert das.get_atom(rhino) in neighbors

    def test_get_neighbors_with_filters(self, das):
        mammal = das.get_node_handle('Concept', 'mammal')
        monkey = das.get_node_handle('Concept', 'monkey')
        chimp = das.get_node_handle('Concept', 'chimp')
        human = das.get_node_handle('Concept', 'human')
        animal = das.get_node_handle('Concept', 'animal')
        rhino = das.get_node_handle('Concept', 'rhino')
        triceratops = das.get_node_handle('Concept', 'triceratops')

        cursor = das.get_traversal_cursor(mammal)
        neighbors = cursor.get_neighbors(link_type='Inheritance', target_type='Concept')

        assert das.get_atom(monkey) in neighbors
        assert das.get_atom(chimp) in neighbors
        assert das.get_atom(human) in neighbors
        assert das.get_atom(animal) in neighbors
        assert das.get_atom(rhino) in neighbors
        neighbors = cursor.get_neighbors(link_type='Fake')
        assert neighbors == []

        cursor = das.get_traversal_cursor(triceratops)
        neighbors = cursor.get_neighbors(link_type='Similarity')
        assert das.get_atom(rhino) in neighbors

        das.add_link(
            {
                'type': 'Similarity',
                'targets': [
                    {'type': 'Concept', 'name': 'triceratops'},
                    {'type': 'Member', 'name': 'X'},
                ],
                'weight': 0.5,
            }
        )

        def my_filter(link) -> bool:
            if 'weight' in link:
                return True
            return False

        neighbors = cursor.get_neighbors(
            link_type='Similarity', filter=my_filter, target_type='Concept'
        )
        x_handle = das.get_node_handle('Member', 'X')
        assert das.get_atom(x_handle) in neighbors

    @pytest.mark.parametrize("run", range(10))
    def test_follow_link(self, das, run):
        mammal = das.get_node_handle('Concept', 'mammal')

        cursor = das.get_traversal_cursor(mammal)

        assert cursor.get()['name'] == 'mammal'

        cursor.follow_link()

        current_cursor = cursor.get()['name']

        assert current_cursor == 'monkey' or 'chimp' or 'human' or 'animal' or 'rhino'

        cursor.follow_link()

        if current_cursor == 'monkey':
            assert cursor.get()['name'] == 'mammal' or 'chimp'
        if current_cursor == 'chimp':
            assert cursor.get()['name'] == 'mammal' or 'monkey' or 'human'
        if current_cursor == 'human':
            assert cursor.get()['name'] == 'mammal' or 'monkey' or 'chimp' or 'ent'
        if current_cursor == 'animal':
            assert cursor.get()['name'] == 'mammal' or 'reptile' or 'earthworm'
        if current_cursor == 'rhino':
            assert cursor.get()['name'] == 'mammal' or 'triceratops'

    def test_follow_link_with_filters(self, das):
        chimp = das.get_node_handle('Concept', 'chimp')

        cursor = das.get_traversal_cursor(chimp)

        assert cursor.get()['name'] == 'chimp'

        with pytest.raises(MultiplePathsError) as exc:
            cursor.follow_link(link_type='Similarity', unique_path=True)
        assert exc.value.message == 'Unable to follow the link. More than one path found'
