import json

import pytest

from hyperon_das.api import DistributedAtomSpace
from hyperon_das.exceptions import QueryParametersException
from hyperon_das.pattern_matcher.pattern_matcher import And, Link, Node, OrderedAssignment, PatternMatchingAnswer, UnorderedAssignment, Variable
from hyperon_das.utils import QueryOutputFormat
from hyperon_das_atomdb.adapters import InMemoryDB


class TestPatternMatchingAnswer:

    @pytest.fixture()
    def all_nodes(self):
        return [
            {'type': 'Concept', 'name': 'human'},
            {'type': 'Concept', 'name': 'monkey'},
            {'type': 'Concept', 'name': 'chimp'},
            {'type': 'Concept', 'name': 'snake'},
            {'type': 'Concept', 'name': 'earthworm'},
            {'type': 'Concept', 'name': 'rhino'},
            {'type': 'Concept', 'name': 'triceratops'},
            {'type': 'Concept', 'name': 'vine'},
            {'type': 'Concept', 'name': 'ent'},
            {'type': 'Concept', 'name': 'mammal'},
            {'type': 'Concept', 'name': 'animal'},
            {'type': 'Concept', 'name': 'reptile'},
            {'type': 'Concept', 'name': 'dinosaur'},
            {'type': 'Concept', 'name': 'plant'},
        ]

    @pytest.fixture()
    def all_links(self):
        return [
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

    @pytest.fixture()
    def database(self, all_nodes, all_links):
        db = InMemoryDB()
        for node in all_nodes:
            db.add_node(node)
        for link in all_links:
            db.add_link(link)
        return db
 
    def patterns(self, database):
        
        animal = Node('Concept', 'animal')
        mammal = Node('Concept', 'mammal')
        human = Node('Concept', 'human')
        chimp = Node('Concept', 'chimp')
        monkey = Node('Concept', 'monkey')
        ent = Node('Concept', 'ent')

        def get_items(assignment, key=-1):
            if isinstance(assignment, OrderedAssignment):
                return assignment.mapping.items()
            elif isinstance(assignment, UnorderedAssignment):
                symbols = []
                for key in assignment.symbols:
                    for i in range(assignment.symbols[key]):
                        symbols.append(key)
                values = []
                for key in assignment.values:
                    for i in range(assignment.values[key]):
                        values.append(key)
                mapping = []
                for symbol, value in zip(symbols, values):
                    mapping.append(tuple([symbol, value]))
                return mapping
            else:
                if key == -1:
                    return assignment.ordered_mapping.mapping.items()
                else:
                    return get_items(assignment.unordered_mappings[key])

        def check_pattern(db, pattern, expected_match, assignments, key=-1):
            answer = PatternMatchingAnswer()
            assert expected_match == pattern.matched(db, answer)
            if expected_match:
                assert len(answer.assignments) == len(assignments)
                ret1 = sorted([sorted([f'{x}={y}' for x, y in get_items(a, key)]) for a in answer.assignments])
                ret2 = sorted([sorted([f'{x}={y}' for x, y in d.items()]) for d in assignments])
                
                print('===================RET1=======================')
                print(f'\n{ret1}\n')
                print('==========================================')
                print('===================RET2=======================')
                print(f'\n{ret2}\n')
                print('==========================================')
                
                # assert ret1 == ret2
        
        check_pattern(
            db=database,
            pattern=Link('Inheritance', [Variable('V1'), mammal], True),
            expected_match=True,
            assignments=[{'V1': '<Concept: chimp>'}, {'V1': '<Concept: monkey>'}, {'V1': '<Concept: rhino>'}, {'V1': '<Concept: human>'}]
        )
        
    def test_basic_matching(self, database):

        db = database
        answer = PatternMatchingAnswer()

        # Nodes
        assert Node("Concept", "mammal").matched(db, answer)
        assert not Node("Concept", "blah").matched(db, answer)
        assert not Node("blah", "mammal").matched(db, answer)
        assert not Node("blah", "blah").matched(db, answer)

        # Asymmetric links
        assert Link("Inheritance", [Node("Concept", "human"), Node("Concept", "mammal")], True).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "human"), Node("Concept", "mammal")], False).matched(db, answer)
        assert not Link("blah", [Node("Concept", "human"), Node("Concept", "mammal")], True).matched(db, answer)
        assert not Link("Inheritance", [Node("Concept", "mammal"), Node("Concept", "human")], True).matched(db, answer)

        # Symmetric links
        assert Link("Similarity", [Node("Concept", "snake"), Node("Concept", "earthworm")], False).matched(db, answer)
        assert Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "snake")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "earthworm"), Node("Concept", "vine")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "vine"), Node("Concept", "earthworm")], False).matched(db, answer)
        assert Link("Similarity", [Node("Concept", "snake"), Node("Concept", "vine")], False).matched(db, answer)
        assert Link("Similarity", [Node("Concept", "vine"), Node("Concept", "snake")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "vine")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "snake")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "blah"), Node("Concept", "snake"), Node("Concept", "vine")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "vine"), Node("Concept", "snake"), Node("Concept", "blah")], False).matched(db, answer)
        assert not Link("Similarity", [Node("Concept", "snake"), Node("blah", "earthworm")], False).matched(db, answer)
        assert not Link("Similarity", [Node("blah", "snake"), Node("Concept", "earthworm")], False).matched(db, answer)
        assert not Link("Similarity", [Node("blah", "earthworm"), Node("Concept", "snake")], False).matched(db, answer)

        # Nested links
        # assert Link('List', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], True).matched(db, answer)
        assert not Link('List', [Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True), Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True)], True).matched(db, answer)
        # assert Link('Set', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], False).matched(db, answer)
        # assert Link('Set', [Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True), Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True)], False).matched(db, answer)
        assert not Link('List', [Link('Inheritance', [Node('Concept', 'blah'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], True).matched(db, answer)
        assert not Link('List', [Link('blah', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], True).matched(db, answer)
        assert not Link('Set', [Link('blah', [Node('Concept', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], True)], False).matched(db, answer)
        assert not Link('Set', [Link('Inheritance', [Node('blah', 'dinosaur'), Node('Concept', 'reptile')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], False)], False).matched(db, answer)
        assert not Link('Set', [Link('Inheritance', [Node('Concept', 'dinosaur'), Node('Concept', 'blah')], True), Link('Inheritance', [Node('Concept', 'triceratops'), Node('Concept', 'dinosaur')], False)], False).matched(db, answer)

        # # Variables
        animal = Node('Concept', 'animal')
        mammal = Node('Concept', 'mammal')
        human = Node('Concept', 'human')
        chimp = Node('Concept', 'chimp')
        monkey = Node('Concept', 'monkey')
        ent = Node('Concept', 'ent')
        assert Link('Inheritance', [human, mammal], True).matched(db, answer)
        assert Link('Inheritance', [monkey, mammal], True).matched(db, answer)
        assert Link('Inheritance', [chimp, mammal], True).matched(db, answer)
        assert Link('Similarity', [human, monkey], False).matched(db, answer)
        assert Link('Similarity', [chimp, monkey], False).matched(db, answer)
        assert Link('Inheritance', [Variable('V1'), mammal], True).matched(db, answer)
        assert Link('Inheritance', [Variable('V1'), Variable('V2')], True).matched(db, answer)
        assert not Link('Inheritance', [Variable('V1'), Variable('V1')], True).matched(db, answer)
        assert Link('Inheritance', [Variable('V2'), Variable('V1')], True).matched(db, answer)
        assert Link('Inheritance', [mammal, Variable('V1')], True).matched(db, answer)
        assert not Link('Inheritance', [animal, Variable('V1')], True).matched(db, answer)
        assert Link('Similarity', [Variable('V1'), Variable('V2')], False).matched(db, answer)
        # assert Link('Similarity', [human, Variable('V1')], False).matched(db, answer)
        # assert Link('Similarity', [Variable('V1'), human], False).matched(db, answer)
        # assert Link('List', [human, ent, Variable('V1'), Variable('V2')], True).matched(db, answer)
        assert not Link('List', [human, Variable('V1'), Variable('V2'), ent], True).matched(db, answer)
        assert not Link('List', [ent, Variable('V1'), Variable('V2'), human], True).matched(db, answer)
        # assert Link('Set', [human, ent, Variable('V1'), Variable('V2')], False).matched(db, answer)
        # assert Link('Set', [human, Variable('V1'), Variable('V2'), ent], False).matched(db, answer)
        # assert Link('Set', [ent, Variable('V1'), Variable('V2'), human], False).matched(db, answer)
        # assert Link('Set', [monkey, Variable('V1'), Variable('V2'), chimp], False).matched(db, answer)