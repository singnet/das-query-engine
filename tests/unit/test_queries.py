from hyperon_das.api import DistributedAtomSpace
from hyperon_das.utils import QueryOutputFormat

def _name(link, index, typed = False):
    named_type = f"{link['targets'][index]['type']}:" if typed else ''
    return f"{named_type}{link['targets'][index]['name']}"

def _print_query_answer(query_answer, typed = False):
    if query_answer:
        for link in query_answer:
            if len(link['targets']) == 2:
                print(f"{link['type']}: {_name(link, 0)} -> {_name(link, 1)}")
            elif len(link['targets']) == 3:
                print(f"{link['type']}: {_name(link, 0)}({_name(link, 1, typed)}) -> {_name(link, 2, typed)}")
            else:
                assert False
def _check_query_answer(query_answer, expected_answer):
    assert len(query_answer) == len(expected_answer)
    for answer in query_answer:
        handles = [link["handle"] for link in answer]
        for expected in expected_answer:
            if len(expected) != len(handles):
                continue
            if sorted(expected) == sorted(handles):
                break
        else:
            assert False

class TestQueries:
    def setup_animals_kb(self, das: DistributedAtomSpace):
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "monkey"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "chimp"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "chimp"}, {"type": "Concept", "name": "monkey"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "snake"}, {"type": "Concept", "name": "earthworm"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "rhino"}, {"type": "Concept", "name": "triceratops"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "snake"}, {"type": "Concept", "name": "vine"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "ent"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "monkey"}, {"type": "Concept", "name": "human"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "chimp"}, {"type": "Concept", "name": "human"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "monkey"}, {"type": "Concept", "name": "chimp"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "earthworm"}, {"type": "Concept", "name": "snake"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "triceratops"}, {"type": "Concept", "name": "rhino"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "vine"}, {"type": "Concept", "name": "snake"},]}) 
        das.add_link({ "type": "Similarity", "targets": [ {"type": "Concept", "name": "ent"}, {"type": "Concept", "name": "human"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "human"}, {"type": "Concept", "name": "mammal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "monkey"}, {"type": "Concept", "name": "mammal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "chimp"}, {"type": "Concept", "name": "mammal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "mammal"}, {"type": "Concept", "name": "animal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "reptile"}, {"type": "Concept", "name": "animal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "snake"}, {"type": "Concept", "name": "reptile"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "dinosaur"}, {"type": "Concept", "name": "reptile"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "triceratops"}, {"type": "Concept", "name": "dinosaur"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "earthworm"}, {"type": "Concept", "name": "animal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "rhino"}, {"type": "Concept", "name": "mammal"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "vine"}, {"type": "Concept", "name": "plant"},]}) 
        das.add_link({ "type": "Inheritance", "targets": [ {"type": "Concept", "name": "ent"}, {"type": "Concept", "name": "plant"},]})

    def test_nested_pattern(self):
        das = DistributedAtomSpace()
        das.add_link({
            "type": "Expression",
            "targets": [
                {"type": "Symbol", "name": "Test"},
                {
                    "type": "Expression",
                    "targets": [
                        {"type": "Symbol", "name": "Test"},
                        {"type": "Symbol", "name": "2"}
                    ]
                }
            ]
        })
        query_params = {
            "toplevel_only": False,
            "return_type": QueryOutputFormat.ATOM_INFO,
        }
        q1 = {
            "atom_type": "link",
            "type": "Expression",
            "targets": [
                {"atom_type": "variable", "name": "v1"},
                {
                    "atom_type": "link",
                    "type": "Expression",
                    "targets": [
                        {"atom_type": "variable", "name": "v2"},
                        {"atom_type": "node", "type": "Symbol", "name": "2"},
                    ]
                }
            ]
        }
        answer = das.query(q1, query_params)
        assert len(answer) == 1
        assert answer[0]["handle"] == "dbcf1c7b610a5adea335bf08f6509978"

    def test_conjunction(self):
        das = DistributedAtomSpace()
        self.setup_animals_kb(das)

        exp1 = {
            "atom_type": "link",
            "type": "Inheritance",
            "targets": [
                {"atom_type": "variable", "name": "v1"},
                {"atom_type": "variable", "name": "v2"},
            ]
        }
        exp2 = {
            "atom_type": "link",
            "type": "Inheritance",
            "targets": [
                {"atom_type": "variable", "name": "v2"},
                {"atom_type": "variable", "name": "v3"},
            ]
        }
        query_answer = das.query([exp1, exp2])
        _check_query_answer(query_answer, [
            ['c93e1e758c53912638438e2a7d7f7b7f', '1c3bf151ea200b2d9e088a1178d060cb'],
            ['f31dfe97db782e8cec26de18dddf8965', '1c3bf151ea200b2d9e088a1178d060cb'],
            ['75756335011dcedb71a0d9a7bd2da9e8', '1c3bf151ea200b2d9e088a1178d060cb'],
            ['116df61c01859c710d178ba14a483509', 'b0f428929706d1d991e4d712ad08f9ab'],
            ['959924e3aab197af80a84c1ab261fd65', 'b0f428929706d1d991e4d712ad08f9ab'],
            ['906fa505ae3bc6336d80a5f9aaa47b3b', '959924e3aab197af80a84c1ab261fd65'],
            ['fbf03d17d6a40feff828a3f2c6e86f05', '1c3bf151ea200b2d9e088a1178d060cb'],
        ])

    def test_returned_assignment(self):
        das = DistributedAtomSpace()
        self.setup_animals_kb(das)

        exp = {
            "atom_type": "link",
            "type": "Inheritance",
            "targets": [
                {"atom_type": "variable", "name": "v1"},
                {"atom_type": "variable", "name": "v2"},
            ]
        }
        for query_answer in das._recursive_query(exp):
            link = query_answer.subgraph
            assignment = query_answer.assignment
            assert assignment.mapping["v1"] == link["targets"][0]["handle"]
            assert assignment.mapping["v2"] == link["targets"][1]["handle"]
