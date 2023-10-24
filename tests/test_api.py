import json

import pytest

from hyperon_das.api import DistributedAtomSpace
from hyperon_das.exceptions import QueryParametersException
from hyperon_das.pattern_matcher.pattern_matcher import And, Link, Variable
from hyperon_das.utils import QueryOutputFormat


class TestDistributedAtomSpace:
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
    def hash_table_api(self, all_nodes, all_links):
        api = DistributedAtomSpace(database='ram_only')
        for node in all_nodes:
            api.add_node(node)
        for link in all_links:
            api.add_link(link)
        return api

    def test_query_handle(self, hash_table_api: DistributedAtomSpace):
        V1 = Variable("V1")
        V2 = Variable("V2")
        V3 = Variable("V3")

        expression = And(
            [
                Link("Inheritance", ordered=True, targets=[V1, V2]),
                Link("Inheritance", ordered=True, targets=[V2, V3]),
            ]
        )

        ret = hash_table_api.query(
            expression, {'return_type': QueryOutputFormat.HANDLE}
        )

        expected_values = [
            {
                'V1': 'd03e59654221c1e8fcda404fd5c8d6cb',
                'V2': '08126b066d32ee37743e255a2558cccd',
                'V3': 'b99ae727c787f1b13b452fd4c9ce1b9a',
            },
            {
                'V1': 'c1db9b517073e51eb7ef6fed608ec204',
                'V2': 'b99ae727c787f1b13b452fd4c9ce1b9a',
                'V3': '0a32b476852eeb954979b87f5f6cb7af',
            },
            {
                'V1': '5b34c54bee150c04f9fa584b899dc030',
                'V2': 'bdfe4e7a431f73386f37c6448afe5840',
                'V3': '0a32b476852eeb954979b87f5f6cb7af',
            },
            {
                'V1': '99d18c702e813b07260baf577c60c455',
                'V2': 'bdfe4e7a431f73386f37c6448afe5840',
                'V3': '0a32b476852eeb954979b87f5f6cb7af',
            },
            {
                'V1': 'af12f10f9ae2002a1607ba0b47ba8407',
                'V2': 'bdfe4e7a431f73386f37c6448afe5840',
                'V3': '0a32b476852eeb954979b87f5f6cb7af',
            },
            {
                'V1': '08126b066d32ee37743e255a2558cccd',
                'V2': 'b99ae727c787f1b13b452fd4c9ce1b9a',
                'V3': '0a32b476852eeb954979b87f5f6cb7af',
            },
            {
                'V1': '1cdffc6b0b89ff41d68bec237481d1e1',
                'V2': 'bdfe4e7a431f73386f37c6448afe5840',
                'V3': '0a32b476852eeb954979b87f5f6cb7af',
            },
        ]

        ret_list = eval('[' + ret[1:-1] + ']')

        number_matches = 0
        for item in ret_list:
            if item in expected_values:
                number_matches += 1

        assert number_matches == 7

        ret_atom_info = hash_table_api.query(
            expression, {'return_type': QueryOutputFormat.ATOM_INFO}
        )

        assert len(eval(ret_atom_info)) == 7

        ret_json = hash_table_api.query(
            expression, {'return_type': QueryOutputFormat.JSON}
        )

        assert len(json.loads(ret_json)) == 7

    def test_query_toplevel_only_success(
        self, hash_table_api: DistributedAtomSpace
    ):
        hash_table_api.add_link(
            {
                'type': 'Evaluation',
                'targets': [
                    {'type': 'Predicate', 'name': 'Predicate:has_name'},
                    {
                        'type': 'Evaluation',
                        'targets': [
                            {
                                'type': 'Predicate',
                                'name': 'Predicate:has_name',
                            },
                            {
                                'type': 'Set',
                                'targets': [
                                    {
                                        'type': 'Reactome',
                                        'name': 'Reactome:R-HSA-164843',
                                    },
                                    {
                                        'type': 'Concept',
                                        'name': 'Concept:2-LTR circle formation',
                                    },
                                ],
                            },
                        ],
                    },
                ],
            }
        )

        expression = Link(
            "Evaluation",
            ordered=True,
            targets=[Variable('V1'), Variable('V2')],
        )

        ret = hash_table_api.query(
            expression,
            {
                'toplevel_only': True,
                'return_type': QueryOutputFormat.ATOM_INFO,
            },
        )

        expected = [
            {
                "V1": {
                    "type": "Predicate",
                    "name": "Predicate:has_name",
                    'atom_type': 'node',
                },
                "V2": {
                    "type": "Evaluation",
                    "targets": [
                        {"type": "Predicate", "name": "Predicate:has_name"},
                        {
                            "type": "Set",
                            "targets": [
                                {
                                    "type": "Reactome",
                                    "name": "Reactome:R-HSA-164843",
                                },
                                {
                                    "type": "Concept",
                                    "name": "Concept:2-LTR circle formation",
                                },
                            ],
                        },
                    ],
                    'atom_type': 'link',
                },
            }
        ]

        assert ret == str(expected)

    def test_query_toplevel_wrong_parameter(
        self, hash_table_api: DistributedAtomSpace
    ):
        expression = Link(
            "Evaluation",
            ordered=True,
            targets=[Variable('V1'), Variable('V2')],
        )
        with pytest.raises(QueryParametersException) as exc_info:
            hash_table_api.query(
                expression,
                {
                    'parameter_fake': True,
                    'return_type': QueryOutputFormat.ATOM_INFO,
                },
            )
        assert exc_info.type is QueryParametersException
        assert (
            exc_info.value.args[1]
            == "possible values ['toplevel_only', 'return_type']"
        )
