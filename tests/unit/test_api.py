import json
from unittest import mock

import pytest
from hyperon_das_atomdb import UNORDERED_LINK_TYPES, WILDCARD

from hyperon_das.api import DistributedAtomSpace
from hyperon_das.client import FunctionsClient
from hyperon_das.exceptions import QueryParametersException
from hyperon_das.pattern_matcher.pattern_matcher import And, Link, Not, Variable
from hyperon_das.utils import QueryOutputFormat
from tests.unit.mock import DistributedAtomSpaceMock


class NodeContainer:
    def __init__(self, das):
        concept = "Concept"
        self.animal = das.get_node(concept, "animal")
        self.mammal = das.get_node(concept, "mammal")
        self.reptile = das.get_node(concept, "reptile")
        self.plant = das.get_node(concept, "plant")
        self.human = das.get_node(concept, "human")
        self.monkey = das.get_node(concept, "monkey")
        self.chimp = das.get_node(concept, "chimp")
        self.earthworm = das.get_node(concept, "earthworm")
        self.snake = das.get_node(concept, "snake")
        self.triceratops = das.get_node(concept, "triceratops")
        self.rhino = das.get_node(concept, "rhino")
        self.vine = das.get_node(concept, "vine")
        self.ent = das.get_node(concept, "ent")
        self.dinosaur = das.get_node(concept, "dinosaur")


class TestDistributedAtomSpace:
    @pytest.fixture()
    def das(self):
        return DistributedAtomSpaceMock()

    @pytest.fixture()
    def nodes(self, das: DistributedAtomSpace):
        return NodeContainer(das)

    @pytest.fixture()
    def all_nodes(self, nodes: NodeContainer):
        return [
            nodes.animal,
            nodes.mammal,
            nodes.reptile,
            nodes.plant,
            nodes.human,
            nodes.monkey,
            nodes.chimp,
            nodes.earthworm,
            nodes.snake,
            nodes.triceratops,
            nodes.rhino,
            nodes.vine,
            nodes.ent,
            nodes.dinosaur,
        ]

    @pytest.fixture()
    def all_similarities(self, nodes):
        return [
            set([nodes.human, nodes.monkey]),
            set([nodes.human, nodes.chimp]),
            set([nodes.chimp, nodes.monkey]),
            set([nodes.earthworm, nodes.snake]),
            set([nodes.triceratops, nodes.rhino]),
            set([nodes.vine, nodes.snake]),
            set([nodes.ent, nodes.human]),
        ]

    @pytest.fixture()
    def all_inheritances(self, nodes):
        return [
            [nodes.human, nodes.mammal],
            [nodes.monkey, nodes.mammal],
            [nodes.chimp, nodes.mammal],
            [nodes.mammal, nodes.animal],
            [nodes.reptile, nodes.animal],
            [nodes.snake, nodes.reptile],
            [nodes.dinosaur, nodes.reptile],
            [nodes.triceratops, nodes.dinosaur],
            [nodes.earthworm, nodes.animal],
            [nodes.rhino, nodes.mammal],
            [nodes.vine, nodes.plant],
            [nodes.ent, nodes.plant],
        ]

    def test_query_method(self, das: DistributedAtomSpace):
        V1 = Variable("V1")
        V2 = Variable("V2")
        V3 = Variable("V3")

        and_expression = And(
            [
                Link("Inheritance", ordered=True, targets=[V1, V2]),
                Link("Inheritance", ordered=True, targets=[V2, V3]),
            ]
        )

        ret = das.pattern_matcher_query(and_expression, {'return_type': QueryOutputFormat.HANDLE})
        assert len(ret['mapping']) == 7
        assert ret['negation'] == False

        ret_atom_info = das.pattern_matcher_query(
            and_expression, {'return_type': QueryOutputFormat.ATOM_INFO}
        )
        assert len(ret_atom_info['mapping']) == 7
        assert ret['negation'] == False

        ret_json = das.pattern_matcher_query(
            and_expression, {'return_type': QueryOutputFormat.JSON}
        )
        assert len(json.loads(ret_json['mapping'])) == 7
        assert ret['negation'] == False

        not_expression = Not(Link("Inheritance", ordered=True, targets=[V1, V2]))
        ret = das.pattern_matcher_query(not_expression)
        assert ret['negation'] == True

    # TODO: Adjust Mock class
    def query_toplevel_only_success(self, das: DistributedAtomSpace):
        expression = Link(
            "Evaluation",
            ordered=True,
            targets=[Variable('V1'), Variable('V2')],
        )

        ret = das.pattern_matcher_query(
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

    def test_query_toplevel_wrong_parameter(self, das: DistributedAtomSpace):
        expression = Link(
            "Evaluation",
            ordered=True,
            targets=[Variable('V1'), Variable('V2')],
        )
        with pytest.raises(QueryParametersException) as exc_info:
            das.pattern_matcher_query(
                expression,
                {
                    'parameter_fake': True,
                    'return_type': QueryOutputFormat.ATOM_INFO,
                },
            )
        assert exc_info.type is QueryParametersException
        assert exc_info.value.args[1] == "possible values ['toplevel_only', 'return_type']"

    def test_get_node(self, das: DistributedAtomSpace):
        human = das.get_node('Concept', "human")
        human_document = das.get_node('Concept', "human", output_format=QueryOutputFormat.ATOM_INFO)
        assert human_document["handle"] == human
        assert human_document["type"] == 'Concept'
        assert human_document["name"] == "human"

    def test_get_atom(self, das: DistributedAtomSpace, nodes: NodeContainer):
        human = nodes.human
        mammal = nodes.mammal
        assert human == das.get_atom(human)
        human_document = das.get_atom(human, output_format=QueryOutputFormat.ATOM_INFO)
        assert human_document["handle"] == human
        assert human_document["type"] == 'Concept'
        assert human_document["name"] == "human"
        link = das.get_link('Inheritance', [human, mammal])
        atom = das.get_atom(link)
        assert atom == link

    def test_get_nodes(self, das: DistributedAtomSpace, nodes: NodeContainer):
        human = nodes.human
        human_document = das.get_nodes(
            'Concept', "human", output_format=QueryOutputFormat.ATOM_INFO
        )[0]
        assert human_document["handle"] == human
        assert human_document["type"] == 'Concept'
        assert human_document["name"] == "human"

    def test_get_link(self, das: DistributedAtomSpace, nodes: NodeContainer):
        human = nodes.human
        monkey = nodes.monkey
        link_handle = das.get_link('Similarity', [human, monkey])
        link = das.get_link(
            'Similarity',
            [human, monkey],
            output_format=QueryOutputFormat.ATOM_INFO,
        )
        assert link["handle"] == link_handle
        assert link["type"] == 'Similarity'
        assert link["template"] == ['Similarity', 'Concept', 'Concept']

    def test_get_link_targets(self, das, all_similarities, all_inheritances):
        test_links = [('Similarity', list(v)) for v in all_similarities] + [
            ('Inheritance', v) for v in all_inheritances
        ]

        for link_type, targets in test_links:
            link_handle = das.get_link(link_type, targets)
            answer = das.get_link_targets(link_handle)
            assert len(answer) == len(targets)
            if link_type == 'Similarity':
                for node in targets:
                    assert node in answer
            else:
                # TODO: remove "sorted" and make this test pass
                for n1, n2 in zip(sorted(answer), sorted(targets)):
                    assert n1 == n2

    def test_get_link_type(self, das, all_inheritances, all_similarities):
        test_links = [('Similarity', list(v)) for v in all_similarities] + [
            ('Inheritance', v) for v in all_inheritances
        ]

        for link_type, targets in test_links:
            link_handle = das.get_link(link_type, targets)
            answer = das.get_link_type(link_handle)
            assert answer == link_type

    def test_get_node_type(self, das: DistributedAtomSpace, all_nodes):
        for node in all_nodes:
            node_type = das.get_node_type(node)
            assert node_type == 'Concept'

    def test_get_node_name(self, das: DistributedAtomSpace, nodes):
        test_nodes = [
            (nodes.animal, "animal"),
            (nodes.mammal, "mammal"),
            (nodes.reptile, "reptile"),
            (nodes.plant, "plant"),
            (nodes.human, "human"),
            (nodes.monkey, "monkey"),
            (nodes.chimp, "chimp"),
            (nodes.earthworm, "earthworm"),
            (nodes.snake, "snake"),
            (nodes.triceratops, "triceratops"),
            (nodes.rhino, "rhino"),
            (nodes.vine, "vine"),
            (nodes.ent, "ent"),
            (nodes.dinosaur, "dinosaur"),
        ]

        for node_handle, node_name in test_nodes:
            name = das.get_node_name(node_handle)
            assert name == node_name

        with pytest.raises(Exception):
            name = das.get_node_name("blah")

    def test_get_links_with_link_templates(self, das, all_similarities):
        link_handles = das.get_links(link_type='Similarity', target_types=['Concept', 'Concept'])
        links = das.get_links(
            link_type='Similarity',
            target_types=['Concept', 'Concept'],
            output_format=QueryOutputFormat.ATOM_INFO,
        )
        assert len(link_handles) == len(links)
        for link in links:
            assert link["handle"] in link_handles
            assert link["type"] == 'Similarity'
            assert link["template"] == ['Similarity', 'Concept', 'Concept']
            assert set(link["targets"]) in all_similarities

    def test_get_links_with_patterns(self, das, all_inheritances, nodes):
        def _check_pattern(link_type, targets, expected):
            link_handles = list(set(das.get_links(link_type=link_type, targets=targets)))
            links = das.get_links(
                link_type=link_type,
                targets=targets,
                output_format=QueryOutputFormat.ATOM_INFO,
            )
            assert len(link_handles) == len(expected)
            for link in links:
                assert link["handle"] in link_handles
                assert link["type"] == link_type or link_type == WILDCARD
                if link_type == 'Similarity':
                    assert link["template"] == [
                        'Similarity',
                        'Concept',
                        'Concept',
                    ]
                if link_type == 'Inheritance':
                    assert link["template"] == [
                        'Inheritance',
                        'Concept',
                        'Concept',
                    ]
                if link["type"] in UNORDERED_LINK_TYPES:
                    assert set(link["targets"]) in expected
                else:
                    assert link["targets"] in expected

        # _check_pattern(
        #    'Similarity',
        #    [nodes.human, WILDCARD],
        #    [
        #        set([nodes.human, nodes.monkey]),
        #        set([nodes.human, nodes.chimp]),
        #        set([nodes.human, nodes.ent]),
        #    ],
        # )
        # _check_pattern(
        #    'Similarity',
        #    [WILDCARD, nodes.human],
        #    [
        #        set([nodes.human, nodes.monkey]),
        #        set([nodes.human, nodes.chimp]),
        #        set([nodes.human, nodes.ent]),
        #    ],
        # )
        _check_pattern('Inheritance', [WILDCARD, WILDCARD], all_inheritances)
        _check_pattern(
            'Inheritance',
            [nodes.human, WILDCARD],
            [
                [nodes.human, nodes.mammal],
            ],
        )
        _check_pattern(
            'Inheritance',
            [WILDCARD, nodes.animal],
            [
                [nodes.mammal, nodes.animal],
                [nodes.reptile, nodes.animal],
                [nodes.earthworm, nodes.animal],
            ],
        )
        _check_pattern(WILDCARD, [nodes.mammal, nodes.human], [])
        # TODO: Implemente patterns in DatabaseMock
        # _check_pattern(WILDCARD, [nodes.chimp, nodes.monkey], [
        #     set([nodes.chimp, nodes.monkey]),
        # ])
        # _check_pattern(WILDCARD, [nodes.monkey, nodes.chimp], [
        #     set([nodes.chimp, nodes.monkey]),
        # ])
        # _check_pattern(WILDCARD, [nodes.human, nodes.mammal], [
        #     [nodes.human, nodes.mammal],
        # ])
        # _check_pattern(WILDCARD, [nodes.human, WILDCARD], [
        #     set([nodes.human, nodes.monkey]),
        #     set([nodes.human, nodes.chimp]),
        #     set([nodes.human, nodes.ent]),
        #     [nodes.human, nodes.mammal],
        # ])

    def test_attach_remote(self):
        das = DistributedAtomSpace()
        assert das.remote_das == []
        with mock.patch(
            'hyperon_das.api.DistributedAtomSpace._is_server_connect',
            return_value=True,
        ):
            das.attach_remote(host='0.0.0.0', port='8000')
        assert len(das.remote_das) == 1
        assert isinstance(das.remote_das[0], FunctionsClient)
        assert das.remote_das[0].url == 'http://0.0.0.0:8000/function/query-engine'
        assert das.remote_das[0].name == 'server-0'
