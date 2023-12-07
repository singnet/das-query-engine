import json
from unittest import mock

import pytest
from hyperon_das_atomdb import UNORDERED_LINK_TYPES, WILDCARD

from hyperon_das.client import FunctionsClient
from hyperon_das.das import DistributedAtomSpace
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


# verify the real need for this test class
class DistributedAtomSpace:
    @pytest.fixture()
    def das(self):
        return DistributedAtomSpace()

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

    def test_get_node(self, das: DistributedAtomSpace):
        human = das.get_node('Concept', "human")
        human_node = das.get_node_handle('Concept', 'human')
        assert human["handle"] == human

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
