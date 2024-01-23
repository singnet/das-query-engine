import pytest
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

from hyperon_das.client import FunctionsClient


# AWS function is offline
class AWSClientIntegration:
    @pytest.fixture()
    def server(self):
        return FunctionsClient(url='http://44.198.65.35/prod/query-engine')

    @pytest.fixture()
    def node_human(self):
        return ExpressionHasher.terminal_hash('Concept', 'human')

    @pytest.fixture()
    def node_monkey(self):
        return ExpressionHasher.terminal_hash('Concept', 'monkey')

    @pytest.fixture()
    def node_mammal(self):
        return ExpressionHasher.terminal_hash('Concept', 'mammal')

    @pytest.fixture()
    def link_similarity_concept_concept(self, node_human, node_monkey):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Similarity'),
            [node_human, node_monkey],
        )

    @pytest.fixture()
    def link_inheritance_concept_concept(self, node_human, node_mammal):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Inheritance'),
            [node_human, node_mammal],
        )

    def test_get_atom(
        self,
        server: FunctionsClient,
        node_human: str,
        node_monkey: str,
        link_similarity_concept_concept: str,
    ):
        result = server.get_atom(handle=node_human)
        assert result['handle'] == node_human
        assert result['name'] == 'human'
        assert result['named_type'] == 'Concept'

        result = server.get_atom(handle=node_monkey)
        assert result['handle'] == node_monkey
        assert result['name'] == 'monkey'
        assert result['named_type'] == 'Concept'

        result = server.get_atom(handle=link_similarity_concept_concept)
        assert result['handle'] == link_similarity_concept_concept
        assert result['named_type'] == 'Similarity'
        assert result['targets'] == [node_human, node_monkey]

    def test_get_node(self, server: FunctionsClient, node_human: str, node_monkey: str):
        result = server.get_node(node_type='Concept', node_name='human')
        assert result['handle'] == node_human
        assert result['name'] == 'human'
        assert result['named_type'] == 'Concept'

        result = server.get_node(node_type='Concept', node_name='monkey')
        assert result['handle'] == node_monkey
        assert result['name'] == 'monkey'
        assert result['named_type'] == 'Concept'

    def test_get_link(
        self,
        server: FunctionsClient,
        node_monkey: str,
        node_human: str,
        node_mammal: str,
        link_similarity_concept_concept: str,
        link_inheritance_concept_concept: str,
    ):
        result = server.get_link(link_type='Similarity', link_targets=[node_human, node_monkey])
        assert result['handle'] == link_similarity_concept_concept
        assert result['named_type'] == 'Similarity'
        assert result['targets'] == [node_human, node_monkey]

        result = server.get_link(link_type='Inheritance', link_targets=[node_human, node_mammal])
        assert result['handle'] == link_inheritance_concept_concept
        assert result['named_type'] == 'Inheritance'
        assert result['targets'] == [node_human, node_mammal]

    def test_get_links(self, server: FunctionsClient):
        ret = server.get_links(link_type='Inheritance', target_types=['Verbatim', 'Verbatim'])
        assert ret is not None

    def test_count_atoms(self, server: FunctionsClient):
        ret = server.count_atoms()
        assert ret[0] == 14
        assert ret[1] == 26

    def test_query(
        self,
        server: FunctionsClient,
        link_similarity_concept_concept,
        link_inheritance_concept_concept,
        node_human,
        node_monkey,
        node_mammal,
    ):
        answer = server.query(
            {
                "atom_type": "link",
                "type": "Inheritance",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "variable", "name": "v2"},
                ],
            },
            {"toplevel_only": False},
        )

        assert len(answer) == 12

        for link in answer:
            if link['handle'] == link_inheritance_concept_concept:
                break

        handles = [target['handle'] for target in link['targets']]

        assert handles == [node_human, node_mammal]

        answer = server.query(
            {
                "atom_type": "link",
                "type": "Similarity",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "variable", "name": "v2"},
                ],
            },
            {"toplevel_only": False},
        )

        assert len(answer) == 14

        for link in answer:
            if link['handle'] == link_similarity_concept_concept:
                break

        handles = [target['handle'] for target in link['targets']]

        assert handles == [node_human, node_monkey]


class TestVultrClientIntegration:
    @pytest.fixture()
    def server(self):
        return FunctionsClient(url='http://45.63.85.59:8080/function/query-engine')

    @pytest.fixture()
    def node_human(self):
        return ExpressionHasher.terminal_hash('Concept', 'human')

    @pytest.fixture()
    def node_monkey(self):
        return ExpressionHasher.terminal_hash('Concept', 'monkey')

    @pytest.fixture()
    def node_mammal(self):
        return ExpressionHasher.terminal_hash('Concept', 'mammal')

    @pytest.fixture()
    def link_similarity_concept_concept(self, node_human, node_monkey):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Similarity'),
            [node_human, node_monkey],
        )

    @pytest.fixture()
    def link_inheritance_concept_concept(self, node_human, node_mammal):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Inheritance'),
            [node_human, node_mammal],
        )

    def test_get_atom(
        self,
        server: FunctionsClient,
        node_human: str,
        node_monkey: str,
        link_similarity_concept_concept: str,
    ):
        result = server.get_atom(handle=node_human)
        assert result['handle'] == node_human
        assert result['name'] == 'human'
        assert result['named_type'] == 'Concept'

        result = server.get_atom(handle=node_monkey)
        assert result['handle'] == node_monkey
        assert result['name'] == 'monkey'
        assert result['named_type'] == 'Concept'

        result = server.get_atom(handle=link_similarity_concept_concept)
        assert result['handle'] == link_similarity_concept_concept
        assert result['named_type'] == 'Similarity'
        assert result['targets'] == [node_human, node_monkey]

    def test_get_node(self, server: FunctionsClient, node_human: str, node_monkey: str):
        result = server.get_node(node_type='Concept', node_name='human')
        assert result['handle'] == node_human
        assert result['name'] == 'human'
        assert result['named_type'] == 'Concept'

        result = server.get_node(node_type='Concept', node_name='monkey')
        assert result['handle'] == node_monkey
        assert result['name'] == 'monkey'
        assert result['named_type'] == 'Concept'

    def test_get_link(
        self,
        server: FunctionsClient,
        node_monkey: str,
        node_human: str,
        node_mammal: str,
        link_similarity_concept_concept: str,
        link_inheritance_concept_concept: str,
    ):
        result = server.get_link(link_type='Similarity', link_targets=[node_human, node_monkey])
        assert result['handle'] == link_similarity_concept_concept
        assert result['named_type'] == 'Similarity'
        assert result['targets'] == [node_human, node_monkey]

        result = server.get_link(link_type='Inheritance', link_targets=[node_human, node_mammal])
        assert result['handle'] == link_inheritance_concept_concept
        assert result['named_type'] == 'Inheritance'
        assert result['targets'] == [node_human, node_mammal]

    def test_get_links(self, server: FunctionsClient):
        ret = server.get_links(link_type='Inheritance', target_types=['Verbatim', 'Verbatim'])
        assert ret is not None

    def test_count_atoms(self, server: FunctionsClient):
        ret = server.count_atoms()
        assert ret[0] == 14
        assert ret[1] == 26

    def test_query(
        self,
        server: FunctionsClient,
        link_similarity_concept_concept,
        link_inheritance_concept_concept,
        node_human,
        node_monkey,
        node_mammal,
    ):
        answer = server.query(
            {
                "atom_type": "link",
                "type": "Inheritance",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "variable", "name": "v2"},
                ],
            },
            {"toplevel_only": False},
        )

        assert len(answer) == 12

        for link in answer:
            if link['handle'] == link_inheritance_concept_concept:
                break

        handles = [target['handle'] for target in link['targets']]

        assert handles == [node_human, node_mammal]

        answer = server.query(
            {
                "atom_type": "link",
                "type": "Similarity",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "variable", "name": "v2"},
                ],
            },
            {"toplevel_only": False},
        )

        assert len(answer) == 14

        for link in answer:
            if link['handle'] == link_similarity_concept_concept:
                break

        handles = [target['handle'] for target in link['targets']]

        assert handles == [node_human, node_monkey]

    # WIP
    def test_get_incoming_links(self, server: FunctionsClient):
        pass
