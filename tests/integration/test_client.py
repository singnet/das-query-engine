import pytest
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

from hyperon_das.client import FunctionsClient

from .remote_das_info import remote_das_host, remote_das_port


class TestVultrClientIntegration:
    @pytest.fixture()
    def server(self):
        return FunctionsClient(
            url=f'http://{remote_das_host}:{remote_das_port}/function/query-engine'
        )

    @pytest.fixture()
    def node_human(self):
        return ExpressionHasher.terminal_hash('Symbol', '"human"')

    @pytest.fixture()
    def node_monkey(self):
        return ExpressionHasher.terminal_hash('Symbol', '"monkey"')

    @pytest.fixture()
    def node_mammal(self):
        return ExpressionHasher.terminal_hash('Symbol', '"mammal"')

    @pytest.fixture()
    def node_similarity(self):
        return ExpressionHasher.terminal_hash('Symbol', 'Similarity')

    @pytest.fixture()
    def node_inheritance(self):
        return ExpressionHasher.terminal_hash('Symbol', 'Inheritance')

    @pytest.fixture()
    def link_similarity_concept_concept(self, node_human, node_monkey):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Expression'),
            [ExpressionHasher.terminal_hash('Symbol', 'Similarity'), node_human, node_monkey],
        )

    @pytest.fixture()
    def link_inheritance_concept_concept(self, node_human, node_mammal):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Expression'),
            [ExpressionHasher.terminal_hash('Symbol', 'Inheritance'), node_human, node_mammal],
        )

    def test_get_atom(
        self,
        server: FunctionsClient,
        node_human: str,
        node_monkey: str,
        node_similarity: str,
        link_similarity_concept_concept: str,
    ):
        result = server.get_atom(handle=node_human)
        assert result['handle'] == node_human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        result = server.get_atom(handle=node_monkey)
        assert result['handle'] == node_monkey
        assert result['name'] == '"monkey"'
        assert result['named_type'] == 'Symbol'

        result = server.get_atom(handle=link_similarity_concept_concept)
        assert result['handle'] == link_similarity_concept_concept
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [node_similarity, node_human, node_monkey]

    def test_get_node(
        self,
        server: FunctionsClient,
        node_human: str,
        node_monkey: str,
    ):
        result = server.get_node(node_type='Symbol', node_name='"human"')
        assert result['handle'] == node_human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        result = server.get_node(node_type='Symbol', node_name='"monkey"')
        assert result['handle'] == node_monkey
        assert result['name'] == '"monkey"'
        assert result['named_type'] == 'Symbol'

    def test_get_link(
        self,
        server: FunctionsClient,
        node_monkey: str,
        node_human: str,
        node_mammal: str,
        link_similarity_concept_concept: str,
        link_inheritance_concept_concept: str,
        node_similarity: str,
        node_inheritance: str,
    ):
        result = server.get_link(
            link_type='Expression', link_targets=[node_similarity, node_human, node_monkey]
        )
        assert result['handle'] == link_similarity_concept_concept
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [node_similarity, node_human, node_monkey]

        result = server.get_link(
            link_type='Expression', link_targets=[node_inheritance, node_human, node_mammal]
        )
        assert result['handle'] == link_inheritance_concept_concept
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [node_inheritance, node_human, node_mammal]

    def test_get_links(self, server: FunctionsClient):
        ret = server.get_links(link_type='Inheritance', target_types=['Verbatim', 'Verbatim'])
        assert ret is not None

    def test_count_atoms(self, server: FunctionsClient):
        ret = server.count_atoms()
        assert ret[0] == 21
        assert ret[1] == 43

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
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "Inheritance"},
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "variable", "name": "v2"},
                ],
            },
            {"no_iterator": True},
        )

        assert len(answer) == 12

        for link in answer:
            if link[1]['handle'] == link_inheritance_concept_concept:
                break

        handles = [target['handle'] for target in link[1]['targets']]

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
            {"no_iterator": True},
        )

        assert len(answer) == 14

        for link in answer:
            if link[1]['handle'] == link_similarity_concept_concept:
                break

        handles = [target['handle'] for target in link[1]['targets']]

        assert handles == [node_human, node_monkey]

    def test_get_incoming_links(self, server: FunctionsClient, node_human: str):
        expected_handles = [
            'bad7472f41a0e7d601ca294eb4607c3a',
            'a45af31b43ee5ea271214338a5a5bd61',
            '16f7e407087bfa0b35b13d13a1aadcae',
            '2a8a69c01305563932b957de4b3a9ba6',
            '2c927fdc6c0f1272ee439ceb76a6d1a4',
            'c93e1e758c53912638438e2a7d7f7b7f',
            'b5459e299a5c5e8662c427f7e01b3bf1',
        ]

        expected_atoms = [server.get_atom(handle) for handle in expected_handles]

        expected_atoms_targets = []
        for atom in expected_atoms:
            targets_document = []
            for target in atom['targets']:
                targets_document.append(server.get_atom(target))
            expected_atoms_targets.append([atom, targets_document])

        response_handles = server.get_incoming_links(
            node_human, targets_document=False, handles_only=True
        )
        assert sorted(response_handles) == sorted(expected_handles)
        response_handles = server.get_incoming_links(
            node_human, targets_document=True, handles_only=True
        )
        assert sorted(response_handles) == sorted(expected_handles)

        response_atoms = server.get_incoming_links(
            node_human, targets_document=False, handles_only=False
        )
        for atom in response_atoms:
            assert atom in expected_atoms
        response_atoms = server.get_incoming_links(node_human)
        for atom in response_atoms:
            assert atom in expected_atoms

        response_atoms_targets = server.get_incoming_links(
            node_human, targets_document=True, handles_only=False
        )
        for atom_targets in response_atoms_targets:
            assert atom_targets in expected_atoms_targets
