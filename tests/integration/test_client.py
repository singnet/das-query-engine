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
        server.get_links('Expression', no_iterator=True)
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

        assert len(handles) == 3
        assert handles[1] == node_human
        assert handles[2] == node_mammal

        answer = server.query(
            {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "Similarity"},
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

        assert len(handles) == 3
        assert handles[1] == node_human
        assert handles[2] == node_monkey

    def test_get_incoming_links(self, server: FunctionsClient, node_human: str):
        expression = ExpressionHasher.named_type_hash("Expression")
        similarity = ExpressionHasher.terminal_hash("Symbol", "Similarity")
        inheritance = ExpressionHasher.terminal_hash("Symbol", "Inheritance")
        mammal = ExpressionHasher.terminal_hash("Symbol", '"mammal"')
        human = ExpressionHasher.terminal_hash("Symbol", '"human"')
        monkey = ExpressionHasher.terminal_hash("Symbol", '"monkey"')
        chimp = ExpressionHasher.terminal_hash("Symbol", '"chimp"')
        ent = ExpressionHasher.terminal_hash("Symbol", '"ent"')
        expected_handles = [
            ExpressionHasher.expression_hash(expression, [similarity, human, monkey]),
            ExpressionHasher.expression_hash(expression, [similarity, human, chimp]),
            ExpressionHasher.expression_hash(expression, [similarity, human, ent]),
            ExpressionHasher.expression_hash(expression, [similarity, monkey, human]),
            ExpressionHasher.expression_hash(expression, [similarity, chimp, human]),
            ExpressionHasher.expression_hash(expression, [similarity, ent, human]),
            ExpressionHasher.expression_hash(expression, [inheritance, human, mammal]),
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
        assert len(response_handles) == 8
        # response_handles has an extra link defining the metta type of '"human"'--> Type
        assert len(set(response_handles).difference(set(expected_handles))) == 1
        response_handles = server.get_incoming_links(
            node_human, targets_document=True, handles_only=True
        )
        assert len(response_handles) == 8
        assert len(set(response_handles).difference(set(expected_handles))) == 1

        response_atoms = server.get_incoming_links(
            node_human, targets_document=False, handles_only=False
        )
        assert len(response_atoms) == 8
        for atom in response_atoms:
            if len(atom["targets"]) == 3:
                assert atom in expected_atoms

        response_atoms = server.get_incoming_links(node_human)
        assert len(response_atoms) == 8
        for atom in response_atoms:
            if len(atom["targets"]) == 3:
                assert atom in expected_atoms

        response_atoms_targets = server.get_incoming_links(
            node_human, targets_document=True, handles_only=False
        )
        assert len(response_atoms_targets) == 8
        for atom_targets in response_atoms_targets:
            if len(atom_targets[0]["targets"]) == 3:
                assert atom_targets in expected_atoms_targets
