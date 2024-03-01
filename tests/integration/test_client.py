import pytest
from hyperon_das_atomdb import AtomDB

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
        return AtomDB.node_handle('Concept', 'human')

    @pytest.fixture()
    def node_monkey(self):
        return AtomDB.node_handle('Concept', 'monkey')

    @pytest.fixture()
    def node_mammal(self):
        return AtomDB.node_handle('Concept', 'mammal')

    @pytest.fixture()
    def link_similarity_concept_concept(self, node_human, node_monkey):
        return AtomDB.link_handle('Similarity', [node_human, node_monkey])

    @pytest.fixture()
    def link_inheritance_concept_concept(self, node_human, node_mammal):
        return AtomDB.link_handle('Inheritance', [node_human, node_mammal])

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
