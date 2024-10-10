import pytest

import hyperon_das.link_filters as link_filter
from hyperon_das.client import FunctionsClient

from .helpers import metta_animal_base_handles
from .remote_das_info import remote_das_host, remote_das_port


class TestVultrClientIntegration:
    @pytest.fixture()
    def server(self):
        return FunctionsClient(host=remote_das_host, port=remote_das_port)

    def test_get_atom(self, server: FunctionsClient):
        result = server.get_atom(handle=metta_animal_base_handles.human)
        assert result['handle'] == metta_animal_base_handles.human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        result = server.get_atom(handle=metta_animal_base_handles.monkey)
        assert result['handle'] == metta_animal_base_handles.monkey
        assert result['name'] == '"monkey"'
        assert result['named_type'] == 'Symbol'

        result = server.get_atom(handle=metta_animal_base_handles.similarity_human_monkey)
        assert result['handle'] == metta_animal_base_handles.similarity_human_monkey
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [
            metta_animal_base_handles.Similarity,
            metta_animal_base_handles.human,
            metta_animal_base_handles.monkey,
        ]

    def test_get_links(self, server: FunctionsClient):
        links1 = server.get_links(
            link_filter.FlatTypeTemplate(['Symbol', 'Symbol', 'Symbol'], 'Expression')
        )
        links2 = server.get_links(link_filter.NamedType('Expression'))
        assert len(links1) == 43
        assert len(links2) == 43

    def test_count_atoms(self, server: FunctionsClient):
        ret = server.count_atoms()
        assert ret == {'atom_count': 66}

    def test_query(self, server: FunctionsClient):
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
            if link.subgraph['handle'] == metta_animal_base_handles.inheritance_human_mammal:
                break

        handles = [target['handle'] for target in link.subgraph['targets']]

        assert len(handles) == 3
        assert handles[1] == metta_animal_base_handles.human
        assert handles[2] == metta_animal_base_handles.mammal

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
            if link.subgraph['handle'] == metta_animal_base_handles.similarity_human_monkey:
                break

        handles = [target['handle'] for target in link.subgraph['targets']]

        assert len(handles) == 3
        assert handles[1] == metta_animal_base_handles.human
        assert handles[2] == metta_animal_base_handles.monkey

    def test_get_incoming_links(self, server: FunctionsClient):
        expected_handles = [
            metta_animal_base_handles.similarity_human_monkey,
            metta_animal_base_handles.similarity_human_chimp,
            metta_animal_base_handles.similarity_human_ent,
            metta_animal_base_handles.similarity_monkey_human,
            metta_animal_base_handles.similarity_chimp_human,
            metta_animal_base_handles.similarity_ent_human,
            metta_animal_base_handles.inheritance_human_mammal,
            metta_animal_base_handles.human_typedef,
        ]

        expected_atoms = [server.get_atom(handle) for handle in expected_handles]

        expected_atoms_targets = []
        for atom in expected_atoms:
            targets_document = []
            for target in atom['targets']:
                targets_document.append(server.get_atom(target))
            expected_atoms_targets.append([atom, targets_document])

        response_handles = server.get_incoming_links(
            metta_animal_base_handles.human, targets_document=False, handles_only=True
        )
        assert all(isinstance(handle, str) for handle in response_handles)
        assert len(response_handles) == 8
        # response_handles has an extra link (named_type == MettaType) defining
        #     the metta type of '"human"'--> Type
        # response_handles has an extra link (named_type == Expression) defining
        #     the metta expression (: "human" Type)
        # assert len(set(response_handles).difference(set(expected_handles))) == 1
        response_handles = server.get_incoming_links(
            metta_animal_base_handles.human, targets_document=True, handles_only=True
        )
        assert all(isinstance(handle, str) for handle in response_handles)
        assert len(response_handles) == 8
        # assert len(set(response_handles).difference(set(expected_handles))) == 1

        response_atoms = server.get_incoming_links(
            metta_animal_base_handles.human, targets_document=False, handles_only=False
        )
        assert len(response_atoms) == 8
        for atom in response_atoms:
            assert isinstance(atom, dict), f"Each item in body must be a dict. Received: {atom}"
            if len(atom["targets"]) == 3:
                assert atom in expected_atoms

        response_atoms = server.get_incoming_links(metta_animal_base_handles.human)
        assert len(response_atoms) == 8
        for atom in response_atoms:
            assert isinstance(atom, dict), f"Each item in body must be a dict. Received: {atom}"
            if len(atom["targets"]) == 3:
                assert atom in expected_atoms

        response_atoms_targets = server.get_incoming_links(
            metta_animal_base_handles.human, targets_document=True, handles_only=False
        )
        assert len(response_atoms_targets) == 8
        for atom in response_atoms_targets:
            assert isinstance(atom, dict), f"Each item in body must be a dict. Received: {atom}"
            atom_targets = atom.pop('targets_document')
            if len(atom_targets) == 3:
                assert [atom, atom_targets] in expected_atoms_targets
