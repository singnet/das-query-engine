from unittest import mock

import pytest
from hyperon_das_atomdb import AtomDoesNotExist, LinkDoesNotExist, NodeDoesNotExist

from hyperon_das import DistributedAtomSpace
from hyperon_das.exceptions import GetTraversalCursorException
from hyperon_das.traverse_engines import TraverseEngine

from .helpers import metta_animal_base_handles
from .remote_das_info import remote_das_host, remote_das_port


class TestRemoteDistributedAtomSpace:
    """Integration tests with OpenFaas function on the Vultr server. Using the Animal Knowledge Base"""

    @pytest.fixture
    def remote_das(self):
        with mock.patch(
            'hyperon_das.query_engines.RemoteQueryEngine._connect_server',
            return_value=f'http://{remote_das_host}:{remote_das_port}/function/query-engine',
        ):
            return DistributedAtomSpace(
                query_engine='remote', host=remote_das_host, port=remote_das_port
            )  # vultr

    def traversal(self, das: DistributedAtomSpace, handle: str):
        return das.get_traversal_cursor(handle)

    # TODO: uncomment the test after the handshake method in servless-function is working
    # def test_server_connection(self):
    #     try:
    #         das = DistributedAtomSpace(
    #             query_engine='remote', host=remote_das_host, port=remote_das_port
    #         )
    #     except Exception as e:
    #         pytest.fail(f'Connection with OpenFaaS server fail, Details: {str(e)}')
    #     if not das.query_engine.remote_das.url:
    #         pytest.fail('Connection with server fail')
    #     assert (
    #         das.query_engine.remote_das.url
    #         == f'http://{remote_das_host}:{remote_das_port}/function/query-engine'
    #     )

    def test_get_atom(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_atom(handle=metta_animal_base_handles.human)
        assert result['handle'] == metta_animal_base_handles.human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        result = remote_das.get_atom(handle=metta_animal_base_handles.inheritance_dinosaur_reptile)
        assert result['handle'] == metta_animal_base_handles.inheritance_dinosaur_reptile
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [
            metta_animal_base_handles.Inheritance,
            metta_animal_base_handles.dinosaur,
            metta_animal_base_handles.reptile,
        ]

        with pytest.raises(AtomDoesNotExist):
            remote_das.get_atom(handle='fake')

    def test_get_node(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_node(node_type='Symbol', node_name='"human"')
        assert result['handle'] == metta_animal_base_handles.human
        assert result['name'] == '"human"'
        assert result['named_type'] == 'Symbol'

        with pytest.raises(NodeDoesNotExist):
            remote_das.get_node(node_type='Fake', node_name='fake')

    def test_get_link(self, remote_das: DistributedAtomSpace):
        result = remote_das.get_link(
            link_type='Expression',
            link_targets=[
                metta_animal_base_handles.Inheritance,
                metta_animal_base_handles.earthworm,
                metta_animal_base_handles.animal,
            ],
        )
        assert result['handle'] == metta_animal_base_handles.inheritance_earthworm_animal
        assert result['named_type'] == 'Expression'
        assert result['targets'] == [
            metta_animal_base_handles.Inheritance,
            metta_animal_base_handles.earthworm,
            metta_animal_base_handles.animal,
        ]

        with pytest.raises(LinkDoesNotExist):
            remote_das.get_link(link_type='Fake', link_targets=['fake1', 'fake2'])

    # TODO: Fix this test
    # def test_get_links(self, remote_das: DistributedAtomSpace):
    #     all_inheritance = [
    #         metta_animal_base_handles.inheritance_human_mammal,
    #         metta_animal_base_handles.inheritance_monkey_mammal,
    #         metta_animal_base_handles.inheritance_chimp_mammal,
    #         metta_animal_base_handles.inheritance_mammal_animal,
    #         metta_animal_base_handles.inheritance_reptile_animal,
    #         metta_animal_base_handles.inheritance_snake_reptile,
    #         metta_animal_base_handles.inheritance_dinosaur_reptile,
    #         metta_animal_base_handles.inheritance_triceratops_dinosaur,
    #         metta_animal_base_handles.inheritance_earthworm_animal,
    #         metta_animal_base_handles.inheritance_rhino_mammal,
    #         metta_animal_base_handles.inheritance_vine_plant,
    #         metta_animal_base_handles.inheritance_ent_plant,
    #     ]

    #     links = remote_das.get_links(link_type='Inheritance')
    #     assert len(links) == 12
    #     assert set([link['handle'] for link in links]) == set(all_inheritance)

    #     links = remote_das.get_links(link_type='Inheritance', target_types=['Concept', 'Concept'])
    #     assert len(links) == 12
    #     assert set([link['handle'] for link in links]) == set(all_inheritance)

    #     links = remote_das.get_links(link_type='Inheritance', link_targets=[earthworm, animal])
    #     assert links[0]['handle'] == inheritance_earthworm_animal

    def test_get_incoming_links(self, remote_das: DistributedAtomSpace):
        expected_handles = [
            metta_animal_base_handles.inheritance_vine_plant,
            metta_animal_base_handles.similarity_snake_vine,
            metta_animal_base_handles.similarity_vine_snake,
        ]

        expected_atoms = [remote_das.get_atom(handle) for handle in expected_handles]

        response_atoms = remote_das.get_incoming_links(
            metta_animal_base_handles.vine, handles_only=False
        )
        for atom in response_atoms:
            if len(atom["targets"]) == 3:
                assert atom in expected_atoms

    def test_count_atoms(self, remote_das: DistributedAtomSpace):
        nodes = 21
        links = 43
        response = remote_das.count_atoms()
        assert response[0] == nodes
        assert response[1] == links

    def test_query(self, remote_das: DistributedAtomSpace):
        all_inheritance_mammal = [
            metta_animal_base_handles.inheritance_chimp_mammal,
            metta_animal_base_handles.inheritance_human_mammal,
            metta_animal_base_handles.inheritance_monkey_mammal,
            metta_animal_base_handles.inheritance_rhino_mammal,
        ]

        answer = remote_das.query(
            {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "Inheritance"},
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "node", "type": "Symbol", "name": '"mammal"'},
                ],
            },
            {'no_iterator': True},
        )

        assert len(answer) == 4

        for _, link in answer:
            assert link['handle'] in all_inheritance_mammal
            if link['handle'] == metta_animal_base_handles.inheritance_chimp_mammal:
                assert link['targets'] == [
                    {
                        'handle': metta_animal_base_handles.Inheritance,
                        'type': 'Symbol',
                        'name': "Inheritance",
                    },
                    {
                        'handle': metta_animal_base_handles.chimp,
                        'type': 'Symbol',
                        'name': '"chimp"',
                    },
                    {
                        'handle': metta_animal_base_handles.mammal,
                        'type': 'Symbol',
                        'name': '"mammal"',
                    },
                ]
            elif link['handle'] == metta_animal_base_handles.inheritance_human_mammal:
                assert link['targets'] == [
                    {
                        'handle': metta_animal_base_handles.Inheritance,
                        'type': 'Symbol',
                        'name': "Inheritance",
                    },
                    {
                        'handle': metta_animal_base_handles.human,
                        'type': 'Symbol',
                        'name': '"human"',
                    },
                    {
                        'handle': metta_animal_base_handles.mammal,
                        'type': 'Symbol',
                        'name': '"mammal"',
                    },
                ]
            elif link['handle'] == metta_animal_base_handles.inheritance_monkey_mammal:
                assert link['targets'] == [
                    {
                        'handle': metta_animal_base_handles.Inheritance,
                        'type': 'Symbol',
                        'name': "Inheritance",
                    },
                    {
                        'handle': metta_animal_base_handles.monkey,
                        'type': 'Symbol',
                        'name': '"monkey"',
                    },
                    {
                        'handle': metta_animal_base_handles.mammal,
                        'type': 'Symbol',
                        'name': '"mammal"',
                    },
                ]
            elif link['handle'] == metta_animal_base_handles.inheritance_rhino_mammal:
                assert link['targets'] == [
                    {
                        'handle': metta_animal_base_handles.Inheritance,
                        'type': 'Symbol',
                        'name': "Inheritance",
                    },
                    {
                        'handle': metta_animal_base_handles.rhino,
                        'type': 'Symbol',
                        'name': '"rhino"',
                    },
                    {
                        'handle': metta_animal_base_handles.mammal,
                        'type': 'Symbol',
                        'name': '"mammal"',
                    },
                ]

    def test_get_traversal_cursor(self, remote_das: DistributedAtomSpace):
        cursor = remote_das.get_traversal_cursor(metta_animal_base_handles.human)
        assert cursor.get()['handle'] == metta_animal_base_handles.human
        with pytest.raises(GetTraversalCursorException):
            remote_das.get_traversal_cursor('fake_handle')

    def test_traverse_engine_methods(self, remote_das: DistributedAtomSpace):
        cursor: TraverseEngine = self.traversal(remote_das, metta_animal_base_handles.dinosaur)
        assert cursor.get()['handle'] == metta_animal_base_handles.dinosaur

        def is_expression_link(link):
            return True if link['type'] == 'Expression' else False

        links_iter = cursor.get_links(filter=is_expression_link)

        expected_links = [
            remote_das.get_atom(handle)
            for handle in [
                metta_animal_base_handles.inheritance_dinosaur_reptile,
                metta_animal_base_handles.inheritance_triceratops_dinosaur,
            ]
        ]

        count = len(expected_links)
        for link in links_iter:
            if len(link["targets"]) == 3:
                assert link in expected_links
                count -= 1
        assert count == 0

        def is_literal(atom: dict):
            return True if atom['is_literal'] is True else False

        neighbors_iter = cursor.get_neighbors(cursor_position=1, filter=is_literal)
        assert neighbors_iter.get()['handle'] == metta_animal_base_handles.reptile

        atom = cursor.follow_link(cursor_position=2, filter=is_literal)
        assert atom['handle'] == metta_animal_base_handles.triceratops

        cursor.goto(metta_animal_base_handles.human)
        assert cursor.get()['handle'] == metta_animal_base_handles.human
