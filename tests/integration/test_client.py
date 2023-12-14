import json

import pytest
from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

from hyperon_das.client import FunctionsClient
from hyperon_das.constants import QueryOutputFormat


# AWS function is offline
class AWSClientIntegration:
    @pytest.fixture()
    def server(self):
        return FunctionsClient(url='http://44.198.65.35/prod/query-engine')

    def test_get_node(self, server: FunctionsClient):
        human_handle = ExpressionHasher.terminal_hash('Concept', 'human')
        ret = server.get_node(node_type="Concept", node_name="human")
        assert ret == human_handle

    def test_get_link(self, server: FunctionsClient):
        human_handle = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey_handle = ExpressionHasher.terminal_hash('Concept', 'monkey')
        link_handle = ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Similarity'),
            [human_handle, monkey_handle],
        )
        ret = server.get_link(
            link_type='Similarity',
            targets=[human_handle, monkey_handle],
            output_format=QueryOutputFormat.HANDLE,
        )
        assert ret == link_handle
        ret = server.get_link(
            link_type='Similarity',
            targets=[human_handle, monkey_handle],
            output_format=QueryOutputFormat.ATOM_INFO,
        )
        assert ret == {
            'handle': link_handle,
            'type': 'Similarity',
            'template': ['Similarity', 'Concept', 'Concept'],
            'targets': [human_handle, monkey_handle],
        }
        ret = server.get_link(
            link_type='Similarity',
            targets=[human_handle, monkey_handle],
            output_format=QueryOutputFormat.JSON,
        )
        assert json.loads(ret) == {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        }

    def test_get_links(self, server: FunctionsClient):
        human_handle = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey_handle = ExpressionHasher.terminal_hash('Concept', 'monkey')
        link_handle = ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Similarity'),
            [human_handle, monkey_handle],
        )
        ret = server.get_links(
            link_type='Similarity',
            target_types=['Concept', 'Concept'],
            output_format=QueryOutputFormat.HANDLE,
        )
        assert len(ret) == 14

    def test_get_atom(self, server: FunctionsClient):
        human_handle = ExpressionHasher.terminal_hash('Concept', 'human')
        monkey_handle = ExpressionHasher.terminal_hash('Concept', 'monkey')
        link_handle = ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Similarity'),
            [human_handle, monkey_handle],
        )
        ret = server.get_atom(handle=monkey_handle, output_format=QueryOutputFormat.HANDLE)
        assert ret == monkey_handle
        ret = server.get_atom(handle=monkey_handle, output_format=QueryOutputFormat.ATOM_INFO)
        assert ret == {
            'handle': monkey_handle,
            'type': 'Concept',
            'name': 'monkey',
        }
        ret = server.get_atom(handle=monkey_handle, output_format=QueryOutputFormat.JSON)
        assert json.loads(ret) == {'type': 'Concept', 'name': 'monkey'}
        ret = server.get_atom(handle=link_handle, output_format=QueryOutputFormat.HANDLE)
        assert ret == link_handle
        ret = server.get_atom(handle=link_handle, output_format=QueryOutputFormat.ATOM_INFO)
        assert ret == {
            'handle': link_handle,
            'type': 'Similarity',
            'template': ['Similarity', 'Concept', 'Concept'],
            'targets': [human_handle, monkey_handle],
        }
        ret = server.get_atom(handle=link_handle, output_format=QueryOutputFormat.JSON)
        assert json.loads(ret) == {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        }

    def test_count_atoms(self, server: FunctionsClient):
        ret = server.count_atoms()
        assert ret[0] == 14
        assert ret[1] == 26

    def test_query(self, server: FunctionsClient):
        query = {
            "atom_type": "link",
            "type": "Inheritance",
            "targets": [
                {"atom_type": "variable", "name": "v1"},
                {"atom_type": "node", "type": "Concept", "name": "mammal"},
            ],
        }
        params = {
            "toplevel_only": False,
            "return_type": QueryOutputFormat.ATOM_INFO,
        }
        ret = server.query(query, params)
        assert len(ret) == 4


class TestVultrClientIntegration:
    @pytest.fixture()
    def server(self):
        return FunctionsClient(url='http://104.238.183.115:8081/function/query-engine')

    @pytest.fixture()
    def node_FBgg0001581(self):
        return ExpressionHasher.terminal_hash('Verbatim', 'FBgg0001581')

    @pytest.fixture()
    def node_FBgg0001782(self):
        return ExpressionHasher.terminal_hash('Verbatim', 'FBgg0001782')

    @pytest.fixture()
    def node_FBlc0004576(self):
        return ExpressionHasher.terminal_hash('Verbatim', 'FBlc0004576')

    @pytest.fixture()
    def node_FBgn0262656(self):
        return ExpressionHasher.terminal_hash('Verbatim', 'FBgn0262656')

    @pytest.fixture()
    def node_schema_fbid(self):
        return ExpressionHasher.terminal_hash('Schema', 'Schema:fb_synonym_primary_FBid')

    @pytest.fixture()
    def node_schema_gene_id(self):
        return ExpressionHasher.terminal_hash('Schema', 'Schema:scRNA-Seq_gene_expression_Gene_ID')

    @pytest.fixture()
    def link_inheritance_verbatim_verbatim(self, node_FBgg0001581, node_FBgg0001782):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Inheritance'),
            [node_FBgg0001581, node_FBgg0001782],
        )

    @pytest.fixture()
    def link_execution_schema_verbatim_verbatim(
        self, node_schema_gene_id, node_FBlc0004576, node_FBgn0262656
    ):
        return ExpressionHasher.expression_hash(
            ExpressionHasher.named_type_hash('Execution'),
            [node_schema_gene_id, node_FBlc0004576, node_FBgn0262656],
        )

    def test_get_atom(
        self,
        server: FunctionsClient,
        node_FBgg0001581: str,
        node_FBgg0001782: str,
        link_inheritance_verbatim_verbatim: str,
    ):
        result = server.get_atom(handle=node_FBgg0001581)
        assert result['handle'] == node_FBgg0001581
        assert result['name'] == 'FBgg0001581'
        assert result['named_type'] == 'Verbatim'

        result = server.get_atom(handle=node_FBgg0001782)
        assert result['handle'] == node_FBgg0001782
        assert result['name'] == 'FBgg0001782'
        assert result['named_type'] == 'Verbatim'

        result = server.get_atom(handle=link_inheritance_verbatim_verbatim)
        assert result['handle'] == link_inheritance_verbatim_verbatim
        assert result['named_type'] == 'Inheritance'
        assert result['targets'] == [node_FBgg0001581, node_FBgg0001782]

    def test_get_node(self, server: FunctionsClient, node_FBgg0001581: str, node_FBgg0001782: str):
        result = server.get_node(node_type='Verbatim', node_name='FBgg0001581')
        assert result['handle'] == node_FBgg0001581
        assert result['name'] == 'FBgg0001581'
        assert result['named_type'] == 'Verbatim'

        result = server.get_node(node_type='Verbatim', node_name='FBgg0001782')
        assert result['handle'] == node_FBgg0001782
        assert result['name'] == 'FBgg0001782'
        assert result['named_type'] == 'Verbatim'

    def test_get_link(
        self,
        server: FunctionsClient,
        node_FBgg0001581: str,
        node_FBgg0001782: str,
        link_inheritance_verbatim_verbatim: str,
    ):
        result = server.get_link(
            link_type='Inheritance', link_targets=[node_FBgg0001581, node_FBgg0001782]
        )
        assert result['handle'] == link_inheritance_verbatim_verbatim
        assert result['named_type'] == 'Inheritance'
        assert result['targets'] == [node_FBgg0001581, node_FBgg0001782]

    def test_get_links(self, server: FunctionsClient):
        ret = server.get_links(link_type='Inheritance', target_types=['Verbatim', 'Verbatim'])
        # assert ret == []

    def test_count_atoms(self, server: FunctionsClient):
        ret = server.count_atoms()
        assert ret is not None

    def test_query(
        self,
        server: FunctionsClient,
        link_execution_schema_verbatim_verbatim,
        node_schema_gene_id,
        node_FBlc0004576,
        node_FBgn0262656,
    ):
        answer = server.query(
            {
                "atom_type": "link",
                "type": "Execution",
                "targets": [
                    {
                        "atom_type": "node",
                        "type": "Schema",
                        "name": "Schema:fb_synonym_primary_FBid",
                    },
                    {"atom_type": "node", "type": "Verbatim", "name": "Myc"},
                    {"atom_type": "variable", "name": "v1"},
                ],
            },
            {"toplevel_only": False},
        )

        assert answer is not None

        answer = server.query(
            {
                "atom_type": "link",
                "type": "Execution",
                "targets": [
                    {
                        "atom_type": "node",
                        "type": "Schema",
                        "name": "Schema:scRNA-Seq_gene_expression_Gene_ID",
                    },
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "node", "type": "Verbatim", "name": 'FBgn0262656'},
                ],
            },
            {"toplevel_only": False},
        )

        for link in answer:
            if link['handle'] == link_execution_schema_verbatim_verbatim:
                break

        handles = [target['handle'] for target in link['targets']]

        assert handles == [node_schema_gene_id, node_FBlc0004576, node_FBgn0262656]
