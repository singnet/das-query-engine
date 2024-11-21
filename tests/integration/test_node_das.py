import time

import pytest

from hyperon_das.das import DistributedAtomSpace


class TestNodeDAS:

    @pytest.fixture
    def remote_das(self):
        yield DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=27017,
            mongo_username='root',
            mongo_password='root',
            redis_port=6379,
            redis_cluster=False,
            redis_ssl=False,
        )



    @pytest.mark.parametrize("query,expected", [
        ([
             "LINK_TEMPLATE", "Expression", "3",
             "NODE", "Symbol", "Similarity",
             "VARIABLE", "v1",
             "VARIABLE", "v2"
         ], 14),
        ([
             "LINK_TEMPLATE", "Expression", "3",
             "NODE", "Symbol", "Similarity",
             "NODE", "Symbol", "\"human\"",
             "VARIABLE", "v1"
         ], 3),
        ([
             "AND", "2",
             "LINK_TEMPLATE", "Expression", "3",
             "NODE", "Symbol", "Similarity",
             "VARIABLE", "v1",
             "NODE", "Symbol", "\"human\"",
             "LINK_TEMPLATE", "Expression", "3",
             "NODE", "Symbol", "Inheritance",
             "VARIABLE", "v1",
             "NODE", "Symbol", "\"plant\"",
         ], 1),
        ([
             "AND", "2",
             "LINK_TEMPLATE", "Expression", "3",
             "NODE", "Symbol", "Similarity",
             "VARIABLE", "v1",
             "VARIABLE", "v2",
             "LINK_TEMPLATE", "Expression", "3",
             "NODE", "Symbol", "Similarity",
             "VARIABLE", "v2",
             "VARIABLE", "v3"
         ], 26)
    ])
    def test_node_das(self, query, expected):
        das = DistributedAtomSpace(query_engine="grpc", host="localhost", port=35700)
        count = 0
        for q in das.query(query, {"tokenize": False}):
            assert isinstance(q, list)
            assert len(q) > 0
            count += 1
        assert count == expected

    @pytest.mark.parametrize("query,expected", [
        ({
             "atom_type": "link",
             "type": "Expression",
             "targets": [
                 {"atom_type": "node", "type": "Symbol", "name": "Similarity"},
                 {"atom_type": "variable", "name": "v1"},
                 {"atom_type": "variable", "name": "v2"},
             ],
         }, 14),
        ({
             'atom_type': 'link', 'type': 'Expression',
             'targets': [
                 {'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'},
                 {'atom_type': 'node', 'type': 'Symbol', 'name': '"human"'},
                 {'atom_type': 'variable', 'name': 'v1'}]
         }, 3),
        ([
             {
                 'atom_type': 'link', 'type': 'Expression',
                 'targets': [{'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'},
                             {'atom_type': 'variable', 'name': 'v1'},
                             {'atom_type': 'node', 'type': 'Symbol', 'name': '"human"'}]},
             {
                 'atom_type': 'link', 'type': 'Expression',
                 'targets': [{'atom_type': 'node', 'type': 'Symbol', 'name': 'Inheritance'},
                             {'atom_type': 'variable', 'name': 'v1'},
                             {'atom_type': 'node', 'type': 'Symbol', 'name': '"plant"'}]}
         ], 1),
        ([
             {
                 'atom_type': 'link', 'type': 'Expression',
                 'targets': [{'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'},
                             {'atom_type': 'variable', 'name': 'v1'}, {'atom_type': 'variable', 'name': 'v2'}]},
             {
                 'atom_type': 'link', 'type': 'Expression',
                 'targets': [{'atom_type': 'node', 'type': 'Symbol', 'name': 'Similarity'},
                             {'atom_type': 'variable', 'name': 'v2'}, {'atom_type': 'variable', 'name': 'v3'}]}
         ], 26)
    ])
    def test_node_das_query_og(self, query, expected, remote_das: DistributedAtomSpace):
        das = DistributedAtomSpace(query_engine="grpc", host="localhost", port=35700, timeout=5)
        redis_mongo_return = list(remote_das.query(query))
        rm_list = [[link.handle for link in qa.subgraph] if isinstance(qa.subgraph, list) else [qa.subgraph.handle] for
                   qa in redis_mongo_return]
        grpc_return = list(das.query(query, {"retry": 3}))
        assert sorted(rm_list) == sorted(grpc_return)

    #
    @pytest.mark.parametrize(
        'nodes,link_type',
        [
            # ('v1,v2', "Expression"),
            # ('v1,v2', "Similarity"),
            ('v1,v2,v3', "Expression"),
            # ('v1,v2,v3', "Similarity"),
        ],
    )
    def test_query_links_nodes_var(self, nodes, link_type):
        das = DistributedAtomSpace(query_engine="grpc", host="localhost", port=35700)
        nodes = nodes.split(',')
        queries = []
        for i, node in enumerate(nodes):
            for j in range(i + 1, len(nodes)):
                query = {
                    'atom_type': 'link',
                    'type': link_type,
                    'targets': [
                        {'atom_type': 'variable', 'name': node},
                        {'atom_type': 'variable', 'name': nodes[j]},
                    ],
                }
                queries.append(query)

        query_answers = das.query(queries)

        for qq in query_answers:
            print(qq)


