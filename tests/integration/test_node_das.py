import pytest

from hyperon_das.das import DistributedAtomSpace


class TestNodeDAS:

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
         }, 14)
    ])
    def test_node_das_query_og(self, query, expected):
        das = DistributedAtomSpace(query_engine="grpc", host="localhost", port=35700)
        # das2 = DistributedAtomSpace(
        #     query_engine='local',
        #     atomdb='redis_mongo',
        #     mongo_port=27017,
        #     mongo_username='root',
        #     mongo_password='root',
        #     redis_port=6379,
        #     redis_cluster=False,
        #     redis_ssl=False,
        # )
        # print(das2.query(query))
        # qqq = das2.query(query)
        # print(qqq)
        # cc = 0
        # for qq in qqq:
        #     print(qq)
        #     cc += 1
        # print(cc)

        count = 0
        for q in das.query(query):
            assert isinstance(q, list)
            assert len(q) > 0
            count += 1
        assert count == expected

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
            # for nn in n:
            #     d.get_atom(qq.assignment.mapping[nn])

    @pytest.mark.parametrize("query", [
        {'atom_type': 'link',
          'type': 'Expression',
          'targets': [{'atom_type': 'node',
                       'type': 'Symbol',
                       'name': 'public.feature.name'},
                      {'atom_type': 'link',
                       'type': 'Expression',
                       'targets': [{'atom_type': 'node',
                                    'type': 'Symbol',
                                    'name': 'public.feature'},
                                   {'atom_type': 'variable', 'name': 'feature_pk'}]},
                      {'atom_type': 'node', 'type': 'Symbol', 'name': '"Abd-B"'}]},
        {'atom_type': 'link',
         'type': 'Expression',
         'targets': [{'atom_type': 'node',
                      'type': 'Symbol',
                      'name': 'public.grp.uniquename'},
                     {'atom_type': 'variable', 'name': 'v1'},
                     {'atom_type': 'variable', 'name': 'v2'}]},
        {'atom_type': 'link',
         'type': 'Expression',
         'targets': [{'atom_type': 'node',
                      'type': 'Symbol',
                      'name': 'public.feature.name'},
                     {'atom_type': 'variable', 'name': 'feature_pk'},
                     {'atom_type': 'node', 'type': 'Symbol', 'name': '"Abd-B"'}]}
    ])
    def test_node_das_query_test(self, query):
        das = DistributedAtomSpace(query_engine="grpc", host="localhost", port=35700)
        count = 0
        try:
            for q in das.query(query):
                print(q)
                assert isinstance(q, list)
                assert len(q) > 0
                count += 1
        except:
            pass
        # assert count == expected
