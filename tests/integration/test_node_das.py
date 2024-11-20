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
        # pytest.skip("skip")
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
        count = 0
        for q in das.query(query):
            assert isinstance(q, list)
            assert len(q) > 0
            count += 1
        assert count == expected
