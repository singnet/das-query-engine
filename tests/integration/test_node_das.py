import pytest

from hyperon_das.query_engines.das_node_query_engine import DASNodeQueryEngine

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
        qe = DASNodeQueryEngine("localhost", 35700 )
        count = 0
        for q in qe.query(query):
            print("::END", q)
            assert q
            count += 1
        assert count == expected
