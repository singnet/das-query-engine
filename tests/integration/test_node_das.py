from dns.resolver import query

from hyperon_das.query_engines.das_node_query_engine import DASNodeQueryEngine

class TestNodeDAS:

    def test_node_das(self):
        qe = DASNodeQueryEngine("localhost", 35700 )
        # query = ["LINK_TEMPLATE", "Expression", "3",
        #     "NODE", "Symbol", "Similarity",
        #     "VARIABLE", "v1",
        #     "VARIABLE", "v2"]
        query = [
        "LINK_TEMPLATE", "Expression", "3",
            "NODE", "Symbol", "Similarity",
            "NODE", "Symbol", "\"human\"",
            "VARIABLE", "v1"
        ]
        qe.query(query)
        for q in qe.query(query):
            print(q)
            assert q
