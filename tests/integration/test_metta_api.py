from hyperon_das import DistributedAtomSpace
test_client.py:from hyperon_das_atomdb.utils.expression_hasher import ExpressionHasher

def _check_node(das: DistibutedAtomSpace, handle: str, node_type: str, node_name: str):
    assert handle == ExpressionHasher.terminal_hash(node_type, node_name)
    symbol = das.get_atom(handle)
    assert symbol["type"] == node_type
    assert symbol["name"] == node_name

class TestMettaAPI:

    def test_match_nested_expression(self):

        def _test_case_1():
            das = DistributedAtomSpace()

            das.add_link(
                {
                    "type": "Expression",
                    "targets": [
                        {"type": "Symbol", "name": "Test"},
                        {
                            "type": "Expression",
                            "targets": [
                                {"type": "Symbol", "name": "Test"},
                                {"type": "Symbol", "name": "2"},
                            ],
                        },
                    ],
                }
            )

            query_1 = {
                'atom_type': 'link',
                'type': 'Expression',
                'targets': [
                    {'atom_type': 'variable', 'name': '$v1'},
                    {
                        'atom_type': 'link',
                        'type': 'Expression',
                        'targets': [
                            {'atom_type': 'node', 'type': 'Symbol', 'name': 'Test'},
                            {'atom_type': 'node', 'type': 'Symbol', 'name': '2'},
                        ],
                    },
                ],
            }

            query_2 = {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "variable", "name": "v1"},
                    {
                        "atom_type": "link",
                        "type": "Expression",
                        "targets": [
                            {"atom_type": "variable", "name": "v2"},
                            {"atom_type": "node", "type": "Symbol", "name": "2"},
                        ],
                    },
                ],
            }

            query_3 = {
                'atom_type': 'link',
                'type': 'Expression',
                'targets': [
                    {'atom_type': 'node', 'type': 'Symbol', 'name': 'Test'},
                    {'atom_type': 'variable', 'name': '$v2'},
                ],
            }

            query_4 = [
                {
                    'atom_type': 'link',
                    'type': 'Expression',
                    'targets': [
                        {'atom_type': 'node', 'type': 'Symbol', 'name': 'Best'},
                        {'atom_type': 'variable', 'name': '$x'},
                    ],
                },
                {
                    'atom_type': 'link',
                    'type': 'Expression',
                    'targets': [
                        {'atom_type': 'variable', 'name': '$v'},
                        {'atom_type': 'variable', 'name': '$x'},
                    ],
                },
            ]

            answer = [query_answer for query_answer in das.query(query_1)]
            assert len(answer) == 1
            handle = answer[0].assignment.mapping["$v1"]
            assert handle == '963d66edfb77236054125e3eb866c8b5'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Symbol"
            assert symbol["name"] == "Test"

            answer = [query_answer for query_answer in das.query(query_2)]
            assert len(answer) == 1
            handle = answer[0].assignment.mapping["v1"]
            assert handle == '963d66edfb77236054125e3eb866c8b5'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Symbol"
            assert symbol["name"] == "Test"
            handle = answer[0].assignment.mapping["v2"]
            assert handle == '963d66edfb77236054125e3eb866c8b5'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Symbol"
            assert symbol["name"] == "Test"

            answer = [query_answer for query_answer in das.query(query_3)]
            assert len(answer) == 2
            handle = answer[0].assignment.mapping["$v2"]
            assert handle == '9f27a331633c8bc3c49435ffabb9110e'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Symbol"
            assert symbol["name"] == "2"
            handle = answer[1].assignment.mapping["$v2"]
            assert handle == '233d9a6da7d49d4164d863569e9ab7b6'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Expression"
            symbol1 = das.get_atom(symbol["targets"][0])
            assert symbol1["type"] == "Symbol"
            assert symbol1["name"] == "Test"
            symbol2 = das.get_atom(symbol["targets"][1])
            assert symbol2["type"] == "Symbol"
            assert symbol2["name"] == "2"

            das.add_link(
                {
                    "type": "Expression",
                    "targets": [
                        {"type": "Symbol", "name": "Best"},
                        {
                            "type": "Expression",
                            "targets": [
                                {"type": "Symbol", "name": "Test"},
                                {"type": "Symbol", "name": "2"},
                            ],
                        },
                    ],
                }
            )

            answer = [query_answer for query_answer in das.query(query_4)]

            assert len(answer) == 2
            handle = answer[0].assignment.mapping["$v"]
            assert handle == '963d66edfb77236054125e3eb866c8b5'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Symbol"
            assert symbol["name"] == "Test"
            handle = answer[0].assignment.mapping["$x"]
            assert handle == '233d9a6da7d49d4164d863569e9ab7b6'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Expression"
            symbol1 = das.get_atom(symbol["targets"][0])
            assert symbol1["type"] == "Symbol"
            assert symbol1["name"] == "Test"
            symbol2 = das.get_atom(symbol["targets"][1])
            assert symbol2["type"] == "Symbol"
            assert symbol2["name"] == "2"

            handle = answer[1].assignment.mapping["$v"]
            assert handle == 'a709a08a70b1bec528d3573aa5b93f16'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Symbol"
            assert symbol["name"] == "Best"
            handle = answer[1].assignment.mapping["$x"]
            assert handle == '233d9a6da7d49d4164d863569e9ab7b6'
            symbol = das.get_atom(handle)
            assert symbol["type"] == "Expression"
            symbol1 = das.get_atom(symbol["targets"][0])
            assert symbol1["type"] == "Symbol"
            assert symbol1["name"] == "Test"
            symbol2 = das.get_atom(symbol["targets"][1])
            assert symbol2["type"] == "Symbol"
            assert symbol2["name"] == "2"

        def _test_case_2():
            das = DistributedAtomSpace()

            das.add_link(
                {
                    "type": "Expression",
                    "targets": [
                        {"type": "Symbol", "name": "outter_expression"}
                        {
                            "type": "Expression",
                            "targets": [
                                {"type": "Symbol", "name": "inner_expression"}
                                {"type": "Symbol", "name": "symbol1"}
                            ]
                        }
                        {
                            "type": "Expression",
                            "targets": [
                                {"type": "Symbol", "name": "inner_expression"}
                                {"type": "Symbol", "name": "symbol2"}
                            ]
                        }
                    ]
                }
            )

            query = {
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "outter_expression"},
                    {
                        "atom_type": "link",
                        "type": "Expression",
                        "targets": [
                            {"atom_type": "node", "type": "Symbol", "name": "inner_expression"},
                            {"atom_type": "variable", "name": "$v1"}
                        ]
                    },
                    {
                        "atom_type": "link",
                        "type": "Expression",
                        "targets": [
                            {"atom_type": "node", "type": "Symbol", "name": "inner_expression"},
                            {"atom_type": "node", "type": "Symbol", "name": "symbol2"},
                        ]
                    }
                ]
            }

            answer = [query_answer for query_answer in das.query(query)]
            assert len(answer) == 1

            handle = answer[0].assignment.mapping["$v1"]
            AQUI: acabei de escrever esse teste. Agora é rodar
            _check_node(das, handle, "Symbol", "symbol1")

        _test_case_1()
        _test_case_2()
