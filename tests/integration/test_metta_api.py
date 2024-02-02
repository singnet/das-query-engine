from hyperon_das import DistributedAtomSpace


class TestMettaAPI:
    def test_match_nested_expression(self):
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

        query = {
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

        answer = [query_answer for query_answer in das.query(query)]
        assert len(answer) == 1
        handle = answer[0].assignment.mapping["$v1"]
        assert handle == '963d66edfb77236054125e3eb866c8b5'
        symbol = das.get_atom(handle)
        assert symbol["type"] == "Symbol"
        assert symbol["name"] == "Test"
