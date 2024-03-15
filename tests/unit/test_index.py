from unittest import mock

from hyperon_das.index import Index, QueryOperators


class TestIndex:
    def test_index_creation(self):
        with mock.patch('hyperon_das.index.QueryOperators.EXISTS', return_value={'options': True}):
            index = Index('collection', 'key', 'asc', condition=True)
        assert index.index.collection == 'collection'
        assert index.index.key == 'key'
        assert index.index.direction == 'asc'
        assert index.index.conditionals == {'options': True}

    def test_index_create_method(self):
        with mock.patch('hyperon_das.index.QueryOperators.EQ', return_value={'options': 'Type'}):
            index = Index('collection', 'key', 'asc', condition='Type')
        expected_index_conditionals = {
            "name": "key_index_asc",
            "partialFilterExpression": {'options': 'Type'},
        }
        expected_index_list = [('key', 1)]

        collection, (index_list, index_conditionals) = index.create()

        assert collection == 'collection'
        assert index_list == expected_index_list
        assert index_conditionals == expected_index_conditionals


class TestQueryOperators:
    def test_EQ(self):
        query_operators = QueryOperators()
        result = query_operators.EQ(type='Type')
        expected_result = {'named_type': {"$eq": 'Type'}}
        assert result == expected_result

    def test_EXISTS(self):
        query_operators = QueryOperators()
        result = query_operators.EXISTS(type=True)
        expected_result = {'named_type': {"$exists": True}}
        assert result == expected_result
