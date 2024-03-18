from hyperon_das.index import Index


class TestIndex:
    def test_index_creation(self):
        index = Index('collection', 'key', 'asc', type='SnetType')
        assert index.index.collection == 'collection'
        assert index.index.key == 'key'
        assert index.index.direction == 'asc'
        assert index.index.conditionals == {'named_type': {'$eq': 'SnetType'}}

    def test_index_create_method(self):
        index = Index('collection', 'key', 'asc', type='SnetType')
        expected_index_conditionals = {
            "name": "key_index_asc",
            "partialFilterExpression": {'named_type': {'$eq': 'SnetType'}},
        }
        expected_index_list = [('key', 1)]

        collection, (index_list, index_conditionals) = index.create()

        assert collection == 'collection'
        assert index_list == expected_index_list
        assert index_conditionals == expected_index_conditionals
