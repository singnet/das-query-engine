from dataclasses import dataclass
from typing import Any, Optional, Tuple


class RedisMongoDB:
    def create_mongodb_index(self, collection, indexes):
        index_list = indexes[0]
        index_options = indexes[1]
        collection.create_index(index_list, **index_options)

    def get_indexes(self, collection_name):
        collection = self.db[collection_name]
        return collection.list_indexes()


@dataclass
class IndexField:
    collection: str
    name: str = ""
    options: Optional[dict] = None
    direction: Optional[str] = 'asc'

    # def __post_init__(self):
    #     if self.direction not in ["asc", "desc"]:
    #         raise ValueError("direction must be either 'asc' or 'desc'")
    #     if self.expression is not None:
    #         if not isinstance(self.expression, tuple):
    #             raise ValueError("expression must be a tuple")
    #         if len(self.expression) != 3:
    #             raise ValueError("expression must be a tuple of length 3")
    #         if self.expression[1] not in [">", ">=", "<", "<=", "==", "!=", "regex"]:
    #             raise ValueError(
    #                 "operator must be one of: '>', '>=', '<', '<=', '==', '!=', 'regex'"
    #             )


class LogicalOperator:
    def DEFAULT(self, **kwargs) -> dict:
        name, expressions = self._build_expressions(**kwargs)
        return {'name': name, 'options': expressions[0]}

    def AND(self, **kwargs) -> dict:
        name, expressions = self._build_expressions(**kwargs)
        return {'name': name, 'options': {'$and': expressions}}

    def OR(self, **kwargs):
        name, expressions = self._build_expressions(**kwargs)
        return {'name': name, 'options': {'$or': expressions}}

    def _build_expressions(self, **kwargs) -> list:
        keys = []
        expressions = []
        for key, value in kwargs.items():
            key = 'named_type' if key == 'type' else key
            if isinstance(value, bool):
                expressions.append({key: {"$exists": value}})
            else:
                expressions.append({key: {"$eq": value}})
            keys.append(key)
        return keys[0], expressions


operator = LogicalOperator()


class Index:
    def __init__(self, **kwargs):
        self.index = IndexField(**kwargs)

    def create_link_index(self, indexes):
        """
        query = {
            "atom_type": "link",
            "type": "Expression",
            "targets": [
                {"atom_type": "node", "type": "Symbol", "name": "test-1"},
                {"atom_type": "node", "type": "Symbol", "name": "test-2"},
                {"atom_type": "node", "type": "Symbol", "name": "test-3"}
            ],
            "score": True
        }

        return collection.create_index(
            [('named_type', 1)],
            **{
                'partialFilterExpression': {
                    '$and': [
                        {'score': {'$exists': True}},
                        {'named_type': {'$eq': 'Similarity'}},
                    ]
                }
            },
        )
        """
        pass

    def create(self) -> tuple:
        index_options = {}
        if self.index.options is not None:
            index_options = {"partialFilterExpression": self.index.options}

        index_list = [(self.index.name, 1 if self.index.direction == "asc" else -1)]

        return self.index.collection, (index_list, index_options)

    def _operator_map(self, operator):
        return {
            ">": "$gt",
            ">=": "$gte",
            "<": "$lt",
            "<=": "$lte",
            "==": "$eq",
            "!=": "$ne",
            "regex": "$regex",
        }[operator]
