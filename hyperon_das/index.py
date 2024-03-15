from dataclasses import dataclass
from typing import Optional


class QueryOperators:
    """This class implements some MongoDB query operators. It is used to construct the query conditions for the indexes."""

    # comparison operators
    def EQ(self, **kwargs) -> dict:
        for key, value in kwargs.items():
            key = 'named_type' if key == 'type' else key
            break  # only one key-value pair
        return {key: {"$eq": value}}

    # element operators
    def EXISTS(self, **kwargs) -> dict:
        for key, value in kwargs.items():
            key = 'named_type' if key == 'type' else key
            break  # only one key-value pair
        return {key: {"$exists": value}}

    # logical operators - Can be used. Don't delete
    # def AND(self, **kwargs) -> dict:
    #     expressions = self._build_expressions(**kwargs)
    #     return {'$and': expressions}

    # def OR(self, **kwargs):
    #     expressions = self._build_expressions(**kwargs)
    #     return {'$or': expressions}

    # def _build_expressions(self, **kwargs) -> list:
    #     expressions = []
    #     for key, value in kwargs.items():
    #         key = 'named_type' if key == 'type' else key
    #         if isinstance(value, bool):
    #             expressions.append({key: {"$exists": value}})
    #         else:
    #             expressions.append({key: {"$eq": value}})
    #     return expressions


operator = QueryOperators()


@dataclass
class IndexField:
    collection: str
    key: str = ""
    direction: Optional[str] = 'asc'
    conditionals: Optional[dict] = None


class Index:
    def __init__(self, collention: str, key: str, direction: Optional[str] = 'asc', **kwargs):
        new_kwargs = {'collection': collention, 'key': key, 'direction': direction}

        for key, value in kwargs.items():
            if isinstance(value, bool):
                new_kwargs['conditionals'] = operator.EXISTS(**kwargs)
            else:
                new_kwargs['conditionals'] = operator.EQ(**kwargs)
            break  # only one key-value pair

        self.index = IndexField(**new_kwargs)

    def create(self) -> tuple:
        index_conditionals = {"name": f"{self.index.key}_index_{self.index.direction}"}
        if self.index.conditionals is not None:
            index_conditionals["partialFilterExpression"] = self.index.conditionals
        index_list = [(self.index.key, 1 if self.index.direction == "asc" else -1)]

        return self.index.collection, (index_list, index_conditionals)
