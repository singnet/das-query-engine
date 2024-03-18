from dataclasses import dataclass
from typing import Optional


@dataclass
class IndexField:
    collection: str
    key: str = ""
    direction: Optional[str] = 'asc'
    conditionals: Optional[dict] = None


class Index:
    def __init__(self, collection: str, key: str, direction: Optional[str] = 'asc', **kwargs):
        new_kwargs = {'collection': collection, 'key': key, 'direction': direction}

        for key, value in kwargs.items():
            key = 'named_type' if key == 'type' else key
            new_kwargs['conditionals'] = {key: {"$eq": value}}
            break  # only one key-value pair

        self.index = IndexField(**new_kwargs)

    def create(self) -> tuple:
        index_conditionals = {"name": f"{self.index.key}_index_{self.index.direction}"}
        if self.index.conditionals is not None:
            index_conditionals["partialFilterExpression"] = self.index.conditionals
        index_list = [(self.index.key, 1 if self.index.direction == "asc" else -1)]

        return self.index.collection, (index_list, index_conditionals)
