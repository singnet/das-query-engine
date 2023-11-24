from dataclasses import dataclass
from typing import Any, Dict, FrozenSet, Optional, Set, Union

from dotenv import dotenv_values

from hyperon_das.constants import QueryOutputFormat
from hyperon_das.exceptions import InvalidAssignment

config = dotenv_values('.env')


@dataclass
class QueryParameters:
    toplevel_only: Optional[bool] = False
    return_type: Optional[QueryOutputFormat] = QueryOutputFormat.HANDLE.value

    @classmethod
    def values(cls) -> list:
        return list(cls.__dataclass_fields__.keys())


class Assignment:
    def __init__(self):
        self.labels: Union[Set[str], FrozenSet] = set()
        self.values: Union[Set[str], FrozenSet] = set()
        self.mapping: Dict[str, str] = {}
        self.hashcode: int = 0

    def __hash__(self) -> int:
        assert self.hashcode
        return self.hashcode

    def __eq__(self, other) -> bool:
        assert self.hashcode and other.hashcode
        return self.hashcode == other.hashcode

    def __lt__(self, other) -> bool:
        assert self.hashcode and other.hashcode
        return self.hashcode < other.hashcode

    def __repr__(self) -> str:
        labels = sorted(self.labels)
        return str(
            [
                tuple([label, self.mapping[label]])
                for label in sorted(self.labels)
            ]
        )

    def __str__(self) -> str:
        return self.__repr__()

    def frozen(self):
        return self.hashcode != 0

    def freeze(self) -> bool:
        if self.frozen():
            return False
        else:
            self.labels = frozenset(self.labels)
            self.values = frozenset(self.values)
            self.hashcode = hash(frozenset(self.mapping.items()))
            return True

    def assign(
        self,
        label: str,
        value: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> bool:
        if label is None or value is None or self.frozen():
            raise InvalidAssignment(
                message=f"Invalid assignment",
                details=f"label = {label} value = {value} hashcode = {self.hashcode}",
            )
        if label in self.labels:
            return self.mapping[label] == value
        else:
            if (
                parameters
                and parameters['no_overload']
                and value in self.values
            ):
                return False
            self.labels.add(label)
            self.values.add(value)
            self.mapping[label] = value
            return True

    def merge(self, other: "Assignment") -> bool:
        assert not self.frozen()
        if other:
            for label, value in other.mapping.items():
                if not self.assign(label, value):
                    return False
        return True


@dataclass
class QueryAnswer:
    grounded_atom: Optional[Dict] = None
    assignment: Optional[Assignment] = None
