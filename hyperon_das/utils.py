import pickle
from dataclasses import dataclass
from importlib import import_module
from typing import Any, Dict, FrozenSet, List, Optional, Set, Union

from hyperon_das.exceptions import InvalidAssignment


class Assignment:
    @staticmethod
    def compose(components: List["Assignment"]) -> Optional["Assignment"]:
        answer = Assignment()
        for component in components:
            if not answer.merge(component):
                return None
        answer.freeze()
        return answer

    def __init__(self, other: Optional["Assignment"] = None):
        self.labels: Union[Set[str], FrozenSet] = set()
        self.values: Union[Set[str], FrozenSet] = set()
        self.hashcode: int = 0
        if other:
            self.mapping: Dict[str, str] = dict(other.mapping)
        else:
            self.mapping: Dict[str, str] = {}

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
        return str([tuple([label, self.mapping[label]]) for label in sorted(self.labels)])

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
                message="Invalid assignment",
                details=f"label = {label} value = {value} hashcode = {self.hashcode}",
            )
        if label in self.labels:
            return self.mapping[label] == value
        else:
            if parameters and parameters['no_overload'] and value in self.values:
                return False
            self.labels.add(label)
            self.values.add(value)
            self.mapping[label] = value
            return True

    def merge(self, other: "Assignment", in_place: bool = True) -> Optional[bool]:
        if in_place:
            assert not self.frozen()
            if other:
                for label, value in other.mapping.items():
                    if not self.assign(label, value):
                        return False
            return True
        else:
            new_assignment = Assignment(self)
            if new_assignment.merge(other):
                new_assignment.freeze()
                return new_assignment
            else:
                return None


@dataclass
class QueryAnswer:
    subgraph: Optional[Dict] = None
    assignment: Optional[Assignment] = None


def get_package_version(package_name: str) -> str:
    package_module = import_module(package_name)
    return getattr(package_module, '__version__', None)


def serialize(payload: Any) -> bytes:
    return pickle.dumps(payload)


def deserialize(payload: bytes) -> Any:
    return pickle.loads(payload)
