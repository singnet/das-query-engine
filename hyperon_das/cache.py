from abc import ABC, abstractmethod
from itertools import product
from typing import Any, Dict, List, Optional

from hyperon_das_atomdb import WILDCARD

from hyperon_das.utils import Assignment, QueryAnswer, QueryOutputFormat


class QueryAnswerIterator(ABC):
    def __init__(self, source: Any):
        self.source = source
        self.current_value = None
        self.iterator = None

    def __iter__(self):
        return self

    def __next__(self):
        if not self.source or self.iterator is None:
            raise StopIteration
        try:
            self.current_value = next(self.iterator)
        except StopIteration as exception:
            self.current_value = None
            raise exception
        return self.current_value

    def get(self) -> Any:
        if not self.source or self.current_value is None:
            raise StopIteration
        return self.current_value

    def __str__(self):
        return str(self.source)

    @abstractmethod
    def is_empty(self) -> bool:
        pass


class ListIterator(QueryAnswerIterator):
    def __init__(self, source: List[Any]):
        super().__init__(source)
        if source:
            self.iterator = iter(self.source)
            self.current_value = source[0]

    def is_empty(self) -> bool:
        return not self.source


class ProductIterator(QueryAnswerIterator):
    def __init__(self, source: List[QueryAnswerIterator]):
        super().__init__(source)
        if not self.is_empty():
            self.current_value = tuple([iterator.get() for iterator in source])
            self.iterator = product(*self.source)

    def is_empty(self) -> bool:
        return any(iterator.is_empty() for iterator in self.source)


class LazyQueryEvaluator(ProductIterator):
    def __init__(
        self,
        link_type: str,
        source: List[QueryAnswerIterator],
        das: "DistributedAtomSpace",
        query_parameters: Optional[Dict[str, Any]],
    ):
        super().__init__(source)
        self.link_type = link_type
        self.query_parameters = query_parameters
        self.das = das
        self.buffered_answer = None

    def _replace_target_handles(self, link: Dict[str, Any]) -> Dict[str, Any]:
        targets = []
        for target_handle in link["targets"]:
            atom = self.das.db.get_atom_as_dict(target_handle)
            if atom.get("targets", None) is not None:
                atom = self._replace_target_handles(atom)
            targets.append(atom)
        link["targets"] = targets
        return link

    def __next__(self):
        if self.buffered_answer:
            try:
                return self.buffered_answer.__next__()
            except StopIteration as exception:
                self.buffered_answer = None
        target_info = super().__next__()
        target_handle = []
        wildcard_flag = False
        for query_answer_target in target_info:
            target = query_answer_target.grounded_atom
            if target.get("atom_type", None) == "variable":
                target_handle.append(WILDCARD)
                wildcard_flag = True
            else:
                target_handle.append(target["handle"])
        das_query_answer = self.das.get_links(
            self.link_type,
            None,
            target_handle,
            output_format=QueryOutputFormat.ATOM_INFO,
        )
        lazy_query_answer = []
        for answer in das_query_answer:
            assignment = None
            if wildcard_flag:
                assignment = Assignment()
                assignment_failed = False
                for query_answer_target, handle in zip(
                    target_info, answer["targets"]
                ):
                    target = query_answer_target.grounded_atom
                    if target.get("atom_type", None) == "variable":
                        if not assignment.assign(target["name"], handle):
                            assignment_failed = True
                    else:
                        if not assignment.merge(
                            query_answer_target.assignment
                        ):
                            assignment_failed = True
                    if not assignment_failed:
                        break
                if assignment_failed:
                    continue
                assignment.freeze()

            lazy_query_answer.append(
                QueryAnswer(self._replace_target_handles(answer), assignment)
            )
        self.buffered_answer = ListIterator(lazy_query_answer)
        return self.buffered_answer.__next__()
