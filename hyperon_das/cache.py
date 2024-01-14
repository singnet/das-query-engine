from abc import ABC, abstractmethod
from itertools import product
from typing import Any, Dict, List, Optional, Tuple

from hyperon_das_atomdb import WILDCARD

from hyperon_das.utils import Assignment, QueryAnswer


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


class AndEvaluator(ProductIterator):
    def __init__(self, source: List[QueryAnswerIterator]):
        super().__init__(source)

    def __next__(self):
        while True:
            candidate = super().__next__()
            assignments = [query_answer.assignment for query_answer in candidate]
            composite_assignment = Assignment.compose(assignments)
            if composite_assignment:
                composite_subgraph = [query_answer.subgraph for query_answer in candidate]
                return QueryAnswer(composite_subgraph, composite_assignment)


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
            atom = self.das.local_backend.get_atom_as_dict(target_handle)
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
            target = query_answer_target.subgraph
            if target.get("atom_type", None) == "variable":
                target_handle.append(WILDCARD)
                wildcard_flag = True
            else:
                target_handle.append(target["handle"])
        das_query_answer = self.das.get_links(self.link_type, None, target_handle)
        lazy_query_answer = []
        for answer in das_query_answer:
            assignment = None
            if wildcard_flag:
                assignment = Assignment()
                assignment_failed = False
                for query_answer_target, handle in zip(target_info, answer["targets"]):
                    target = query_answer_target.subgraph
                    if target.get("atom_type", None) == "variable":
                        if not assignment.assign(target["name"], handle):
                            assignment_failed = True
                    else:
                        if not assignment.merge(query_answer_target.assignment):
                            assignment_failed = True
                    if assignment_failed:
                        break
                if assignment_failed:
                    continue
                assignment.freeze()
            lazy_query_answer.append(QueryAnswer(self._replace_target_handles(answer), assignment))
        self.buffered_answer = ListIterator(lazy_query_answer)
        return self.buffered_answer.__next__()


class TraverselinksIterator(ListIterator):
    def __init__(self, source: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]], cursor: str, handles_only: Optional[bool] = False, **kwargs) -> None:
        super().__init__(source)
        self.cursor = cursor
        self.link_type = kwargs.get('link_type')
        self.cursor_position = kwargs.get('cursor_position')
        self.target_type = kwargs.get('target_type')
        self.custom_filter = kwargs.get('filter')
        self.targets_only = kwargs.get('targets_only', False)
        # self.handles_only = kwargs.get('handles_only', False)

    def __next__(self):
        if not self.source:
            raise StopIteration

        if (
            not self.link_type
            and self.cursor_position is None
            and not self.target_type
            and not self.custom_filter
        ):
            link, targets = super().__next__()
            if self.targets_only:
                return targets
            return link

        while True:
            link, targets = super().__next__()
            if self._filter(link, targets):
                if self.targets_only:
                    return targets
                return link

    def __getitem__(self, index):
        return self.source[index] if self.source else []

    def _filter(self, link: Dict[str, Any], targets: Dict[str, Any]) -> bool:
        if self.link_type and self.link_type != link['named_type']:
            return False

        if isinstance(self.cursor_position, int) and self.cursor_position >= 0:
            try:
                if self.cursor != link['targets'][self.cursor_position]:
                    return False
            except IndexError:
                return False

        if self.target_type:
            targets_type = [target['named_type'] for target in targets]
            if self.target_type not in targets_type:
                return False

        if self.custom_filter and callable(self.custom_filter):
            ret = self.custom_filter(link)
            if not isinstance(ret, bool):
                raise TypeError('The function must return a boolean')
            if ret is False:
                return

        # if self.handles_only:
        #    return link['handle']

        return True


class TraverseNeighborsIterator(QueryAnswerIterator):
    def __init__(self, source: TraverselinksIterator) -> None:
        super().__init__(source)
        self.cursor = self.source.cursor
        self.target_type = self.source.target_type
        self.buffered_answer = None
        self.visited_neighbors = []
        if source:
            self.iterator = iter(self.source)
            self.current_value = source[0]

    def __next__(self):
        if self.buffered_answer:
            try:
                return self.buffered_answer.__next__()
            except StopIteration:
                self.buffered_answer = None

        while True:
            targets = super().__next__()
            _new_neighbors = []
            match_found = False
            for target in targets:
                if (
                    self.cursor != target['handle']
                    and target not in self.visited_neighbors
                    and (self.target_type == target['named_type'] or not self.target_type)
                ):
                    match_found = True
                    _new_neighbors.append(target)
                    self.visited_neighbors.append(target)

            if match_found:
                self.buffered_answer = ListIterator(_new_neighbors)
                return self.buffered_answer.__next__()

    def is_empty(self) -> bool:
        return not self.source
