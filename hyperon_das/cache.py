from abc import ABC, abstractmethod
from collections import deque
from itertools import product
from threading import Semaphore, Thread
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

    def __str__(self):
        return str(self.source)

    @abstractmethod
    def is_empty(self) -> bool:
        pass

    def get(self) -> Any:
        if not self.source or self.current_value is None:
            raise StopIteration
        return self.current_value


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
        # das: "DistributedAtomSpace" Circular import,
        das,
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
            except StopIteration:
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


class TraverseLinksIterator(QueryAnswerIterator):
    def __init__(self, source: List[Tuple[Dict[str, Any], List[Dict[str, Any]]]], **kwargs) -> None:
        super().__init__(source)
        self.cursor = kwargs.get('cursor')
        self.link_type = kwargs.get('link_type')
        self.cursor_position = kwargs.get('cursor_position')
        self.target_type = kwargs.get('target_type')
        self.custom_filter = kwargs.get('filter')
        self.targets_only = kwargs.get('targets_only', False)
        self.current_value = self._find_first_valid_element()
        if not self.is_empty():
            self.iterator = iter(source)

    def __next__(self):
        while True:
            link, targets = super().__next__()
            if (
                not self.link_type
                and self.cursor_position is None
                and not self.target_type
                and not self.custom_filter
            ) or self._filter(link, targets):
                self.current_value = targets if self.targets_only else link
                break

        return self.current_value

    def _find_first_valid_element(self):
        if self.source:
            for link, targets in self.source:
                if self._filter(link, targets):
                    return targets if self.targets_only else link

    def _filter(self, link: Dict[str, Any], targets: Dict[str, Any]) -> bool:
        if self.link_type and self.link_type != link['named_type']:
            return False

        try:
            if (
                self.cursor_position is not None
                and self.cursor != link['targets'][self.cursor_position]
            ):
                return False
        except IndexError:
            return False
        except Exception as e:
            raise e

        if self.target_type:
            if not any(target['named_type'] == self.target_type for target in targets):
                return False

        if self.custom_filter and callable(self.custom_filter):
            ret = self.custom_filter(link)
            if not isinstance(ret, bool):
                raise TypeError('The function must return a boolean')
            if ret is False:
                return False

        return True

    def is_empty(self) -> bool:
        return not self.current_value


class TraverseNeighborsIterator(QueryAnswerIterator):
    def __init__(self, source: TraverseLinksIterator, **kwargs) -> None:
        super().__init__(source)
        self.buffered_answer = None
        self.cursor = self.source.cursor
        self.target_type = self.source.target_type
        self.visited_neighbors = []
        self.current_value = self._find_first_valid_element()
        if not self.is_empty():
            self.iterator = source

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
                if self._filter(target):
                    match_found = True
                    self.visited_neighbors.append(target['handle'])
                    _new_neighbors.append(target)

            if match_found:
                self.buffered_answer = ListIterator(_new_neighbors)
                self.current_value = self.buffered_answer.__next__()
                return self.current_value

    def _find_first_valid_element(self):
        if self.source.current_value:
            for target in self.source.current_value:
                if self._filter(target):
                    return target

    def _filter(self, target: Dict[str, Any]) -> bool:
        handle = target['handle']
        if (
            self.cursor != handle
            and handle not in self.visited_neighbors
            and (self.target_type == target['named_type'] or not self.target_type)
        ):
            return True
        return False

    def is_empty(self) -> bool:
        return not self.current_value


class IncomingLinksIterator(QueryAnswerIterator):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        super().__init__(source)
        if not self.source.is_empty():
            self.backend = kwargs.get('backend')
            self.targets_document = kwargs.get('targets_document', False)
            self.atom_handle = kwargs.get('atom_handle')
            self.chunk_size = kwargs.get('chunk_size', 1000)
            self.cursor = kwargs.get('cursor', 0)
            self.buffer_queue = deque()
            self.iterator = self.source
            self.current_value = self._get_current_value()
            self.fetch_data_thread = Thread(target=self._fetch_data)
            if self.cursor != 0:
                self.semaphore = Semaphore(1)
                self.fetch_data_thread.start()

    def __next__(self) -> Any:
        if self.iterator:
            try:
                self._get_next_value()
            except StopIteration as e:
                if self.fetch_data_thread.is_alive():
                    self.fetch_data_thread.join()
                self.iterator = None
                if self.cursor == 0 and len(self.buffer_queue) == 0:
                    self.current_value = None
                    raise e
                self._refresh_iterator()
                self.fetch_data_thread = Thread(target=self._fetch_data)
                if self.cursor != 0:
                    self.fetch_data_thread.start()
        return self.get()

    def _fetch_data(self) -> None:
        kwargs = self._get_fetch_data_kwargs()
        while True:
            if self.semaphore.acquire(blocking=False):
                try:
                    cursor, links_handle = self.backend.get_incoming_links(
                        self.atom_handle, **kwargs
                    )
                    self.cursor = cursor
                    self.buffer_queue.extend(links_handle)
                finally:
                    self.semaphore.release()
                break

    def _refresh_iterator(self) -> None:
        if self.semaphore.acquire(blocking=False):
            try:
                self.source = ListIterator(list(self.buffer_queue))
                self.iterator = self.source
                self.current_value = self._get_current_value()
                self.buffer_queue.clear()
            finally:
                self.semaphore.release()

    def is_empty(self) -> bool:
        return not self.iterator and self.cursor == 0

    def _get_next_value(self) -> None:
        raise NotImplementedError("Subclasses must implement _get_next_value method")

    def _get_current_value(self) -> Any:
        raise NotImplementedError("Subclasses must implement _get_current_value method")

    def _get_fetch_data_kwargs(self) -> Dict[str, Any]:
        raise NotImplementedError("Subclasses must implement _get_fetch_data_kwargs method")


class LocalIncomingLinks(IncomingLinksIterator):
    def __init__(self, source: QueryAnswerIterator, **kwargs) -> None:
        super().__init__(source, **kwargs)

    def _get_next_value(self) -> None:
        link_handle = next(self.iterator)
        link_document = self.backend.get_atom(link_handle, targets_document=self.targets_document)
        self.current_value = link_document

    def _get_current_value(self) -> Any:
        return self.backend.get_atom(self.source.get(), targets_document=self.targets_document)

    def _get_fetch_data_kwargs(self) -> Dict[str, Any]:
        return {'handles_only': True, 'cursor': self.cursor, 'chunk_size': self.chunk_size}


class RemoteIncomingLinks(IncomingLinksIterator):
    def __init__(self, source: QueryAnswerIterator, **kwargs) -> None:
        super().__init__(source, **kwargs)
        self.returned_handles = set()

    def _get_next_value(self) -> None:
        while True:
            link_document = next(self.iterator)
            if isinstance(link_document, tuple):
                handle = link_document[0]['handle']
            else:
                handle = link_document['handle']
            if handle not in self.returned_handles:
                self.returned_handles.add(handle)
                self.current_value = link_document
                break

    def _get_current_value(self) -> Any:
        return self.source.get()

    def _get_fetch_data_kwargs(self) -> Dict[str, Any]:
        return {
            'cursor': self.cursor,
            'chunk_size': self.chunk_size,
            'targets_document': self.targets_document,
        }
