from abc import ABC, abstractmethod
from collections import deque
from itertools import product
from threading import Semaphore, Thread
from typing import Any, Generic, TypeVar, Iterator, Callable

from hyperon_das_atomdb import WILDCARD

from hyperon_das.query_engines.query_engine_protocol import QueryEngine
from hyperon_das.utils import Assignment, QueryAnswer


T = TypeVar("T")
V = TypeVar("V")


class QueryAnswerIterator(ABC, Generic[T, V]):
    def __init__(self, source: T):
        self.source = source
        self.current_value: V | None = None
        self.iterator: Iterator[V] | None = None

    def __iter__(self) -> "QueryAnswerIterator":
        return self

    def __next__(self) -> V:
        if not self.source or self.iterator is None:
            raise StopIteration
        try:
            self.current_value = next(self.iterator)
        except StopIteration as exception:
            self.current_value = None
            raise exception
        return self.current_value

    def __str__(self) -> str:
        return str(self.source)

    @abstractmethod
    def is_empty(self) -> bool:
        """
        Determines if the iterator has no more elements to iterate over.

        Returns:
            bool: True if the iterator is empty and has no more elements to yield, False otherwise.
        """
        ...

    def get(self) -> V:
        if not self.source or self.current_value is None:
            raise StopIteration
        return self.current_value


class ListIterator(QueryAnswerIterator[list[V], V]):
    def __init__(self, source: list[V]):
        super().__init__(source)
        if source:
            self.iterator = iter(self.source)
            self.current_value = source[0]

    def is_empty(self) -> bool:
        return not self.source


class ProductIterator(QueryAnswerIterator[list[QueryAnswerIterator], tuple[QueryAnswer]]):
    def __init__(self, source: list[QueryAnswerIterator]):
        super().__init__(source)
        if not self.is_empty():
            self.current_value = tuple([iterator.get() for iterator in source])
            self.iterator = product(*self.source)

    def is_empty(self) -> bool:
        return any(iterator.is_empty() for iterator in self.source)


class AndEvaluator(ProductIterator):
    def __init__(self, source: list[QueryAnswerIterator]):
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
        source: list[QueryAnswerIterator],
        query_engine: QueryEngine,
        query_parameters: dict[str, Any] | None,
    ):
        super().__init__(source)
        self.link_type = link_type
        self.query_parameters = query_parameters
        self.query_engine = query_engine
        self.buffered_answer: ListIterator[QueryAnswer] | None = None

    def _replace_target_handles(self, link: dict[str, Any]) -> dict[str, Any]:
        targets: list[dict[str, Any]] = []
        for target_handle in link["targets"]:
            atom = self.query_engine.get_atom(target_handle)
            if atom is None:
                continue
            if atom.get("targets", None) is not None:
                atom = self._replace_target_handles(atom)
            targets.append(atom)
        link["targets"] = targets
        return link

    def __next__(self) -> QueryAnswer:
        if self.buffered_answer:
            try:
                return next(self.buffered_answer)
            except StopIteration:
                self.buffered_answer = None
        while self.buffered_answer is None:
            target_info = super().__next__()
            target_handle = []
            wildcard_flag = False
            for query_answer_target in target_info:
                target = query_answer_target.subgraph
                if query_answer_target.assignment:
                    wildcard_flag = True
                if target.get("atom_type", None) == "variable":
                    target_handle.append(WILDCARD)
                    wildcard_flag = True
                else:
                    target_handle.append(target["handle"])
            das_query_answer: list[dict] = self.query_engine.get_links(
                self.link_type, None, target_handle
            )
            lazy_query_answer: list[QueryAnswer] = []
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
                lazy_query_answer.append(
                    QueryAnswer(self._replace_target_handles(answer), assignment)
                )
            if lazy_query_answer:
                self.buffered_answer = ListIterator(lazy_query_answer)
                return next(self.buffered_answer)

        raise StopIteration


class BaseLinksIterator(Generic[T, V], QueryAnswerIterator[T, V], ABC):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        super().__init__(source)
        if not self.source.is_empty():
            if not hasattr(self, 'backend'):
                self.backend = kwargs.get('backend')
            self.chunk_size = kwargs.get('chunk_size', 1000)
            self.cursor = kwargs.get('cursor', 0)
            self.buffer_queue = deque()
            self.iterator = self.source
            self.current_value = self.get_current_value()
            self.fetch_data_thread = Thread(target=self._fetch_data)
            if self.cursor != 0:
                self.semaphore = Semaphore(1)
                self.fetch_data_thread.start()

    def __next__(self) -> V:
        if self.iterator:
            try:
                return self.get_next_value()
            except StopIteration as e:
                self.current_value = None
                self.iterator = None
                if self.fetch_data_thread.is_alive():
                    self.fetch_data_thread.join()
                if self.cursor == 0 and len(self.buffer_queue) == 0:
                    self.current_value = None
                    raise e
                self._refresh_iterator()
                self.fetch_data_thread = Thread(target=self._fetch_data)
                if self.cursor != 0:
                    self.fetch_data_thread.start()
                return self.__next__()
        raise StopIteration

    def _fetch_data(self) -> None:
        kwargs = self.get_fetch_data_kwargs()
        while True:
            if self.semaphore.acquire(blocking=False):
                try:
                    cursor, answer = self.get_fetch_data(**kwargs)
                    self.cursor = cursor
                    self.buffer_queue.extend(answer)
                finally:
                    self.semaphore.release()
                break

    def _refresh_iterator(self) -> None:
        if self.semaphore.acquire(blocking=False):
            try:
                self.source = ListIterator(list(self.buffer_queue))
                self.iterator = self.source
                self.current_value = self.get_current_value()
                self.buffer_queue.clear()
            finally:
                self.semaphore.release()

    def is_empty(self) -> bool:
        return not self.iterator

    @abstractmethod
    def get_next_value(self) -> V:
        raise NotImplementedError("Subclasses must implement get_next_value method")

    @abstractmethod
    def get_current_value(self) -> V:
        raise NotImplementedError("Subclasses must implement get_current_value method")

    @abstractmethod
    def get_fetch_data_kwargs(self) -> dict[str, Any]:
        raise NotImplementedError("Subclasses must implement get_fetch_data_kwargs method")

    @abstractmethod
    def get_fetch_data(self, **kwargs) -> tuple:
        raise NotImplementedError("Subclasses must implement get_fetch_data method")


class LocalIncomingLinks(BaseLinksIterator[ListIterator, dict[str, Any]]):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        self.atom_handle = kwargs.get('atom_handle')
        self.targets_document = kwargs.get('targets_document', False)
        super().__init__(source, **kwargs)

    def get_next_value(self) -> dict[str, Any]:
        if not self.is_empty() and self.backend:
            link_handle = next(self.iterator)
            link_document = self.backend.get_atom(
                link_handle, targets_document=self.targets_document
            )
            self.current_value = link_document
        return self.current_value

    def get_current_value(self) -> dict[str, Any] | None:
        if self.backend:
            try:
                return self.backend.get_atom(
                    self.source.get(), targets_document=self.targets_document
                )
            except StopIteration:
                return None

    def get_fetch_data_kwargs(self) -> dict[str, Any]:
        return {'handles_only': True, 'cursor': self.cursor, 'chunk_size': self.chunk_size}

    def get_fetch_data(self, **kwargs) -> tuple:
        if self.backend:
            return self.backend.get_incoming_links(self.atom_handle, **kwargs)


class RemoteIncomingLinks(BaseLinksIterator):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        self.atom_handle = kwargs.get('atom_handle')
        self.targets_document = kwargs.get('targets_document', False)
        self.returned_handles = set()
        super().__init__(source, **kwargs)

    def get_next_value(self) -> Any:
        if not self.is_empty():
            while True:
                link_document = next(self.iterator)
                if isinstance(link_document, tuple) or isinstance(link_document, list):
                    handle = link_document[0]['handle']
                else:
                    handle = link_document['handle']
                if handle not in self.returned_handles:
                    self.returned_handles.add(handle)
                    self.current_value = link_document
                    break
        return self.current_value

    def get_current_value(self) -> Any:
        try:
            return self.source.get()
        except StopIteration:
            return None

    def get_fetch_data_kwargs(self) -> dict[str, Any]:
        return {
            'cursor': self.cursor,
            'chunk_size': self.chunk_size,
            'targets_document': self.targets_document,
        }

    def get_fetch_data(self, **kwargs) -> tuple:
        if self.backend:
            return self.backend.get_incoming_links(self.atom_handle, **kwargs)


class LocalGetLinks(BaseLinksIterator):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        self.link_type = kwargs.get('link_type')
        self.target_types = kwargs.get('target_types')
        self.link_targets = kwargs.get('link_targets')
        self.toplevel_only = kwargs.get('toplevel_only')
        super().__init__(source, **kwargs)

    def get_next_value(self) -> Any:
        if not self.is_empty() and self.backend:
            value = next(self.iterator)
            self.current_value = self.backend._to_link_dict_list([value])[0]
        return self.current_value

    def get_current_value(self) -> Any:
        if self.backend:
            try:
                value = self.source.get()
                return self.backend._to_link_dict_list([value])[0]
            except StopIteration:
                return None

    def get_fetch_data_kwargs(self) -> dict[str, Any]:
        return {
            'cursor': self.cursor,
            'chunk_size': self.chunk_size,
            'toplevel_only': self.toplevel_only,
        }

    def get_fetch_data(self, **kwargs) -> tuple:
        if self.backend:
            return self.backend._get_related_links(
                self.link_type, self.target_types, self.link_targets, **kwargs
            )


class RemoteGetLinks(BaseLinksIterator):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        self.link_type = kwargs.get('link_type')
        self.target_types = kwargs.get('target_types')
        self.link_targets = kwargs.get('link_targets')
        self.toplevel_only = kwargs.get('toplevel_only')
        self.returned_handles = set()
        super().__init__(source, **kwargs)

    def get_next_value(self) -> Any:
        if not self.is_empty():
            value = next(self.iterator)
            handle = value.get('handle')
            if handle not in self.returned_handles:
                self.returned_handles.add(handle)
                self.current_value = value
        return self.current_value

    def get_current_value(self) -> Any:
        try:
            return self.source.get()
        except StopIteration:
            return None

    def get_fetch_data_kwargs(self) -> dict[str, Any]:
        return {
            'cursor': self.cursor,
            'chunk_size': self.chunk_size,
            'toplevel_only': self.toplevel_only,
        }

    def get_fetch_data(self, **kwargs) -> tuple:
        if self.backend:
            return self.backend.get_links(
                self.link_type, self.target_types, self.link_targets, **kwargs
            )


class CustomQuery(BaseLinksIterator):
    def __init__(self, source: ListIterator, **kwargs) -> None:
        self.index_id = kwargs.pop('index_id', None)
        self.backend = kwargs.pop('backend', None)
        self.is_remote = kwargs.pop('is_remote', False)
        self.kwargs = kwargs
        super().__init__(source, **kwargs)

    def get_next_value(self) -> Any:
        if not self.is_empty():
            self.current_value = next(self.iterator)
        return self.current_value

    def get_current_value(self) -> Any:
        try:
            return self.source.get()
        except StopIteration:
            return None

    def get_fetch_data_kwargs(self) -> dict[str, Any]:
        kwargs = self.kwargs
        kwargs.update({'cursor': self.cursor, 'chunk_size': self.chunk_size})
        return kwargs

    def get_fetch_data(self, **kwargs) -> tuple:
        if self.backend:
            if self.is_remote:
                return self.backend.custom_query(
                    self.index_id, query=kwargs.get('query', []), **kwargs
                )
            else:
                return self.backend.get_atoms_by_index(
                    self.index_id, query=kwargs.get('query', []), **kwargs
                )


class TraverseLinksIterator(
    QueryAnswerIterator[LocalIncomingLinks | RemoteIncomingLinks, dict[str, Any]]
):
    def __init__(self, source: LocalIncomingLinks | RemoteIncomingLinks, **kwargs) -> None:
        super().__init__(source)
        self.cursor = kwargs.get('cursor')
        self.targets_only = kwargs.get('targets_only', False)
        self.buffer = None
        self.link_type = kwargs.get('link_type')
        self.cursor_position = kwargs.get('cursor_position')
        self.target_type = kwargs.get('target_type')
        self.custom_filter: Callable[[dict[str, Any]], bool] | None = kwargs.get('filter')
        if not self.source.is_empty():
            self.iterator = self.source
            self.current_value = self._find_first_valid_element()
            self.buffer = self.current_value

    def __next__(self) -> dict[str, Any]:
        while True:
            if self.buffer:
                buffered_value, self.buffer = self.buffer, None
                return buffered_value
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

    def _filter(self, link: dict[str, Any], targets: dict[str, Any]) -> bool:
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

        if self.custom_filter:
            deep_link = link.copy()
            deep_link['targets'] = targets
            if self.apply_custom_filter(deep_link) is False:
                return False

        return True

    def apply_custom_filter(
        self, atom: dict[str, Any], custom_filter: Callable[[dict[str, any]], bool] | None = None
    ) -> bool:
        custom_filter = custom_filter if custom_filter else self.custom_filter

        assert callable(
            custom_filter
        ), "The custom_filter must be a function with this signature 'F(atom: dict) -> bool: ...'"

        try:
            result = custom_filter(atom)
            assert isinstance(result, bool), "The custom_filter must return a boolean value"
            return result
        except Exception as e:
            raise Exception(
                f"Error while applying the custom filter: {type(e)}({e})"
            )

    def is_empty(self) -> bool:
        return not self.current_value


class TraverseNeighborsIterator(QueryAnswerIterator[TraverseLinksIterator, dict[str, Any]]):
    def __init__(self, source: TraverseLinksIterator, **kwargs) -> None:
        super().__init__(source)
        self.buffered_answer: ListIterator | None = None
        self.cursor = self.source.cursor
        self.target_type = self.source.target_type
        self.visited_neighbors = []
        self.custom_filter: Callable[[dict[str, any]], bool] | None = kwargs.get('filter')
        if not self.source.is_empty():
            self.iterator = source
            self.current_value = self._find_first_valid_element()

    def __next__(self) -> dict[str, Any]:
        if self.buffered_answer:
            try:
                return next(self.buffered_answer)
            except StopIteration:
                self.buffered_answer = None

        while True:
            targets: list[dict[str, Any]] = super().__next__()
            _new_neighbors, match_found = self._process_targets(targets)
            if match_found:
                self.buffered_answer = ListIterator(_new_neighbors)
                self.current_value = next(self.buffered_answer)
                return self.current_value

    def _find_first_valid_element(self) -> dict[str, Any]:
        for targets in self.iterator:
            _new_neighbors, match_found = self._process_targets(targets)
            if match_found:
                self.buffered_answer = ListIterator(_new_neighbors)
                return self.buffered_answer.get()

    def _process_targets(self, targets: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], bool]:
        answer: list[dict[str, Any]] = []
        match_found = False
        for target in targets:
            if self._filter(target):
                match_found = True
                self.visited_neighbors.append(target['handle'])
                answer.append(target)
        return answer, match_found

    def _filter(self, target: dict[str, Any]) -> bool:
        handle = target['handle']
        if not (
            self.cursor != handle
            and handle not in self.visited_neighbors
            and (self.target_type == target['named_type'] or not self.target_type)
        ):
            return False

        if self.custom_filter:
            return self.source.apply_custom_filter(target, custom_filter=self.custom_filter)

        return True

    def is_empty(self) -> bool:
        return not self.current_value
