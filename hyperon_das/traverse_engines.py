from random import choice
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Union

from hyperon_das.cache import ListIterator
from hyperon_das.exceptions import MultiplePathsError

if TYPE_CHECKING:
    from hyperon_das.das import DistributedAtomSpace


class TraverseEngine(ABC):
    def __init__(self, handle: str, **kwargs) -> None:
        self.das: DistributedAtomSpace = kwargs['das']
        self._cursor = handle

    def _get_incoming_links(self):
        return self.das.get_incoming_links(atom_handle=self._cursor, handles_only=False)

    def _filter_links(
        self,
        links: List[Dict[str, Any]],
        link_type: str = None,
        cursor_position: int = None,
        target_type: str = None,
        custom_filter: Callable = None,
        handles_only: bool = False,
    ) -> List[Dict[str, Any]]:
        if link_type or cursor_position is not None or target_type or custom_filter or handles_only:
            filtered_links = []
            for link in links:
                if link_type and link_type != link['named_type']:
                    continue
                if cursor_position is not None and link['targets'].index(self._cursor) != cursor_position:
                    continue
                if target_type and target_type not in link['targets_type']:
                    continue
                if custom_filter and callable(custom_filter):
                    ret = custom_filter(link)
                    if not isinstance(ret, bool):
                        raise TypeError('The function must return a boolean')
                    if ret is False:
                        continue
                if handles_only:
                    link = link['handle']
                filtered_links.append(link)
            links = filtered_links
        return links

    @abstractmethod
    def get(self) -> Union[str, Dict[str, Any]]:
        ...

    @abstractmethod
    def get_links(self, **kwargs) -> ListIterator:
        ...

    @abstractmethod
    def get_neighbors(self, **kwargs) -> Union[List[str], List[Dict[str, Any]]]:
        ...

    def follow_link(self, **kwargs) -> None:
        filtered_links_iterator = self.get_links(
            link_type=kwargs.get('link_type'),
            target_type=kwargs.get('target_type'),
            filter=kwargs.get('filter'),
        )

        filtered_links = [link for link in filtered_links_iterator]

        if not filtered_links:
            return

        unique_path = kwargs.get('unique_path', False)

        if unique_path and len(filtered_links) > 1:
            raise MultiplePathsError(
                message='Unable to follow the link. More than one path found',
                details=f'{len(filtered_links)} paths',
            )

        link = choice(filtered_links)

        if self._cursor in link['targets']:
            link['targets'].remove(self._cursor)

        handle = choice(link['targets'])

        self.goto(handle)

    def goto(self, handle: str) -> None:
        self._cursor = handle


class HandleOnlyTraverseEngine(TraverseEngine):
    def get(self) -> str:
        return self._cursor

    def get_links(self, **kwargs) -> ListIterator:
        incoming_links = self._get_incoming_links()

        if not incoming_links:
            return ListIterator([])

        filtered_links = self._filter_links(
            links=incoming_links,
            link_type=kwargs.get('link_type'),
            cursor_position=kwargs.get('cursor_position'),
            target_type=kwargs.get('target_type'),
            handles_only=True,
        )

        return ListIterator(filtered_links)

    def get_neighbors(self, **kwargs) -> List[str]:
        filtered_links_iterator = self.get_links(
            link_type=kwargs.get('link_type'),
            target_type=kwargs.get('target_type'),
            filter=kwargs.get('filter'),
        )

        targets_set = {target for link in filtered_links_iterator for target in link['targets']}

        if not targets_set:
            return []

        targets_set.discard(self._cursor)

        return list(targets_set)


class DocumentTraverseEngine(TraverseEngine):
    def get(self) -> Dict[str, Any]:
        return self.das.get_atom(self._cursor)

    def get_links(self, **kwargs) -> ListIterator:
        incoming_links = self._get_incoming_links()

        if not incoming_links:
            return ListIterator([])

        filtered_links = self._filter_links(
            links=incoming_links,
            link_type=kwargs.get('link_type'),
            cursor_position=kwargs.get('cursor_position'),
            target_type=kwargs.get('target_type'),
            custom_filter=kwargs.get('filter'),
        )

        return ListIterator(filtered_links)

    def get_neighbors(self, **kwargs) -> List[Dict[str, Any]]:
        filtered_links_iterator = self.get_links(
            link_type=kwargs.get('link_type'),
            target_type=kwargs.get('target_type'),
            filter=kwargs.get('filter'),
        )

        targets_document = []
        for link in filtered_links_iterator:
            for document in link['targets_document']:
                if self._cursor == document['handle'] or document in targets_document:
                    continue
                targets_document.append(document)

        return targets_document
