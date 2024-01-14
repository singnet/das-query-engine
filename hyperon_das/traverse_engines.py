from abc import ABC, abstractmethod
from random import choice
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

from hyperon_das.cache import (
    ListIterator,
    QueryAnswerIterator,
    TraverselinksIterator,
    TraverseNeighborsIterator,
)
from hyperon_das.exceptions import MultiplePathsError

if TYPE_CHECKING:
    from hyperon_das.das import DistributedAtomSpace


class TraverseEngine(ABC):
    def __init__(self, handle: str, **kwargs) -> None:
        self.das: DistributedAtomSpace = kwargs['das']
        self._cursor = handle

    def _get_incoming_links(self, **kwargs) -> List[Union[Tuple[Dict[str, Any], List[Dict[str, Any]]], Dict[str, Any]]]:
        return self.das.get_incoming_links(atom_handle=self._cursor, **kwargs)

    @abstractmethod
    def get(self) -> Union[str, Dict[str, Any]]:
        ...

    @abstractmethod
    def get_links(self, **kwargs) -> QueryAnswerIterator:
        ...

    @abstractmethod
    def get_neighbors(self, **kwargs) -> QueryAnswerIterator:
        ...

    @abstractmethod
    def follow_link(self, **kwargs) -> None:
        ...

    def goto(self, handle: str) -> None:
        self._cursor = handle


class HandleOnlyTraverseEngine(TraverseEngine):
    def get(self) -> str:
        return self._cursor
   
    def get_links(self, **kwargs) -> QueryAnswerIterator:
        incoming_links = self._get_incoming_links(handles_only=False, targets_document=True)
        return TraverselinksIterator(source=incoming_links, cursor=self._cursor, handles_only=True, **kwargs)

    def get_neighbors(self, **kwargs) -> List[str]:
        filtered_links_iterator = self.get_links(
            link_type=kwargs.get('link_type'), target_type=kwargs.get('target_type')
        )

        targets_set = {target for link in filtered_links_iterator for target in link['targets']}

        if not targets_set:
            return []

        targets_set.discard(self._cursor)

        return list(targets_set)

    def follow_link(self, **kwargs) -> None:
        target_type = kwargs.get('target_type')

        filtered_links_iterator = self.get_links(
            link_type=kwargs.get('link_type'), target_type=target_type, handles_only=False
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

        valid_targets = []

        valid_targets = []
        for target in link['targets_document']:
            handle = target['handle']
            if target_type:
                if handle != self._cursor and target['named_type'] == target_type:
                    valid_targets.append(target)
            else:
                if handle != self._cursor:
                    valid_targets.append(target)

        valid_targets2 = []
        for target in link['targets_document']:
            if (
                target['handle'] != self._cursor and target['named_type'] == target_type
            ) or not target_type:
                valid_targets2.append(target)

        if valid_targets:
            handle = choice(valid_targets)
            self._cursor = handle


class DocumentTraverseEngine(TraverseEngine):
    def get(self) -> Dict[str, Any]:
        return self.das.get_atom(self._cursor)

    def get_links(self, **kwargs) -> QueryAnswerIterator:
        incoming_links = self._get_incoming_links(handles_only=False, targets_document=True)
        return TraverselinksIterator(source=incoming_links, cursor=self._cursor, **kwargs)

    def get_neighbors(self, **kwargs) -> QueryAnswerIterator:
        filtered_targets_iterator = self.get_links(
            targets_only=True,
            link_type=kwargs.get('link_type'),
            target_type=kwargs.get('target_type'),
            filter=kwargs.get('filter'),
        )
        return TraverseNeighborsIterator(source=filtered_targets_iterator)

    def follow_link(self, **kwargs) -> None:
        target_type = kwargs.get('target_type')

        filtered_targets_iterator = self.get_links(
            targets_only=True,
            link_type=kwargs.get('link_type'),
            target_type=target_type,
            filter=kwargs.get('filter'),
        )

        filtered_targets = [target for target in filtered_targets_iterator]

        if not filtered_targets:
            return

        unique_path = kwargs.get('unique_path', False)

        if unique_path and len(filtered_targets) > 1:
            raise MultiplePathsError(
                message='Unable to follow the link. More than one path found',
                details=f'{len(filtered_targets)} paths',
            )

        targets = filtered_targets[0]

        valid_targets = []
        for target in targets:
            if self._cursor != target['handle'] and (
                target_type == target['named_type'] or not target_type
            ):
                valid_targets.append(target)

        if valid_targets:
            target = choice(valid_targets)
            self._cursor = target['handle']
