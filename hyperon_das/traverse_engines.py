import contextlib
from abc import ABC, abstractmethod
from random import choice
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

from hyperon_das.cache import (
    FollowLinkIterator,
    ListIterator,
    QueryAnswerIterator,
    TraverseLinksIterator,
    TraverseNeighborsIterator,
)
from hyperon_das.exceptions import MultiplePathsError

if TYPE_CHECKING:
    from hyperon_das.das import DistributedAtomSpace


class TraverseEngine:
    def __init__(self, handle: str, **kwargs) -> None:
        self.das: DistributedAtomSpace = kwargs['das']
        self._cursor = handle

    def get(self) -> Dict[str, Any]:
        return self.das.get_atom(self._cursor)

    def get_links(self, **kwargs) -> QueryAnswerIterator:
        incoming_links = self.das.get_incoming_links(
            atom_handle=self._cursor, handles_only=False, targets_document=True
        )
        return TraverseLinksIterator(source=incoming_links, cursor=self._cursor, **kwargs)

    def get_neighbors(self, **kwargs) -> QueryAnswerIterator:
        filtered_links = self.get_links(targets_only=True, **kwargs)
        return TraverseNeighborsIterator(source=filtered_links)

    def follow_link(self, **kwargs) -> None:
        filtered_neighbors = self.get_neighbors(**kwargs)
        ret = FollowLinkIterator(source=filtered_neighbors)
        with contextlib.suppress(StopIteration):
            atom = next(ret)
            self._cursor = atom['handle']

    def goto(self, handle: str) -> None:
        try:
            self.das.get_atom(handle)
        except AtomDoesNotExist as e:
            raise e
        self._cursor = handle
