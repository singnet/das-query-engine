import contextlib
from abc import ABC, abstractmethod
from random import choice
from typing import TYPE_CHECKING, Any, Dict, List, Tuple, Union

from hyperon_das_atomdb import AtomDoesNotExist

from hyperon_das.cache import (
    ListIterator,
    QueryAnswerIterator,
    TraverseLinksIterator,
    TraverseNeighborsIterator,
)

if TYPE_CHECKING:  # pragma no cover
    from hyperon_das.das import DistributedAtomSpace


class TraverseEngine:
    def __init__(self, handle: str, **kwargs) -> None:
        self.das: DistributedAtomSpace = kwargs['das']
        self._cursor = self.das.get_atom(handle)

    def get(self) -> Dict[str, Any]:
        return self.das.get_atom(self._cursor['handle'])

    def get_links(self, **kwargs) -> QueryAnswerIterator:
        incoming_links = self.das.get_incoming_links(
            atom_handle=self._cursor['handle'], handles_only=False, targets_document=True
        )
        return TraverseLinksIterator(source=incoming_links, cursor=self._cursor['handle'], **kwargs)

    def get_neighbors(self, **kwargs) -> QueryAnswerIterator:
        filtered_links = self.get_links(targets_only=True, **kwargs)
        return TraverseNeighborsIterator(source=filtered_links)

    def follow_link(self, **kwargs) -> Dict[str, Any]:
        filtered_neighbors = self.get_neighbors(**kwargs)
        if not filtered_neighbors.is_empty():
            self._cursor = filtered_neighbors.get()
        return self._cursor

    def goto(self, handle: str) -> Dict[str, Any]:
        try:
            self._cursor = self.das.get_atom(handle)
        except AtomDoesNotExist as e:
            raise e
        return self._cursor
