import random
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Set

from hyperon_das_atomdb import AtomDB

from hyperon_das.cache import ListIterator
from hyperon_das.query_engines import QueryEngine


class TraverseEngine(ABC):
    def __init__(self, handle: str, **kwargs) -> None:
        super().__init__()

    @abstractmethod
    def get(self):
        ...

    @abstractmethod
    def get_links(self, kwargs):
        ...

    @abstractmethod
    def get_neighbors(self, kwargs):
        ...

    @abstractmethod
    def follow_link(self, kwargs):
        ...

    @abstractmethod
    def goto(self, handle: str):
        ...


class HandleOnlyTraverseEngine:
    pass


class DocumentTraverseEngine(TraverseEngine):
    def __init__(self, handle: str, **kwargs) -> None:
        self.das = kwargs['das']
        self._cursor = handle

    def get(self) -> Dict[str, Any]:
        return self.das.get_atom(self._cursor)

    def get_links(self, **kwargs) -> ListIterator:
        link_type = kwargs.get('link_type')
        cursor_position = kwargs.get('cursor_position')
        target_type = kwargs.get('target_type')
        custom_filter = kwargs.get('filter')

        links = self.das.get_incoming_links(atom_handle=self._cursor, handles_only=False)

        if link_type or cursor_position is not None or target_type or custom_filter:
            filtered_links = []
            for link in links:
                if link_type and link_type != link['named_type']:
                    continue
                if cursor_position is not None and link['targets'].index(self._cursor) != cursor_position:
                    continue
                if target_type and target_type not in link['targets_type']:
                    continue
                # WIP
                if custom_filter:
                    pass
                filtered_links.append(link)
            links = filtered_links

        return ListIterator(links)

    def get_neighbors(self, **kwargs) -> List[str]:
        link_type = kwargs.get('link_type')
        target_type = kwargs.get('target_type')
        custom_filter = kwargs.get('filter')

        links = self.das.get_incoming_links(atom_handle=self._cursor, handles_only=False)

        if link_type or target_type or custom_filter:
            filtered_links = []

            for link in links:
                if link_type and link_type != link['named_type']:
                    continue
                if target_type and target_type not in link['targets_type']:
                    continue
                # WIP
                if custom_filter:
                    pass
                filtered_links.append(link)

            links = filtered_links

        result = set()
        for link in links:
            result.update(link['targets'])
        
        result.discard(self._cursor)

        return list(result)

    def follow_link(self, **kwargs):
        link_type = kwargs.get('link_type')
        target_type = kwargs.get('target_type')
        unique_path = kwargs.get('unique_path', False)
        # custom_filter = kwargs.get('filter')

        links = self.das.get_incoming_links(atom_handle=self._cursor, handles_only=False)

        if link_type or target_type:
            filtered_links = []

            for link in links:
                if link_type and link_type != link['named_type']:
                    continue
                if target_type and target_type not in link['targets_type']:
                    continue
                filtered_links.append(link)

            links = filtered_links

        if unique_path and len(links) > 1:
            raise ValueError

        link = random.choice(links)
        link['targets'].remove(self._cursor)
        handle = random.choice(link['targets'])

        self.goto(handle)

    def goto(self, handle: str):
        self._cursor = handle
        return self.get()
