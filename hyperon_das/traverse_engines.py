from typing import TYPE_CHECKING, Any, Dict, Iterator

from hyperon_das_atomdb import AtomDoesNotExist

from hyperon_das.cache.iterators import TraverseLinksIterator, TraverseNeighborsIterator

if TYPE_CHECKING:  # pragma no cover
    from hyperon_das.das import DistributedAtomSpace


class TraverseEngine:
    def __init__(self, handle: str, **kwargs) -> None:
        self.das: DistributedAtomSpace = kwargs['das']

        try:
            atom = self.das.get_atom(handle)
        except AtomDoesNotExist as e:
            raise e

        self._cursor = atom

    def get(self) -> Dict[str, Any]:
        """Returns the current cursor.

        Returns:
            Dict[str, Any]: The current cursor. A Python dict with all atom data.
        """
        return self._cursor

    def get_links(self, **kwargs) -> Iterator:
        """Returns all links that have the current cursor as one of their targets, that is, any links that point to the cursor.

        In this method it's possible pass the following parameters:

        1. **link_type=XXX**: Filters to only contain links whose named_type == XXX.
        2. **cursor_position=N**: Filters the response so that only links with the current cursor at the position `n` of their target are returned.
        3. **target_type=XXX**: Filters to only contain links whose at least one of the targets has named_type == XXX.
        4. **filter=F**: F is a function used to filter results after every other filters have been applied.
            F should expect a dict (the atom document) and return True if and only if this atom should be kept.

        Returns:
            Iterator: An iterator that contains the links that match the criteria.

        Examples:
            >>> def has_score(atom):
                    if 'score' in atom and score > 0.5:
                        return True
                    return False
            >>> links = traverse_engine.get_links(
                    link_type='Ex',
                    cursor_position=2,
                    target_type='Sy',
                    filter=has_score
                )
            >>> next(links)
        """
        incoming_links = self.das.get_incoming_links(
            atom_handle=self._cursor['handle'],
            no_iterator=False,
            targets_document=True,
            cursor=0,
            chunk_size=kwargs.get('chunk_size', 500),
        )
        return TraverseLinksIterator(source=incoming_links, cursor=self._cursor['handle'], **kwargs)

    def get_neighbors(self, **kwargs) -> Iterator:
        """Get all of "neighbors" that pointing to current cursor.

        In this method it's possible pass the following parameters:

        1. **link_type=XXX**: Filters to only contain links whose named_type == XXX.
        2. **cursor_position=N**: Filters the response so that only links with the current cursor at the position `n` of their target are returned.
        3. **target_type=XXX**: Filters to only contain links whose at least one of the targets has named_type == XXX.
        4. **filter=F**: F is a function used to filter results after every other filters have been applied.
            F should expect a dict (the atom document) and return True if and only if this atom should be kept.

        Returns:
            Iterator: An iterator that contains the neighbors that match the criteria.

        Examples:
            >>> neighbors = traverse_engine.get_neighbors(
                    link_type='Ex',
                    cursor_position=2,
                    target_type='Sy',
                )
            >>> next(neighbors)
        """
        filtered_links = self.get_links(targets_only=True, **kwargs)
        return TraverseNeighborsIterator(source=filtered_links)

    def follow_link(self, **kwargs) -> Dict[str, Any]:
        """Update the current cursor by following the first of the neighbors that points to the current cursor.

        In this method it's possible pass the following parameters:

        1. **link_type=XXX**: Filters to only contain links whose named_type == XXX.
        2. **cursor_position=N**: Filters the response so that only links with the current cursor at the position `n` of their target are returned.
        3. **target_type=XXX**: Filters to only contain links whose at least one of the targets has named_type == XXX.
        4. **filter=F**: F is a function used to filter results after every other filters have been applied.
            F should expect a dict (the atom document) and return True if and only if this atom should be kept.

        Returns:
            Dict[str, Any]: The current cursor. A Python dict with all atom data.
        """
        filtered_neighbors = self.get_neighbors(**kwargs)
        if not filtered_neighbors.is_empty():
            self._cursor = filtered_neighbors.get()
        return self._cursor

    def goto(self, handle: str) -> Dict[str, Any]:
        """Reset current cursor to the passed handle.

        Args:
            handle (str): The handle of the atom to go to

        Raises:
            AtomDoesNotExist: If the corresponding atom doesn't exist

        Returns:
            Dict[str, Any]: The current cursor. A Python dict with all atom data.

        Examples:
            >>> traverse_engine.goto('asd1234567890')
            >>> {
                    'handle': 'asd1234567890',
                    'type': 'AI,
                    'composite_type_hash': 'd99asd1234567890',
                    'name': 'snet',
                    'named_type': 'AI'
                }
        """
        try:
            self._cursor = self.das.get_atom(handle)
        except AtomDoesNotExist as e:
            raise e
        return self._cursor
