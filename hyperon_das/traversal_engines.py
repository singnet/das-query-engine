from abc import ABC, abstractmethod
from typing import Any, Dict, List

from hyperon_das_atomdb import AtomDB

from hyperon_das.query_engines import QueryEngine


class TraverseEngine(ABC):
    def __init__(self, handle: str, **kwargs) -> None:
        super().__init__()
    @abstractmethod
    def get(self): ...    
    @abstractmethod
    def get_links(self, kwargs): ...
    @abstractmethod
    def get_neighbors(self, kwargs): ...
    @abstractmethod
    def follow_link(self, kwargs): ...
    @abstractmethod
    def goto(self, handle: str): ...

class DocumentTraverseEngine(TraverseEngine):
    def __init__(self, handle: str, **kwargs) -> None:
        self.backend: AtomDB = kwargs['backend']
        self.query_engine = kwargs['query_engine']
        self._cursor = handle
        
    def get(self) -> Dict[str, Any]:
        return self.query_engine.get_atom(self._cursor)
    
    def get_links(self, **kwargs) -> List[Dict[str, Any]]:
        link_type = kwargs.get('link_type')
        cursor_position = kwargs.get('cursor_position')
        target_type = kwargs.get('target_type')
        
        #links = self.backend.get_links_pointing_atom(atom_handle=self._cursor)
        
        links = self.query_engine.get_links_pointing_atom(atom_handle=self._cursor)
        
        if link_type:
            links = [link for link in links if link_type == link['named_type']]
        
        if cursor_position is not None:
            links = [link for link in links if link['targets'].index(self._cursor) == cursor_position]
        
        if target_type:
            links2 = []
            for link in links:
                for target in link['targets']:
                    try:
                        _type = self.backend.get_link_type(target)
                    except Exception:
                        try:
                            _type = self.backend.get_node_type(target)
                        except Exception:
                            _type = None
                    if _type == target_type:
                        links2.append(link)
                        break                    
        
        return links

    def get_neighbors(self, **kwargs):
        pass
    
    def follow_link(self, **kwargs):
        pass
    
    def goto(self, handle: str):
        self._cursor = handle
        return self.get()