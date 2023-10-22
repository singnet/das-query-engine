from dataclasses import dataclass
from enum import Enum, auto
from typing import Optional


class QueryOutputFormat(int, Enum):
    HANDLE = auto()
    ATOM_INFO = auto()
    JSON = auto()


@dataclass
class QueryParameters:
    toplevel_only: Optional[bool] = False
    return_type: Optional[QueryOutputFormat] = QueryOutputFormat.HANDLE.value

    @classmethod
    def values(cls) -> list:
        return list(cls.__dataclass_fields__.keys())
