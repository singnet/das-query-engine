from enum import Enum, auto


class QueryOutputFormat(int, Enum):
    HANDLE = auto()
    ATOM_INFO = auto()
    JSON = auto()
