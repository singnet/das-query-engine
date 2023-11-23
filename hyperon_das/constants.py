from enum import Enum, auto


class DatabaseType(Enum):
    REDIS_MONGO = 'redis_mongo'
    RAM_ONLY = 'ram_only'

    @classmethod
    def _types(cls) -> list:
        return [choices.value for choices in cls]

    @property
    def types(self):
        return self._types()


class QueryOutputFormat(int, Enum):
    HANDLE = auto()
    ATOM_INFO = auto()
    JSON = auto()


class DasType(Enum):
    CLIENT = 'client'
    SERVER = 'server'

    @classmethod
    def _types(cls) -> list:
        return [choices.value for choices in cls]

    @property
    def types(self):
        return self._types()
