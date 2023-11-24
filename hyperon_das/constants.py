from enum import Enum, auto


class DatabaseType(Enum):
    REDIS_MONGO = 'redis_mongo'
    RAM_ONLY = 'ram_only'

    @classmethod
    def types(cls) -> list:
        return [choices.value for choices in cls]


class QueryOutputFormat(int, Enum):
    HANDLE = auto()
    ATOM_INFO = auto()
    JSON = auto()


class DasType(Enum):
    CLIENT = 'client'
    SERVER = 'server'

    @classmethod
    def types(cls) -> list:
        return [choices.value for choices in cls]
