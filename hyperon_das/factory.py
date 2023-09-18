from abc import abstractmethod
from enum import Enum
from typing import Protocol

from hyperon_das_atomdb import IAtomDB
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB


class DatabaseType(Enum):
    REDIS_MONGO = 'redis_mongo'
    HASHTABLE = 'hash_table'

    @classmethod
    def values(cls):
        return [choices.value for choices in cls]


class IDatabaseFactory(Protocol):
    @abstractmethod
    def create_redis_mongo_database(self):
        ...  # pragma no cover

    @abstractmethod
    def create_hastable_database(self):
        ...  # pragma no cover


class DatabaseFactory:
    def __init__(self, database_name: DatabaseType) -> None:
        self.name = database_name

    def create_redis_mongo_database(self):
        return RedisMongoDB

    def create_hastable_database(self):
        # TODO: WIP
        return InMemoryDB


def database_factory(factory: IDatabaseFactory) -> IAtomDB:
    redis_mongo_database = factory.create_redis_mongo_database()
    hashtable_database = factory.create_hastable_database()

    if factory.name == DatabaseType.REDIS_MONGO.value:
        return redis_mongo_database()

    if factory.name == DatabaseType.HASHTABLE.value:
        return hashtable_database()
