from abc import abstractmethod
from enum import Enum
from typing import Protocol

from hyperon_das_atomdb import IAtomDB
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB

from tests.mock import DatabaseMock


class DatabaseType(Enum):
    REDIS_MONGO = 'redis_mongo'
    RAM_ONLY = 'ram_only'
    TEST = 'test'

    @classmethod
    def values(cls):
        return [choices.value for choices in cls]


class IDatabaseFactory(Protocol):
    @abstractmethod
    def create_redis_mongo_database(self):
        ...  # pragma no cover

    @abstractmethod
    def create_ram_only_database(self):
        ...  # pragma no cover

    @abstractmethod
    def create_test_database(self):
        ...  # pragma no cover


class DatabaseFactory:
    def __init__(self, database_name: DatabaseType) -> None:
        self.name = database_name

    def create_redis_mongo_database(self):
        return RedisMongoDB

    def create_ram_only_database(self):
        return InMemoryDB

    def create_test_database(self):
        return DatabaseMock


def database_factory(factory: IDatabaseFactory) -> IAtomDB:
    redis_mongo_database = factory.create_redis_mongo_database()
    ram_only_database = factory.create_ram_only_database()
    test_database = factory.create_test_database()

    if factory.name == DatabaseType.REDIS_MONGO.value:
        return redis_mongo_database()

    if factory.name == DatabaseType.RAM_ONLY.value:
        return ram_only_database()

    if factory.name == DatabaseType.TEST.value:
        return test_database()
