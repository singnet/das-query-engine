from abc import abstractmethod
from enum import Enum
from typing import Optional, Protocol

from hyperon_das_atomdb import IAtomDB
from hyperon_das_atomdb.adapters import InMemoryDB, RedisMongoDB, ServerDB

from hyperon_das.constants import DatabaseType


class IDatabaseFactory(Protocol):
    @abstractmethod
    def create_redis_mongo_database(self):
        ...  # pragma no cover

    @abstractmethod
    def create_ram_only_database(self):
        ...  # pragma no cover

    @abstractmethod
    def create_server_database(self):
        ...  # pragma no cover


class DatabaseFactory:
    def __init__(self, database_name: DatabaseType) -> None:
        self.name = database_name

    def create_redis_mongo_database(self):
        return RedisMongoDB

    def create_ram_only_database(self):
        return InMemoryDB

    def create_server_database(self):
        return ServerDB


def database_factory(
    factory: IDatabaseFactory,
    host: Optional[str] = None,
    port: Optional[str] = None,
) -> IAtomDB:
    redis_mongo_database = factory.create_redis_mongo_database()
    ram_only_database = factory.create_ram_only_database()
    server_database = factory.create_server_database()

    if factory.name == DatabaseType.REDIS_MONGO.value:
        return redis_mongo_database()

    if factory.name == DatabaseType.RAM_ONLY.value:
        return ram_only_database()

    if factory.name == DatabaseType.SERVER.value:
        return server_database(host=host, port=port)
