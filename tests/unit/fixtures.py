import hyperon_das_atomdb.adapters
from pytest import fixture
from unittest import mock
from hyperon_das.das import DistributedAtomSpace, LocalQueryEngine, RemoteQueryEngine
from tests.unit.mock import MockRedis
import mongomock


def mongo_mock():
    return mongomock.MongoClient().db


def redis_mock():
    return MockRedis()


@fixture
def das_local_ram_engine():
    das = DistributedAtomSpace(atomdb="ram", query_engine="local")
    yield das


@fixture
def das_remote_ram_engine():
    das = DistributedAtomSpace(atomdb="ram", query_engine="remote")
    yield das


@fixture
def das_remote_redis_mongo_engine():
    das = DistributedAtomSpace(atomdb="redis_mongo", query_engine="remote")
    yield das


@fixture
def das_local_redis_mongo_engine():
    mongo_db = mongo_mock()
    redis_db = redis_mock()
    with mock.patch(
            "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_mongo_db",
            return_value=mongo_db,
    ), mock.patch(
        "hyperon_das_atomdb.adapters.redis_mongo_db.RedisMongoDB._connection_redis",
        return_value=redis_db,
    ):
        yield DistributedAtomSpace(atomdb="redis_mongo", query_engine="local")
    print("A")
