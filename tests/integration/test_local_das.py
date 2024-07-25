import pytest

from hyperon_das import DistributedAtomSpace
from tests.integration.remote_das_info import remote_das_host, remote_das_port
from tests.utils import load_animals_base

from .helpers import _db_down, _db_up, cleanup, mongo_port, redis_port


class TestLocalDASRedisMongo:
    @pytest.fixture(scope="session", autouse=True)
    def _cleanup(self, request):
        return cleanup(request)

    def test_queries(self):
        _db_up()
        das = DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )
        assert das.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}
        load_animals_base(das)
        assert das.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}
        das.commit_changes()
        assert das.count_atoms() == {'atom_count': 40, 'node_count': 0, 'link_count': 0}
        assert das.count_atoms({'precise': True}) == {
            'atom_count': 40,
            'node_count': 14,
            'link_count': 26,
        }

        _db_down()

    def test_add_atom_persistence(self):
        _db_up()
        das = DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )
        assert das.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}
        load_animals_base(das)
        assert das.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}
        das.commit_changes()
        assert das.count_atoms() == {'atom_count': 40, 'node_count': 0, 'link_count': 0}
        assert das.count_atoms({'context': 'remote'}) == {
            'atom_count': 0,
            'node_count': 0,
            'link_count': 0,
        }

        das.add_node({"type": "Concept", "name": "dog"})
        das.add_link(
            {
                "type": "Inheritance",
                "targets": [
                    {"type": "Concept", "name": "dog"},
                    {"type": "Concept", "name": "mammal"},
                ],
            }
        )
        das.commit_changes()
        assert das.count_atoms({'precise': True}) == {
            'atom_count': 42,
            'node_count': 15,
            'link_count': 27,
        }

        das2 = DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )
        assert das2.count_atoms({'precise': True}) == {
            'atom_count': 42,
            'node_count': 15,
            'link_count': 27,
        }

        _db_down()

    def test_fetch_atoms(self):
        _db_up()
        das = DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )
        assert das.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}
        das.fetch(
            query={
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "Inheritance"},
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "node", "type": "Symbol", "name": '"mammal"'},
                ],
            },
            host=remote_das_host,
            port=remote_das_port,
        )
        assert das.count_atoms({'precise': True}) == {
            'atom_count': 10,
            'node_count': 6,
            'link_count': 4,
        }
        _db_down()


class TestLocalDASRamOnly:
    def test_fetch_atoms_local_das_ram_only(self):
        das = DistributedAtomSpace()
        assert das.count_atoms() == {'atom_count': 0, 'node_count': 0, 'link_count': 0}
        das.fetch(
            query={
                "atom_type": "link",
                "type": "Expression",
                "targets": [
                    {"atom_type": "node", "type": "Symbol", "name": "Inheritance"},
                    {"atom_type": "variable", "name": "v1"},
                    {"atom_type": "node", "type": "Symbol", "name": '"mammal"'},
                ],
            },
            host=remote_das_host,
            port=remote_das_port,
        )
        assert das.count_atoms({'precise': True}) == {
            'atom_count': 10,
            'node_count': 6,
            'link_count': 4,
        }
