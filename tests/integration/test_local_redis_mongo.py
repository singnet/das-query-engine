from unittest import mock

import pytest

from hyperon_das import DistributedAtomSpace
from hyperon_das.index import Index
from tests.utils import load_animals_base

from .helpers import _db_down, _db_up, cleanup, load_metta_animals_base, mongo_port, redis_port


class TestLocalDASRedisMongo:
    @pytest.fixture(scope="session", autouse=True)
    def _cleanup(self, request):
        return cleanup(request)

    def _test_queries(self):
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
        assert das.count_atoms() == (0, 0)
        load_animals_base(das)
        assert das.count_atoms() == (0, 0)
        das.commit_changes()
        assert das.count_atoms() == (14, 26)

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
        assert das.count_atoms() == (0, 0)
        load_animals_base(das)
        assert das.count_atoms() == (0, 0)
        das.commit_changes()
        assert das.count_atoms() == (14, 26)

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
        assert das.count_atoms() == (15, 27)

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
        assert das2.count_atoms() == (15, 27)

        _db_down()

    def test_create_field_index(self):
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

        load_metta_animals_base(das)
        das.add_link(
            {
                "type": "Expression",
                "targets": [
                    {"type": "Symbol", "name": 'Similarity', "is_literal": False},
                    {"type": "Symbol", "name": '"human"', "is_literal": True},
                    {"type": "Symbol", "name": '"monkey"', "is_literal": True},
                ],
                "score": 0.5,
            }
        )
        das.commit_changes()

        def create_index(atom_type, field, **kwargs):
            collection_name, indexes = Index(atom_type, field, **kwargs).create()
            if collection_name == 'node':
                collections = [das.backend.mongo_nodes_collection]
            elif collection_name == 'link':
                collections = das.backend.mongo_link_collection.values()
            index_list = indexes[0]
            index_options = indexes[1]
            for collection in collections:
                idx_name = collection.create_index(index_list, **index_options)
            return idx_name

        link_collections = list(das.backend.mongo_link_collection.values())

        links_2 = link_collections[0]
        links_1 = link_collections[1]
        links_n = link_collections[2]

        response = links_n.find({'named_type': 'Expression', 'score': 0.5}).explain()

        with pytest.raises(KeyError):
            response['queryPlanner']['winningPlan']['inputStage']['indexName']

        # Create the Index
        with mock.patch(
            'hyperon_das.DistributedAtomSpace.create_partial_field_index', side_effect=create_index
        ):
            my_index = das.create_field_index(atom_type='link', field='score', type='Expression')

        links_2_index_names = [idx.get('name') for idx in links_2.list_indexes()]
        links_1_index_names = [idx.get('name') for idx in links_1.list_indexes()]
        links_n_index_names = [idx.get('name') for idx in links_n.list_indexes()]

        assert my_index in links_2_index_names
        assert my_index in links_1_index_names
        assert my_index in links_n_index_names

        # Using the index
        response = links_n.find({'named_type': 'Expression', 'score': 0.5}).explain()

        assert my_index == response['queryPlanner']['winningPlan']['inputStage']['indexName']
        _db_down()
