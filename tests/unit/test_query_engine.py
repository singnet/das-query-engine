import copy
import itertools
from unittest import mock

import pytest
from hyperon_das_atomdb.database import AtomT, LinkT, NodeT
from pytest import FixtureRequest

from hyperon_das.das import DistributedAtomSpace
from hyperon_das.link_filters import LinkFilter, LinkFilterType
from tests.unit.fixtures import (  # noqa: F811,F401
    das_local_ram_engine,
    das_local_redis_mongo_engine,
    das_remote_ram_engine,
)
from tests.utils import animal_base_handles, load_animals_base, query_constructor


def add_atom(atom, das, engine):
    if "targets" in atom:
        atom_cp = copy.deepcopy(atom)
        atom_cp["targets"] = [NodeT(**n) for n in atom["targets"]]
        result = das.add_link(LinkT(**atom_cp))
    else:
        result = das.add_node(NodeT(**atom))

    if engine == "das_local_redis_mongo_engine":
        das.commit_changes()
    return result


@pytest.mark.parametrize("engine", ["das_local_ram_engine", "das_local_redis_mongo_engine"])
class TestQueryEngine:
    # scenarios = [local_ram_scenario, local_redis_mongo_scenario]

    def create_links(self, pair_mtx, link_type="Test", node_type="Test"):
        return [
            {
                "type": link_type,
                "targets": [{"type": node_type, "name": p[0]}, {"type": node_type, "name": p[1]}],
            }
            for p in itertools.product(*pair_mtx)
            if p[0] != p[1]
        ]

    def load_database_animals(self, das, engine):
        load_animals_base(das)
        if engine == "das_local_redis_mongo_engine":
            das.commit_changes()

    @pytest.mark.parametrize(
        "atom,handle",
        [
            ({"type": "Test", "name": "A"}, "815212e3d7ac246e70c1744d14a8c402"),
            (
                {
                    "type": "Test",
                    "targets": [{"type": "Test", "name": "A"}, {"type": "Test", "name": "B"}],
                },
                "d9ea058d9e000a0ffca1cc444a13e771",
            ),
        ],
    )
    def test_get_atom(self, engine, atom, handle, request: FixtureRequest):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        node = add_atom(atom, das, engine)
        atom = das.get_atom(node.handle)
        assert atom
        assert isinstance(atom, AtomT)
        assert atom.handle == handle

    @pytest.mark.parametrize(
        "atoms,expected",
        [
            ([{"type": "Test", "name": "A"}], 1),
            ([{"type": "Test", "name": "A"}, {"type": "Test", "name": "B"}], 2),
            (
                [
                    {"type": "Test", "name": "A"},
                    {
                        "type": "Test",
                        "targets": [{"type": "Test", "name": "A"}, {"type": "Test", "name": "B"}],
                    },
                ],
                2,
            ),
        ],
    )
    def test_get_atoms(self, engine, atoms, expected, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        handle_list = [add_atom(atom, das, engine).handle for atom in atoms]
        result_list = [atom.handle for atom in das.get_atoms(handle_list)]
        assert len(result_list) == expected
        assert sorted(result_list) == sorted(handle_list)

    @pytest.mark.parametrize(
        "link_filter,expected",
        [
            (
                LinkFilter(
                    filter_type=LinkFilterType.TARGETS,
                    toplevel_only=False,
                    link_type="Test",
                    target_types=["Test"],
                    targets=["*", "*"],
                ),
                12,
            ),
        ],
    )
    def test_get_links(self, engine, link_filter, expected, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        links = self.create_links([['A', 'B', 'C', 'D'], ['A', 'B', 'C', 'D']])
        for link in links:
            add_atom(link, das, engine)
        result_links = das.get_links(link_filter)
        assert len(result_links) == expected
        assert all([isinstance(link, LinkT) for link in result_links])

    @pytest.mark.parametrize(
        "link_filter,expected",
        [
            (
                LinkFilter(
                    filter_type=LinkFilterType.TARGETS,
                    toplevel_only=False,
                    link_type="Test",
                    target_types=["Test"],
                    targets=["*", "*"],
                ),
                12,
            ),
        ],
    )
    def test_get_link_handles(self, engine, link_filter, expected, request):
        # def get_link_handles(self, link_filter: LinkFilter) -> HandleSetT:
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        links = self.create_links([['A', 'B', 'C', 'D'], ['A', 'B', 'C', 'D']])
        for link in links:
            add_atom(link, das, engine)
        result_links = das.get_link_handles(link_filter)
        assert len(result_links) == expected
        assert all([isinstance(link, str) for link in result_links])

    @pytest.mark.parametrize(
        "handle,mtx,expected",
        [
            (
                "815212e3d7ac246e70c1744d14a8c402",
                [['A', 'B', 'C', 'D'], ['A', 'B', 'C', 'D']],
                6,
            ),  # A
            ("cc506de53938e1132d6cfb4746c37e13", [['B', 'C', 'D'], ['A', 'C']], 2),  # B
            ("c78b709c472f5476546e27d88e763fa5", [['A', 'B', 'C', 'D'], ['A', 'B', 'D']], 3),  # C
            ("481ecdd572fa530f3c06410885c957e5", [['A', 'B', 'C'], ['A', 'B', 'C']], 0),  # D
        ],
    )
    def test_get_incoming_links(self, engine, handle, mtx, expected, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        links = self.create_links(mtx)
        for link in links:
            add_atom(link, das, engine)
        result_links = das.get_incoming_links(handle)
        assert len(result_links) == expected
        assert all([isinstance(link, LinkT) for link in result_links])

    @pytest.mark.parametrize(
        "query,expected",
        [
            (query_constructor(("link", "Test", [("variable", "v1"), ("node", "Test", "E")])), 3),
            (
                query_constructor(("link", "Test", [("node", "Test", "A"), ("node", "Test", "E")])),
                1,
            ),
            (
                query_constructor(("link", "Test", [("node", "Test", "A"), ("node", "Test", "A")])),
                0,
            ),
            (query_constructor(("node", "Test", "A")), 1),
        ],
    )
    def test_query(self, engine, query, expected, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        links = self.create_links([['A', 'B', 'C'], ['D', 'E', 'F']])
        links_t = [add_atom(link, das, engine) for link in links]
        assert isinstance(links_t, list)
        query_result = list(das.query(query))
        assert len(query_result) == expected

    @pytest.mark.parametrize(
        "query,expected",
        [
            (
                query_constructor(
                    ("link", "Inheritance", [("variable", "v1"), ("variable", "v2")])
                ),
                12,
            ),
            (
                [
                    query_constructor(
                        ("link", "Inheritance", [("variable", "v1"), ("variable", "v2")])
                    ),
                    query_constructor(
                        ("link", "Inheritance", [("variable", "v2"), ("variable", "v3")])
                    ),
                ],
                7,
            ),
        ],
    )
    def test_query_loaded(self, engine, query, expected, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        self.load_database_animals(das, engine)
        resp = das.query(query, {"no_iterator": True})
        assert len(resp) == expected

    @pytest.mark.parametrize(
        "index_params,query_params,expected",
        [
            (
                # index_params
                {"atom_type": "node", "fields": ["value"], "named_type": "Test"},
                # query_params / custom attributes
                {"value": 3},
                # expected
                "815212e3d7ac246e70c1744d14a8c402",
            ),
            (
                {"atom_type": "node", "fields": ["value", "strength"], "named_type": "Test"},
                {"value": 3, "strength": 5},
                "815212e3d7ac246e70c1744d14a8c402",
            ),
            (
                {"atom_type": "link", "fields": ["value"], "named_type": "Test3"},
                {"value": 3},
                "b3f66ec1535de7702c38e94408fa4a17",
            ),
            (
                {"atom_type": "link", "fields": ["value"], "named_type": "Test2"},
                {"value": 3, "round": 2},
                "c454552d52d55d3ef56408742887362b",
            ),
            (
                {"atom_type": "link", "fields": ["value", "round"], "named_type": "Test"},
                {"value": 3, "round": 2},
                "0cbc6611f5540bd0809a388dc95a615b",
            ),
        ],
    )
    def test_custom_query(self, engine, index_params, query_params, expected, request):
        # def custom_query(self, index_id: str, query: Query, **kwargs) -> Union[Iterator, List[AtomT]]:
        if engine == "das_local_ram_engine":
            pytest.skip("Not implemented")
        pytest.skip("Not working, update atom-db")
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        if index_params.get("atom_type") == "link":
            link = self.create_links([['A'], ['B']], index_params["named_type"])[0]
            link.update({"custom_attributes": query_params})
            atom = add_atom(link, das, engine)
        else:
            node = {
                "type": index_params["named_type"],
                "name": "A",
                "custom_attributes": query_params,
            }
            atom = add_atom(node, das, engine)
        index_id = das.create_field_index(
            atom_type=index_params.get("atom_type"),
            fields=index_params.get("fields"),
            named_type=index_params.get("named_type"),
        )
        cursor, atoms = das.custom_query(index_id, query_params)
        assert isinstance(cursor, int)
        assert isinstance(atoms, list)
        assert cursor == 0
        assert len(atoms) == 1
        handles = [atom.handle for atom in atoms]
        assert atom.handle in handles
        assert expected in handles
        assert all(isinstance(a, AtomT) for a in atoms)
        # pass

    @pytest.mark.parametrize(
        "params",
        [
            ({}),
            ({"precise": True}),
            ({"precise": False}),
        ],
    )
    def test_count_atoms(self, engine, params, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        links = self.create_links([['A', 'B', 'C'], ['D', 'E', 'F']])
        links_t = [add_atom(link, das, engine) for link in links]
        atoms_count = das.count_atoms(params)
        assert atoms_count
        assert isinstance(atoms_count, dict)
        assert isinstance(atoms_count["atom_count"], int)
        assert atoms_count["atom_count"] == 15
        if params.get("precise", False):
            assert isinstance(atoms_count["node_count"], int)
            assert isinstance(atoms_count["link_count"], int)
            assert atoms_count["node_count"] == 6
            assert atoms_count["link_count"] == len(links_t)

    @pytest.mark.parametrize(
        "handle", [animal_base_handles.animal, animal_base_handles.inheritance_mammal_animal]
    )
    def test_get_traversal_cursor_get(self, engine, handle, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        self.load_database_animals(das, engine)
        cursor = das.get_traversal_cursor(handle)
        current_cursor = cursor.get()
        assert current_cursor.handle == handle

    @pytest.mark.parametrize(
        "handle", [animal_base_handles.animal, animal_base_handles.inheritance_mammal_animal]
    )
    @pytest.mark.skip("Error 'Node' object is not subscriptable ")
    def test_get_traversal_cursor_links(self, engine, handle, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        self.load_database_animals(das, engine)
        cursor = das.get_traversal_cursor(handle)
        current_cursor = cursor.get_links()
        assert current_cursor

    @pytest.mark.parametrize(
        "handle", [animal_base_handles.animal, animal_base_handles.inheritance_mammal_animal]
    )
    @pytest.mark.skip("Error 'Node' object is not subscriptable ")
    def test_get_traversal_cursor_neighbors(self, engine, handle, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        self.load_database_animals(das, engine)
        cursor = das.get_traversal_cursor(handle)
        current_cursor = cursor.get_neighbors()
        assert current_cursor

    @pytest.mark.parametrize(
        "index_params,expected",
        [
            (
                {"atom_type": "node", "fields": ["value"], "named_type": "Test"},
                "node_cd369ff5bf310db2a8a384e5ea1a9312",
            ),
            (
                {"atom_type": "node", "fields": ["value", "strength"], "named_type": "Test"},
                "node_bd9239a91659891e0211c7c83661693a",
            ),
            (
                {"atom_type": "link", "fields": ["value"], "named_type": "Test3"},
                "link_ceebe960507ac415a96189c08710e32e",
            ),
            (
                {"atom_type": "link", "fields": ["value"], "named_type": "Test2"},
                "link_2402202c9846034470f4a139d2cb73aa",
            ),
            (
                {"atom_type": "link", "fields": ["value", "round"], "named_type": "Test"},
                "link_8985e514f249e287a04918d7790416fc",
            ),
        ],
    )
    def test_create_field_index(self, engine, index_params, expected, request):
        if engine == "das_local_ram_engine":
            pytest.skip("Not implemented")
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        index_id = das.create_field_index(
            atom_type=index_params.get("atom_type"),
            fields=index_params.get("fields"),
            named_type=index_params.get("named_type"),
        )
        assert index_id
        assert index_id == expected

    @pytest.mark.skip("Not working")
    def test_fetch(self, engine, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        # links = [LinkT(**{"type": l["type"], "targets": [NodeT(**n) for n in l["targets"]]}) for l in animal_base_handles._get_links()]
        # nodes = [NodeT(**n) for n in animal_base_handles._get_nodes()]
        # atoms = links + nodes
        links = list(animal_base_handles._get_links())
        nodes = list(animal_base_handles._get_nodes())
        atoms = links + nodes
        with mock.patch('hyperon_das.client.FunctionsClient.fetch', return_value=atoms), mock.patch(
            'hyperon_das.utils.check_server_connection', return_value=(200, 'OK')
        ):
            das.fetch(host="0.0.0.0", port=1)
            if engine == "das_local_redis_mongo_engine":
                das.commit_changes()
        count = das.count_atoms({"precise": True})
        assert count == {
            "atoms_count": 40,
            "node_count": 14,
            "link_count": 26,
        }

    def test_create_context(self, engine, request):
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        context = das.create_context("blah", {})
        assert context.name == "blah"

    @pytest.mark.parametrize(
        "query",
        [
            {'name': 'animal'},
            # {'custom_attributes.name': 'human'}
        ],
    )
    def test_get_atoms_by_field(self, engine, query, request):
        # def get_atoms_by_field(self, query: Query) -> HandleListT:
        if engine == "das_local_ram_engine":
            pytest.skip("Not implemented")
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        add_atom({"type": "Concept", "name": query["name"]}, das, engine)
        atom_text_field = das.get_atoms_by_field(query)
        assert atom_text_field

    def test_get_atoms_by_text_field(self, engine, request):
        if engine == "das_local_ram_engine":
            pytest.skip("Not implemented")
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        add_atom({"type": "Concept", "name": "human"}, das, engine)
        atom_text_field = das.get_atoms_by_text_field(text_value='human', field='name')
        assert atom_text_field

    def test_get_node_by_name_starting_with(self, engine, request):
        if engine == "das_local_ram_engine":
            pytest.skip("Not implemented")
        das: DistributedAtomSpace = request.getfixturevalue(engine)
        add_atom({"type": "Concept", "name": "mammal"}, das, engine)
        atoms = das.get_node_by_name_starting_with('Concept', 'mam')
        assert atoms
