import json
import os
from typing import Type

from hyperon_das_atomdb import AtomDB
from hyperon_das_atomdb.database import LinkT, NodeT

DistributedAtomSpace = Type["DistributedAtomSpace"]


class AnimalBaseHandlesCollection:
    human = AtomDB.node_handle("Concept", "human")
    monkey = AtomDB.node_handle("Concept", "monkey")
    chimp = AtomDB.node_handle("Concept", "chimp")
    mammal = AtomDB.node_handle("Concept", "mammal")
    ent = AtomDB.node_handle("Concept", "ent")
    animal = AtomDB.node_handle("Concept", "animal")
    reptile = AtomDB.node_handle("Concept", "reptile")
    dinosaur = AtomDB.node_handle("Concept", "dinosaur")
    triceratops = AtomDB.node_handle("Concept", "triceratops")
    rhino = AtomDB.node_handle("Concept", "rhino")
    earthworm = AtomDB.node_handle("Concept", "earthworm")
    snake = AtomDB.node_handle("Concept", "snake")
    vine = AtomDB.node_handle("Concept", "vine")
    plant = AtomDB.node_handle("Concept", "plant")

    similarity_human_monkey = AtomDB.link_handle("Similarity", [human, monkey])
    similarity_human_chimp = AtomDB.link_handle("Similarity", [human, chimp])
    similarity_chimp_monkey = AtomDB.link_handle("Similarity", [chimp, monkey])
    similarity_snake_earthworm = AtomDB.link_handle("Similarity", [snake, earthworm])
    similarity_rhino_triceratops = AtomDB.link_handle("Similarity", [rhino, triceratops])
    similarity_snake_vine = AtomDB.link_handle("Similarity", [snake, vine])
    similarity_human_ent = AtomDB.link_handle("Similarity", [human, ent])
    inheritance_human_mammal = AtomDB.link_handle("Inheritance", [human, mammal])
    inheritance_monkey_mammal = AtomDB.link_handle("Inheritance", [monkey, mammal])
    inheritance_chimp_mammal = AtomDB.link_handle("Inheritance", [chimp, mammal])
    inheritance_mammal_animal = AtomDB.link_handle("Inheritance", [mammal, animal])
    inheritance_reptile_animal = AtomDB.link_handle("Inheritance", [reptile, animal])
    inheritance_snake_reptile = AtomDB.link_handle("Inheritance", [snake, reptile])
    inheritance_dinosaur_reptile = AtomDB.link_handle("Inheritance", [dinosaur, reptile])
    inheritance_triceratops_dinosaur = AtomDB.link_handle("Inheritance", [triceratops, dinosaur])
    inheritance_earthworm_animal = AtomDB.link_handle("Inheritance", [earthworm, animal])
    inheritance_rhino_mammal = AtomDB.link_handle("Inheritance", [rhino, mammal])
    inheritance_vine_plant = AtomDB.link_handle("Inheritance", [vine, plant])
    inheritance_ent_plant = AtomDB.link_handle("Inheritance", [ent, plant])
    similarity_monkey_human = AtomDB.link_handle("Similarity", [monkey, human])
    similarity_chimp_human = AtomDB.link_handle("Similarity", [chimp, human])
    similarity_monkey_chimp = AtomDB.link_handle("Similarity", [monkey, chimp])
    similarity_earthworm_snake = AtomDB.link_handle("Similarity", [earthworm, snake])
    similarity_triceratops_rhino = AtomDB.link_handle("Similarity", [triceratops, rhino])
    similarity_vine_snake = AtomDB.link_handle("Similarity", [vine, snake])
    similarity_ent_human = AtomDB.link_handle("Similarity", [ent, human])

    def _get_nodes(self):
        for att in dir(self):
            if "_" in att:
                continue
            yield {"type": "Concept", "name": att}

    def _get_links(self):
        for att in dir(self):
            if att.startswith("_"):
                continue
            values = att.split("_")
            if len(values) > 1:
                yield {"type": values[0].capitalize(), "targets": [
                    {"type": "Concept", "name": n} for n in values[1:]
                ]}


animal_base_handles = AnimalBaseHandlesCollection()


def load_animals_base(das: DistributedAtomSpace) -> None:
    for node in animal_base_handles._get_nodes():
        das.add_node(NodeT(**node))

    for link in animal_base_handles._get_links():
        link["targets"] = [NodeT(**node) for node in link["targets"]]
        das.add_link(LinkT(**link))




# def test_load():
#     class das:
#         def __init__(self):
#             self.nodes = []
#             self.links = []
#
#         def add_node(self, value):
#             self.nodes.append(value.to_dict())
#
#         def add_link(self, value):
#             self.links.append(value.to_dict())
#
#     old = das()
#     new = das()
#     load_animals_base2(new)
#     load_animals_base(old)
#     assert len(old.nodes) == len(new.nodes)
#     assert len(old.links) == len(new.links)
#     assert sorted(old.nodes, key=lambda x: x["name"]) == sorted(new.nodes, key=lambda x: x["name"])
#
#     def kkey(x):
#         targets = [node["name"] for node in x["targets_documents"]]
#         key = "_".join(
#             [x["named_type"], *targets]).lower()
#         return key
#
#     assert sorted(old.links, key=kkey) == sorted(new.links, key=kkey)
