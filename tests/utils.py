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


animal_base_handles = AnimalBaseHandlesCollection()


def load_animals_base(das: DistributedAtomSpace) -> None:
    das.add_node(NodeT(**{"type": "Concept", "name": "human"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "monkey"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "chimp"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "mammal"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "reptile"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "snake"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "dinosaur"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "triceratops"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "earthworm"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "rhino"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "vine"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "ent"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "animal"}))
    das.add_node(NodeT(**{"type": "Concept", "name": "plant"}))

    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "human"}),
                    NodeT(**{"type": "Concept", "name": "monkey"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "human"}),
                    NodeT(**{"type": "Concept", "name": "chimp"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "chimp"}),
                    NodeT(**{"type": "Concept", "name": "monkey"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "snake"}),
                    NodeT(**{"type": "Concept", "name": "earthworm"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "rhino"}),
                    NodeT(**{"type": "Concept", "name": "triceratops"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "snake"}),
                    NodeT(**{"type": "Concept", "name": "vine"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "human"}),
                    NodeT(**{"type": "Concept", "name": "ent"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "monkey"}),
                    NodeT(**{"type": "Concept", "name": "human"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "chimp"}),
                    NodeT(**{"type": "Concept", "name": "human"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "monkey"}),
                    NodeT(**{"type": "Concept", "name": "chimp"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "earthworm"}),
                    NodeT(**{"type": "Concept", "name": "snake"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "triceratops"}),
                    NodeT(**{"type": "Concept", "name": "rhino"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "vine"}),
                    NodeT(**{"type": "Concept", "name": "snake"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Similarity",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "ent"}),
                    NodeT(**{"type": "Concept", "name": "human"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "human"}),
                    NodeT(**{"type": "Concept", "name": "mammal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "monkey"}),
                    NodeT(**{"type": "Concept", "name": "mammal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "chimp"}),
                    NodeT(**{"type": "Concept", "name": "mammal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "mammal"}),
                    NodeT(**{"type": "Concept", "name": "animal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "reptile"}),
                    NodeT(**{"type": "Concept", "name": "animal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "snake"}),
                    NodeT(**{"type": "Concept", "name": "reptile"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "dinosaur"}),
                    NodeT(**{"type": "Concept", "name": "reptile"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "triceratops"}),
                    NodeT(**{"type": "Concept", "name": "dinosaur"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "earthworm"}),
                    NodeT(**{"type": "Concept", "name": "animal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "rhino"}),
                    NodeT(**{"type": "Concept", "name": "mammal"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "vine"}),
                    NodeT(**{"type": "Concept", "name": "plant"}),
                ],
            }
        )
    )
    das.add_link(
        LinkT(
            **{
                "type": "Inheritance",
                "targets": [
                    NodeT(**{"type": "Concept", "name": "ent"}),
                    NodeT(**{"type": "Concept", "name": "plant"}),
                ],
            }
        )
    )
