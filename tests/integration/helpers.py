import os
import subprocess
from typing import Type

from hyperon_das_atomdb import AtomDB

DistributedAtomSpace = Type['DistributedAtomSpace']

redis_port = "15926"
mongo_port = "15927"
scripts_path = "./tests/integration/scripts/"
devnull = open(os.devnull, 'w')

DAS_MONGODB_HOSTNAME = os.environ.get("DAS_MONGODB_HOSTNAME")
DAS_MONGODB_PORT = os.environ.get("DAS_MONGODB_PORT")
DAS_MONGODB_USERNAME = os.environ.get("DAS_MONGODB_USERNAME")
DAS_MONGODB_PASSWORD = os.environ.get("DAS_MONGODB_PASSWORD")
DAS_REDIS_HOSTNAME = os.environ.get("DAS_REDIS_HOSTNAME")
DAS_REDIS_PORT = os.environ.get("DAS_REDIS_PORT")
DAS_REDIS_USERNAME = os.environ.get("DAS_REDIS_USERNAME")
DAS_REDIS_PASSWORD = os.environ.get("DAS_REDIS_PASSWORD")
DAS_USE_REDIS_CLUSTER = os.environ.get("DAS_USE_REDIS_CLUSTER")
DAS_USE_REDIS_SSL = os.environ.get("DAS_USE_REDIS_SSL")

os.environ["DAS_MONGODB_HOSTNAME"] = "localhost"
os.environ["DAS_MONGODB_PORT"] = mongo_port
os.environ["DAS_MONGODB_USERNAME"] = "dbadmin"
os.environ["DAS_MONGODB_PASSWORD"] = "dassecret"
os.environ["DAS_REDIS_HOSTNAME"] = "localhost"
os.environ["DAS_REDIS_PORT"] = redis_port
os.environ["DAS_REDIS_USERNAME"] = ""
os.environ["DAS_REDIS_PASSWORD"] = ""
os.environ["DAS_USE_REDIS_CLUSTER"] = "false"
os.environ["DAS_USE_REDIS_SSL"] = "false"


def cleanup(request):
    def restore_environment():
        if DAS_MONGODB_HOSTNAME:
            os.environ["DAS_MONGODB_HOSTNAME"] = DAS_MONGODB_HOSTNAME
        if DAS_MONGODB_PORT:
            os.environ["DAS_MONGODB_PORT"] = DAS_MONGODB_PORT
        if DAS_MONGODB_USERNAME:
            os.environ["DAS_MONGODB_USERNAME"] = DAS_MONGODB_USERNAME
        if DAS_MONGODB_PASSWORD:
            os.environ["DAS_MONGODB_PASSWORD"] = DAS_MONGODB_PASSWORD
        if DAS_REDIS_HOSTNAME:
            os.environ["DAS_REDIS_HOSTNAME"] = DAS_REDIS_HOSTNAME
        if DAS_REDIS_PORT:
            os.environ["DAS_REDIS_PORT"] = DAS_REDIS_PORT
        if DAS_REDIS_USERNAME:
            os.environ["DAS_REDIS_USERNAME"] = DAS_REDIS_USERNAME
        if DAS_REDIS_PASSWORD:
            os.environ["DAS_REDIS_PASSWORD"] = DAS_REDIS_PASSWORD
        if DAS_USE_REDIS_CLUSTER:
            os.environ["DAS_USE_REDIS_CLUSTER"] = DAS_USE_REDIS_CLUSTER
        if DAS_USE_REDIS_SSL:
            os.environ["DAS_USE_REDIS_SSL"] = DAS_USE_REDIS_SSL

    def enforce_containers_removal():
        _db_down()

    request.addfinalizer(restore_environment)
    request.addfinalizer(enforce_containers_removal)


def _db_up():
    subprocess.call(
        ["bash", f"{scripts_path}/redis-up.sh", redis_port], stdout=devnull, stderr=devnull
    )
    subprocess.call(
        ["bash", f"{scripts_path}/mongo-up.sh", mongo_port], stdout=devnull, stderr=devnull
    )


def _db_down():
    subprocess.call(
        ["bash", f"{scripts_path}/redis-down.sh", redis_port], stdout=devnull, stderr=devnull
    )
    subprocess.call(
        ["bash", f"{scripts_path}/mongo-down.sh", mongo_port], stdout=devnull, stderr=devnull
    )


# fmt: off
def load_metta_animals_base(das: DistributedAtomSpace) -> None:
    das.add_node({"type": "Symbol", "name": 'Similarity', "is_literal": False})
    das.add_node({"type": "Symbol", "name": 'Inheritance', "is_literal": False})
    das.add_node({"type": "Symbol", "name": 'Symbol', "is_literal": False})
    das.add_node({"type": "Symbol", "name": 'Type', "is_literal": False})
    das.add_node({"type": "Symbol", "name": 'MettaType', "is_literal": False})
    das.add_node({"type": "Symbol", "name": 'Expression', "is_literal": False})
    das.add_node({"type": "Symbol", "name": 'Concept', "is_literal": False})
    das.add_node({"type": "Symbol", "name": '"human"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"monkey"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"chimp"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"mammal"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"reptile"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"snake"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"dinosaur"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"triceratops"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"earthworm"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"rhino"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"vine"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"ent"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"animal"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": '"plant"', "is_literal": True})
    das.add_node({"type": "Symbol", "name": ':', "is_literal": False})
    das.add_node({"type": "Symbol", "name": '<:', "is_literal": False})

    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"human"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"monkey"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"chimp"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"mammal"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"reptile"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"snake"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"dinosaur"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"triceratops"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"earthworm"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"rhino"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"vine"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"ent"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"animal"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": '"plant"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": 'Type', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": 'Concept', "is_literal": False}, {"type": "Symbol", "name": 'Type', "is_literal": False}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": ':', "is_literal": False}, {"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": 'Type', "is_literal": False}]})

    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"human"', "is_literal": True}, {"type": "Symbol", "name": '"monkey"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"human"', "is_literal": True}, {"type": "Symbol", "name": '"chimp"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"chimp"', "is_literal": True}, {"type": "Symbol", "name": '"monkey"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"snake"', "is_literal": True}, {"type": "Symbol", "name": '"earthworm"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"rhino"', "is_literal": True}, {"type": "Symbol", "name": '"triceratops"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"snake"', "is_literal": True}, {"type": "Symbol", "name": '"vine"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"human"', "is_literal": True}, {"type": "Symbol", "name": '"ent"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"monkey"', "is_literal": True}, {"type": "Symbol", "name": '"human"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"chimp"', "is_literal": True}, {"type": "Symbol", "name": '"human"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"monkey"', "is_literal": True}, {"type": "Symbol", "name": '"chimp"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"earthworm"', "is_literal": True}, {"type": "Symbol", "name": '"snake"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"triceratops"', "is_literal": True}, {"type": "Symbol", "name": '"rhino"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"vine"', "is_literal": True}, {"type": "Symbol", "name": '"snake"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": '"ent"', "is_literal": True}, {"type": "Symbol", "name": '"human"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"human"', "is_literal": True}, {"type": "Symbol", "name": '"mammal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"monkey"', "is_literal": True}, {"type": "Symbol", "name": '"mammal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"chimp"', "is_literal": True}, {"type": "Symbol", "name": '"mammal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"mammal"', "is_literal": True}, {"type": "Symbol", "name": '"animal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"reptile"', "is_literal": True}, {"type": "Symbol", "name": '"animal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"snake"', "is_literal": True}, {"type": "Symbol", "name": '"reptile"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"dinosaur"', "is_literal": True}, {"type": "Symbol", "name": '"reptile"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"triceratops"', "is_literal": True}, {"type": "Symbol", "name": '"dinosaur"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"earthworm"', "is_literal": True}, {"type": "Symbol", "name": '"animal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"rhino"', "is_literal": True}, {"type": "Symbol", "name": '"mammal"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"vine"', "is_literal": True}, {"type": "Symbol", "name": '"plant"', "is_literal": True}]})
    das.add_link({"type": "Expression", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": '"ent"', "is_literal": True}, {"type": "Symbol", "name": '"plant"', "is_literal": True}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": 'Similarity', "is_literal": False}, {"type": "Symbol", "name": 'Type', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": 'Concept', "is_literal": False}, {"type": "Symbol", "name": 'Type', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": 'Inheritance', "is_literal": False}, {"type": "Symbol", "name": 'Type', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"human"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"monkey"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"chimp"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"snake"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"earthworm"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"rhino"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"triceratops"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"vine"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"ent"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"mammal"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"animal"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"reptile"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"dinosaur"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})
    # das.add_link({"type": "MettaType", "targets": [{"type": "Symbol", "name": '"plant"', "is_literal": True}, {"type": "Symbol", "name": 'Concept', "is_literal": False}]})


class MettaAnimalBaseHandlesCollection:
    Concept = AtomDB.node_handle('Symbol', 'Concept')
    Similarity = AtomDB.node_handle('Symbol', 'Similarity')
    Inheritance = AtomDB.node_handle('Symbol', 'Inheritance')
    Symbol = AtomDB.node_handle('Symbol', 'Symbol')
    Type = AtomDB.node_handle('Symbol', 'Type')
    # MettaType = AtomDB.node_handle('Symbol', 'MettaType')
    Expression = AtomDB.node_handle('Symbol', 'Expression')
    human = AtomDB.node_handle('Symbol', '"human"')
    monkey = AtomDB.node_handle('Symbol', '"monkey"')
    chimp = AtomDB.node_handle('Symbol', '"chimp"')
    mammal = AtomDB.node_handle('Symbol', '"mammal"')
    ent = AtomDB.node_handle('Symbol', '"ent"')
    animal = AtomDB.node_handle('Symbol', '"animal"')
    reptile = AtomDB.node_handle('Symbol', '"reptile"')
    dinosaur = AtomDB.node_handle('Symbol', '"dinosaur"')
    triceratops = AtomDB.node_handle('Symbol', '"triceratops"')
    rhino = AtomDB.node_handle('Symbol', '"rhino"')
    earthworm = AtomDB.node_handle('Symbol', '"earthworm"')
    snake = AtomDB.node_handle('Symbol', '"snake"')
    vine = AtomDB.node_handle('Symbol', '"vine"')
    plant = AtomDB.node_handle('Symbol', '"plant"')
    typedef_mark = AtomDB.node_handle('Symbol', ':')
    typedef2_mark = AtomDB.node_handle('Symbol', '<:')

    node_handles = [ 
        Concept, Similarity, Inheritance, Symbol, Type, Expression, human, monkey, chimp,
        mammal, ent, animal, reptile, dinosaur, triceratops, rhino, earthworm, snake, vine,
        plant, typedef_mark, typedef2_mark]

    human_typedef = AtomDB.link_handle('Expression', [typedef_mark, human, Concept])
    monkey_typedef = AtomDB.link_handle('Expression', [typedef_mark, monkey, Concept])
    chimp_typedef = AtomDB.link_handle('Expression', [typedef_mark, chimp, Concept])
    snake_typedef = AtomDB.link_handle('Expression', [typedef_mark, snake, Concept])
    earthworm_typedef = AtomDB.link_handle('Expression', [typedef_mark, earthworm, Concept])
    rhino_typedef = AtomDB.link_handle('Expression', [typedef_mark, rhino, Concept])
    triceratops_typedef = AtomDB.link_handle('Expression', [typedef_mark, triceratops, Concept])
    vine_typedef = AtomDB.link_handle('Expression', [typedef_mark, vine, Concept])
    ent_typedef = AtomDB.link_handle('Expression', [typedef_mark, ent, Concept])
    mammal_typedef = AtomDB.link_handle('Expression', [typedef_mark, mammal, Concept])
    animal_typedef = AtomDB.link_handle('Expression', [typedef_mark, animal, Concept])
    reptile_typedef = AtomDB.link_handle('Expression', [typedef_mark, reptile, Concept])
    dinosaur_typedef = AtomDB.link_handle('Expression', [typedef_mark, dinosaur, Concept])
    plant_typedef = AtomDB.link_handle('Expression', [typedef_mark, plant, Concept])
    similarity_typedef = AtomDB.link_handle('Expression', [typedef_mark, Similarity, Type])
    inheritance_typedef = AtomDB.link_handle('Expression', [typedef_mark, Inheritance, Type])
    concept_typedef = AtomDB.link_handle('Expression', [typedef_mark, Concept, Type])

    similarity_human_monkey = AtomDB.link_handle('Expression', [Similarity, human, monkey])
    similarity_human_chimp = AtomDB.link_handle('Expression', [Similarity, human, chimp])
    similarity_chimp_monkey = AtomDB.link_handle('Expression', [Similarity, chimp, monkey])
    similarity_snake_earthworm = AtomDB.link_handle('Expression', [Similarity, snake, earthworm])
    similarity_rhino_triceratops = AtomDB.link_handle('Expression', [Similarity, rhino, triceratops])
    similarity_snake_vine = AtomDB.link_handle('Expression', [Similarity, snake, vine])
    similarity_human_ent = AtomDB.link_handle('Expression', [Similarity, human, ent])
    inheritance_human_mammal = AtomDB.link_handle('Expression', [Inheritance, human, mammal])
    inheritance_monkey_mammal = AtomDB.link_handle('Expression', [Inheritance, monkey, mammal])
    inheritance_chimp_mammal = AtomDB.link_handle('Expression', [Inheritance, chimp, mammal])
    inheritance_mammal_animal = AtomDB.link_handle('Expression', [Inheritance, mammal, animal])
    inheritance_reptile_animal = AtomDB.link_handle('Expression', [Inheritance, reptile, animal])
    inheritance_snake_reptile = AtomDB.link_handle('Expression', [Inheritance, snake, reptile])
    inheritance_dinosaur_reptile = AtomDB.link_handle('Expression', [Inheritance, dinosaur, reptile])
    inheritance_triceratops_dinosaur = AtomDB.link_handle('Expression', [Inheritance, triceratops, dinosaur])
    inheritance_earthworm_animal = AtomDB.link_handle('Expression', [Inheritance, earthworm, animal])
    inheritance_rhino_mammal = AtomDB.link_handle('Expression', [Inheritance, rhino, mammal])
    inheritance_vine_plant = AtomDB.link_handle('Expression', [Inheritance, vine, plant])
    inheritance_ent_plant = AtomDB.link_handle('Expression', [Inheritance, ent, plant])
    similarity_monkey_human = AtomDB.link_handle('Expression', [Similarity, monkey, human])
    similarity_chimp_human = AtomDB.link_handle('Expression', [Similarity, chimp, human])
    similarity_monkey_chimp = AtomDB.link_handle('Expression', [Similarity, monkey, chimp])
    similarity_earthworm_snake = AtomDB.link_handle('Expression', [Similarity, earthworm, snake])
    similarity_triceratops_rhino = AtomDB.link_handle('Expression', [Similarity, triceratops, rhino])
    similarity_vine_snake = AtomDB.link_handle('Expression', [Similarity, vine, snake])
    similarity_ent_human = AtomDB.link_handle('Expression', [Similarity, ent, human])

    link_handles = [
        human_typedef, monkey_typedef, chimp_typedef, snake_typedef, earthworm_typedef,
        rhino_typedef, triceratops_typedef, vine_typedef, ent_typedef, mammal_typedef,
        animal_typedef, reptile_typedef, dinosaur_typedef, plant_typedef, similarity_typedef,
        inheritance_typedef, concept_typedef, similarity_human_monkey, similarity_human_chimp,
        similarity_chimp_monkey, similarity_snake_earthworm, similarity_rhino_triceratops,
        similarity_snake_vine, similarity_human_ent, inheritance_human_mammal,
        inheritance_monkey_mammal, inheritance_chimp_mammal, inheritance_mammal_animal,
        inheritance_reptile_animal, inheritance_snake_reptile, inheritance_dinosaur_reptile,
        inheritance_triceratops_dinosaur, inheritance_earthworm_animal, inheritance_rhino_mammal,
        inheritance_vine_plant, inheritance_ent_plant, similarity_monkey_human,
        similarity_chimp_human, similarity_monkey_chimp, similarity_earthworm_snake,
        similarity_triceratops_rhino, similarity_vine_snake, similarity_ent_human]
# fmt: on


metta_animal_base_handles = MettaAnimalBaseHandlesCollection()
