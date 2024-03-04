import random
import string

from hyperon_das import DistributedAtomSpace


def up_knowledge_base_animals(das: DistributedAtomSpace) -> None:
    all_links = [
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'chimp'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'chimp'},
                {'type': 'Concept', 'name': 'monkey'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'snake'},
                {'type': 'Concept', 'name': 'earthworm'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'rhino'},
                {'type': 'Concept', 'name': 'triceratops'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'snake'},
                {'type': 'Concept', 'name': 'vine'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'ent'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'human'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'monkey'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'chimp'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'mammal'},
                {'type': 'Concept', 'name': 'animal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'reptile'},
                {'type': 'Concept', 'name': 'animal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'snake'},
                {'type': 'Concept', 'name': 'reptile'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'dinosaur'},
                {'type': 'Concept', 'name': 'reptile'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'triceratops'},
                {'type': 'Concept', 'name': 'dinosaur'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'earthworm'},
                {'type': 'Concept', 'name': 'animal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'rhino'},
                {'type': 'Concept', 'name': 'mammal'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'vine'},
                {'type': 'Concept', 'name': 'plant'},
            ],
        },
        {
            'type': 'Inheritance',
            'targets': [
                {'type': 'Concept', 'name': 'ent'},
                {'type': 'Concept', 'name': 'plant'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'monkey'},
                {'type': 'Concept', 'name': 'human'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'chimp'},
                {'type': 'Concept', 'name': 'human'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'monkey'},
                {'type': 'Concept', 'name': 'chimp'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'earthworm'},
                {'type': 'Concept', 'name': 'snake'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'triceratops'},
                {'type': 'Concept', 'name': 'rhino'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'vine'},
                {'type': 'Concept', 'name': 'snake'},
            ],
        },
        {
            'type': 'Similarity',
            'targets': [
                {'type': 'Concept', 'name': 'ent'},
                {'type': 'Concept', 'name': 'human'},
            ],
        },
    ]

    for link in all_links:
        das.add_link(link)


def up_n_links_in_database(das: DistributedAtomSpace, n: int) -> None:
    for name in [
        ''.join([random.choice(string.ascii_lowercase) for c in range(5)]) for i in range(n)
    ]:
        das.add_link(
            {
                'type': 'Inheritance',
                'targets': [
                    {'type': 'Concept', 'name': 'human'},
                    {'type': 'Concept', 'name': name},
                ],
            }
        )
