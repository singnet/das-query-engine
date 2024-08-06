"""Test node and link generation and Hyperon DAS load performance."""
import random
import time
from random import randint
from typing import Any, Dict, List, Optional, Tuple

import pytest

from hyperon_das import DistributedAtomSpace
from tests.integration.helpers import _db_down, _db_up, mongo_port, redis_port


def measure(func):  # pylint: disable=missing-function-docstring
    def wrapper(*args, **kwargs):
        start = time.time()
        values = func(*args, **kwargs)
        end = time.time()
        TestPerformance.time = end - start
        print(f'Elapsed time: {TestPerformance.time}')
        return values

    return wrapper


class TestPerformance:  # pylint: disable=too-many-instance-attributes,too-many-arguments
    """Test node/link generation and Hyperon DAS load."""

    time = 0.0

    @pytest.fixture(autouse=True)
    def _initialize(
        self,
        node_range: str,
        word_range: str,
        letter_range: str,
        alphabet_range: str,
        word_link_percentage: float,
        letter_link_percentage: float,
        seed: Any,
    ) -> None:
        self.node_type = 'Concept'  # pylint: disable=attribute-defined-outside-init
        self.node_range = [  # pylint: disable=attribute-defined-outside-init
            int(i) for i in node_range.split('-')
        ]
        self.word_range = [  # pylint: disable=attribute-defined-outside-init
            int(i) for i in word_range.split('-')
        ]
        self.letter_range = [  # pylint: disable=attribute-defined-outside-init
            int(i) for i in letter_range.split('-')
        ]
        self.alphabet_range = [  # pylint: disable=attribute-defined-outside-init
            int(i) for i in alphabet_range.split('-')
        ]
        self.word_link_percentage = (  # pylint: disable=attribute-defined-outside-init
            word_link_percentage
        )
        self.letter_link_percentage = (  # pylint: disable=attribute-defined-outside-init
            letter_link_percentage
        )
        self.seed = seed  # pylint: disable=attribute-defined-outside-init

        if seed:
            random.seed(seed)

    @pytest.fixture(scope="class", autouse=True)
    def database(self):
        """Pytest fixture to manage database lifecycle."""
        _db_up()
        yield
        _db_down()

    @measure
    def generate_links_word(self, word_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Generate links from between all pair of nodes which share at least one common word in
        their names.
        Outputs a dict containing the link reference {node1->node2} as key and the number of
        common words as value.

        Args:
            word_dict (Dict[str, Any]): Dict of words

        Returns:
            Dict[str, Any]: Dict of links

        """
        links_dict_word: Dict[str, Any] = {}
        for v in word_dict.values():
            v = list(v)
            for i, _ in enumerate(v):
                for j in range(i + 1, len(v)):
                    if random.random() > self.word_link_percentage:
                        continue
                    key = f'{min(v[i], v[j])}->{max(v[i], v[j])}'
                    if key in links_dict_word:
                        links_dict_word[key] += 1
                    else:
                        links_dict_word[key] = 1
        return links_dict_word

    @measure
    def generate_nodes(
        self, das: Optional[DistributedAtomSpace] = None
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
        """
        Creates and add to Hyperon DAS nodes of type "Concept" using W words of length L
        using an alphabet (all small caps) of size K.

        Args:
            das (Optional[DistributedAtomSpace]): Instance of Hyperon DAS to add the nodes.
                Defaults to None.

        Returns:
            Tuple[List[Dict[str, Any]], Dict[str, Any]]: Returns a tuple containing the list
                of nodes as first element and a dict of words.

        """
        node_list = []
        node_names = set()
        word_dict: Dict[str, Any] = {}
        for node_index in range(*self.node_range):
            node: Dict[str, Any] = {'name': []}
            word_list = []
            for word_index in range(*self.word_range):
                node['name'].append([])
                w = ''.join(
                    [chr(97 + randint(*self.alphabet_range)) for _ in range(*self.letter_range)]
                )
                node['name'][word_index - self.word_range[0]] = w
                word_list.append(w)
            node['name'] = ' '.join(node['name'])
            node['type'] = self.node_type
            if das is not None:
                das.add_node(node)
            node_list.append(node)
            for w in word_list:
                if w in word_dict:
                    word_dict[w].add(node_index)
                else:
                    word_dict[w] = {node_index}
            node_names.add(node['name'])
        return node_list, word_dict

    @staticmethod
    def compare_str(a: str, b: str) -> int:
        """
        Compares two strings by counting the number of positions where the letters are identical.
        Returns a numerical score representing the strength of their similarity based on this count.
        Args:
            a (str): First string to compare
            b (str): Second string to compare

        Returns:
            strength (int): Count of similar letters between the strings.
        """
        strength = 0
        for i, s in enumerate(a):
            if s == ' ':
                continue
            if s == b[i]:
                strength += 1
        return strength

    @measure
    def generate_links_letter(self, node_list: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Creates links of type Similarity between all pair of nodes which share at least one common
        letter in the same position in their names.
        Outputs a dict containing the link reference {node1->node2} as key and the number of
        common letters as value.
        Args:
            node_list (List[Dict[str, Any]]): List of nodes

        Returns:
            letter_link_list (Dict[str, Any]): Dict containing the links as key and strength
                as value.

        """
        links_letter = {}
        words_per_letter = (self.word_range[1] - self.word_range[0]) * (
            self.letter_range[1] - self.letter_range[0]
        )
        for i, _ in enumerate(node_list):
            for j in range(i + 1, len(node_list)):
                if random.random() > self.letter_link_percentage:
                    continue
                key = f'{min(i, j)}->{max(i, j)}'
                strength = TestPerformance.compare_str(node_list[i]['name'], node_list[j]['name'])
                if strength > 0:
                    links_letter[key] = strength / words_per_letter
        return links_letter

    @staticmethod
    @measure
    def add_links(
        das: DistributedAtomSpace,
        links: Dict[str, Any],
        node_list: List[Dict[str, Any]],
        link_type: str,
        strength_divisor: int = 1,
    ) -> None:
        """
        Adds link to an instance of Hyperon DAS.
        Args:
            das (DistributedAtomSpace): Instance of Hyperon DAS
            links (Dict[str, Any]): Dict containing links to add
            node_list (List[Dict[str, Any]]): List of Nodes to retrieve as link's targets
            link_type (str): Type of link
            strength_divisor (int): Divisor number to divide the strength of the link

        Returns:
            None

        """
        for k, v in links.items():
            das.add_link(
                {
                    'type': link_type,
                    'targets': [node_list[int(i)] for i in k.split('->')],
                    'strength': v / strength_divisor,
                }
            )
        das.commit_changes()

    @measure
    def count_atoms(
        self, das: DistributedAtomSpace, options: Optional[Dict[str, Any]] = None
    ) -> Dict[str, int]:
        """
        Retrieve the count of atoms measuring the time for the request.
        Args:
            das (DistributedAtomSpace): Instance of Hyperon DAS
            options (Optional[Dict[str, Any]]): Options dict to retrieve atoms

        Returns:
            count (Dict[str, int]): Dict containing number of atoms, nodes and links.

        """
        return das.count_atoms(options)

    def test_load_performance(self, database):  # pylint: disable=unused-argument
        """
        Tests the load performance of a Hyperon DAS
        Args:
            database: Pytest fixture to manage the database

        """
        print('Nodes Range: ' + ' to '.join((str(n) for n in self.node_range)))
        print('Words Range: ' + ' to '.join((str(n) for n in self.word_range)))
        print('Letter Range: ' + ' to '.join((str(n) for n in self.letter_range)))
        print('Alphabet Range: ' + ' to '.join((chr(97 + k) for k in self.alphabet_range)))
        print('Generating Nodes')
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
        node_list, word_dict = self.generate_nodes(das)
        das.commit_changes()

        print('Nodes: ' + str(len(node_list)))
        print('Words: ' + str(len(word_dict)))
        print('Generating links word')
        links_word = self.generate_links_word(word_dict)
        print('Links Word: ' + str(len(links_word)))
        print('Adding Links')
        word_count: int = self.word_range[1] - self.word_range[0]
        self.add_links(das, links_word, node_list, 'TokenSimilarity', strength_divisor=word_count)
        print('Generating links letter')
        links_letter = self.generate_links_letter(node_list)
        print('Links Letter: ' + str(len(links_letter)))
        print('Adding Links')
        self.add_links(das, links_letter, node_list, 'Similarity')
        print('Counting Links precisely')
        count_atoms_links_nodes = self.count_atoms(das, {'precise': True})
        print(count_atoms_links_nodes)
        print('Counting Links')
        count_atom = self.count_atoms(das)
        print(count_atom)
        total = len(node_list) + len(links_word) + len(links_letter)
        assert total == count_atom['atom_count']
        assert {
            "node_count": len(node_list),
            "link_count": len(links_word) + len(links_letter),
            "atom_count": total,
        } == count_atoms_links_nodes
