"""Test node and link generation and Hyperon DAS load performance."""
import random
import time
from random import randint
from typing import Any, Dict, List, Optional, Tuple

import pytest

from hyperon_das import DistributedAtomSpace
from tests.integration.helpers import _db_down, _db_up, mongo_port, redis_port

# pylint: disable=attribute-defined-outside-init,disable=too-many-instance-attributes
# pylint: disable=unused-argument,too-many-arguments,missing-function-docstring


def measure(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        values = func(*args, **kwargs)
        end = time.perf_counter()
        TestPerformance.time = end - start
        print(f'Elapsed ({func.__name__}) time: {TestPerformance.time}')
        return values

    return wrapper


class TestPerformance:
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
        self.node_type = 'Concept'
        self.node_range = [int(i) for i in node_range.split('-')]
        self.word_range = [int(i) for i in word_range.split('-')]
        self.letter_range = [int(i) for i in letter_range.split('-')]
        self.alphabet_range = [int(i) for i in alphabet_range.split('-')]
        self.word_link_percentage = word_link_percentage
        self.letter_link_percentage = letter_link_percentage
        self.seed = seed
        self.node_count = 0
        self.link_word_count = 0
        self.link_letter_count = 0
        self.word_count = 0

        if seed:
            random.seed(seed)

    @pytest.fixture(scope="class", autouse=True)
    def database(self):
        """Pytest fixture to manage database lifecycle."""
        _db_up()
        yield
        _db_down()

    @pytest.fixture()
    def das(self):
        yield DistributedAtomSpace(
            query_engine='local',
            atomdb='redis_mongo',
            mongo_port=mongo_port,
            mongo_username='dbadmin',
            mongo_password='dassecret',
            redis_port=redis_port,
            redis_cluster=False,
            redis_ssl=False,
        )

    def print_status(self):
        print('Nodes Range: ' + ' to '.join((str(n) for n in self.node_range)))
        print('Words Range: ' + ' to '.join((str(n) for n in self.word_range)))
        print('Letter Range: ' + ' to '.join((str(n) for n in self.letter_range)))
        print('Alphabet Range: ' + ' to '.join((chr(97 + k) for k in self.alphabet_range)))
        print('Nodes: ' + str(self.node_count))
        print('Words: ' + str(self.word_count))
        print('Links Word: ' + str(self.link_word_count))
        print('Links Letter: ' + str(self.link_letter_count))

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

    def _load_database(self, das: DistributedAtomSpace):
        node_list: list[dict[str, Any]]
        node_list, word_dict = self.generate_nodes(das)
        das.commit_changes()
        self.node_count = len(node_list)
        self.word_count = len(word_dict)
        links_word = self.generate_links_word(word_dict)
        self.link_word_count = len(links_word)
        word_count: int = self.word_range[1] - self.word_range[0]
        self.add_links(das, links_word, node_list, 'TokenSimilarity', strength_divisor=word_count)
        links_letter = self.generate_links_letter(node_list)
        self.link_letter_count = len(links_letter)
        self.add_links(das, links_letter, node_list, 'Similarity')
        count_atoms_links_nodes: dict[str, int] = self.count_atoms(das, {'precise': True})
        self.count_atoms(das)
        return count_atoms_links_nodes

    def test_load_performance(self, database, das: DistributedAtomSpace):
        print('')
        count_atoms_links_nodes = self._load_database(das)
        self.print_status()
        total = self.node_count + self.link_word_count + self.link_letter_count
        assert total == count_atoms_links_nodes['atom_count']
        assert {
            "node_count": self.node_count,
            "link_count": self.link_word_count + self.link_letter_count,
            "atom_count": total,
        } == count_atoms_links_nodes

    def test_query_atom_by_name(self, database, das: DistributedAtomSpace):
        pass

    def test_query_node_by_name(self, database, das: DistributedAtomSpace):
        pass

    def test_query_by_text_field(self, database, das: DistributedAtomSpace):
        pass

    def test_query_node_by_name_starting_with(self, database, das: DistributedAtomSpace):
        pass

    @measure
    def query(self, das, query):
        return das.query(query)

    @measure
    def process_query(self, query_answers):
        print(query_answers)
        for query_answer in query_answers:
            print(query_answer.assignment)
            atom_matching_v1 = das.get_atom(query_answer.assignment.mapping['v1'])
            atom_matching_v2 = das.get_atom(query_answer.assignment.mapping['v2'])
            atom_matching_v3 = das.get_atom(query_answer.assignment.mapping['v3'])
            atom_matching_v4 = das.get_atom(query_answer.assignment.mapping['v4'])
            print("v1:", atom_matching_v1['type'], atom_matching_v1['name'])
            # print("v2:", atom_matching_v2['type'], atom_matching_v2['name'])
            # print("v3:", atom_matching_v3['type'], atom_matching_v2['name'])
            # print("v4:", atom_matching_v4['type'], atom_matching_v2['name'])
            # rewrited_query = query_answer.subgraph
            # print(rewrited_query)
            print()

    def test_query_nodes_var(self, database, das: DistributedAtomSpace):
        # links = das.get_links(link_type='TokenSimilarity', target_types=['Concept', 'Concept'])
        # links = das.get_links(link_type='TokenSimilarity', link_targets=['4c9941037a9dae9a34194098132b3940', '*'])
        # cursor = das.get_traversal_cursor('4c9941037a9dae9a34194098132b3940')
        # print(cursor.get())
        # print("All neighbors:", [(d['type'], d['name']) for d in cursor.get_neighbors()])
        # for link in links:
        #     print(link['type'], link['targets'])
        nodes = ['v1', 'v2', 'v3', 'v4']
        queries = []
        for i, node in enumerate(nodes):
            for j in range(i+1, len(nodes)):
                query = {
                    'atom_type': 'link',
                    'type': 'TokenSimilarity',
                    'targets': [
                        {'atom_type': 'variable', 'name': node},
                        {'atom_type': 'variable', 'name': nodes[j]},
                    ],
                }
                queries.append(query)



        print(queries)
        query_answers = self.query(das, queries)
        self.process_query(query_answers)


