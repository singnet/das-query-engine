"""Test node and link generation and Hyperon DAS load performance."""
import random
import re
import statistics
import time
from random import randint
from typing import Any

import pytest
from conftest import PERFORMANCE_REPORT

from hyperon_das import DistributedAtomSpace
from tests.integration.helpers import _db_down, _db_up, mongo_port, redis_port

# pylint: disable=attribute-defined-outside-init,disable=too-many-instance-attributes
# pylint: disable=unused-argument,too-many-arguments,missing-function-docstring,too-many-locals


def measure(func):
    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        values = func(*args, **kwargs)
        end = time.perf_counter()
        TestPerformance.time = end - start
        if TestPerformance.debug:
            print(f'Elapsed ({func.__name__}) time: {TestPerformance.time}')
        return values

    return wrapper


class TestPerformance:
    """Test node/link generation and Hyperon DAS load."""

    time = 0.0
    is_database_loaded = False
    debug = False
    test_duration = {}

    def _initialize(
        self,
        node_number: str,
        word_size: str,
        letter_size: str,
        alphabet_range: str,
        word_link_percentage: float,
        letter_link_percentage: float,
        seed: Any,
        debug: bool,
    ) -> None:
        self.node_type: str = 'Concept'
        self.node_number: int = int(node_number)
        self.word_size: int = int(word_size)
        self.letter_size: int = int(letter_size)
        self.alphabet_range: list[int] = [int(i) for i in alphabet_range.split('-')]
        self.word_link_percentage: float = float(word_link_percentage)
        self.letter_link_percentage: float = float(letter_link_percentage)
        self.seed: Any = seed
        self.node_count: int = 0
        self.link_word_count: int = 0
        self.link_letter_count: int = 0
        self.word_count: int = 0
        TestPerformance.debug = debug
        # self.test_duration = {}

        if seed:
            random.seed(seed)

    @pytest.fixture(autouse=True)
    def _initialize_fixture(
        self,
        node_number: str,
        word_size: str,
        letter_size: str,
        alphabet_range: str,
        word_link_percentage: float,
        letter_link_percentage: float,
        seed: Any,
    ) -> None:
        self._initialize(
            node_number,
            word_size,
            letter_size,
            alphabet_range,
            word_link_percentage,
            letter_link_percentage,
            seed,
            False,
        )

    @pytest.fixture(scope="class", autouse=True)
    def database(self):
        """Pytest fixture to manage database lifecycle."""
        _db_up()
        yield
        _db_down()

    @pytest.fixture
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

    @pytest.fixture(scope='class', autouse=True)
    def print_results(self):
        yield
        for k, v in self.test_duration.items():
            PERFORMANCE_REPORT.append(
                f'{k}\tMedian: {statistics.median(v)}\tSTDEV: {statistics.stdev(v)}'
            )

    @pytest.fixture
    def measurement(self, repeat, request):
        regex = r"^(.*)(?=\[)"
        test_name = re.compile(regex).search(request.node.name).group(1)
        if test_name not in self.test_duration:
            self.test_duration[test_name] = []
        start_time = time.perf_counter()
        yield
        end_time = time.perf_counter()
        self.test_duration[test_name].append(end_time - start_time)

    def print_status(self):
        if TestPerformance.debug:
            print()
            print('Word Size:', self.word_size)
            print('Letter Size:', self.letter_size)
            print('Alphabet Range: ' + ' to '.join((chr(97 + k) for k in self.alphabet_range)))
            print('Nodes: ' + str(self.node_count))
            print('Links Word: ' + str(self.link_word_count))
            print('Links Letter: ' + str(self.link_letter_count))

    def _create_word(self) -> str:
        return ''.join([chr(97 + randint(*self.alphabet_range)) for _ in range(self.letter_size)])

    @staticmethod
    def compare_words(a: str, b: str) -> int:
        return len(set(a.split(' ')) & set(b.split(' ')))

    @measure
    def generate_links_word(self, node_list: list[dict[str, Any]]) -> dict[str, Any]:
        links_dict_word: dict[str, Any] = {}
        for i, v in enumerate(node_list):
            for j in range(i + 1, len(node_list)):
                if random.random() > self.word_link_percentage:
                    continue
                strength = self.compare_words(v['name'], node_list[j]['name'])
                if strength > 0:
                    links_dict_word[f'{i}->{j}'] = strength
        return links_dict_word

    @measure
    def generate_nodes(self, das: DistributedAtomSpace | None = None) -> list[dict[str, Any]]:
        """
        Creates and add to Hyperon DAS nodes of type "Concept" using W words of length L
        using an alphabet (all small caps) of size K.

        Args:
            das (DistributedAtomSpace | None): Instance of Hyperon DAS to add the nodes.
                Defaults to None.

        Returns:
            tuple[list[dict[str, Any]], dict[str, Any]]: Returns a tuple containing the list
                of nodes as first element and a dict of words.

        """
        node_list = []
        node_names = set()

        for _ in range(self.node_number):
            word_list = [self._create_word() for _ in range(self.word_size)]
            node = {
                'name': ' '.join(word_list),
                'type': self.node_type,
            }
            if das is not None:
                das.add_node(node)
            node_list.append(node)
            node_names.add(node['name'])

        return node_list

    @staticmethod
    def compare_str(a: str, b: str) -> int:
        """
        Compares two strings by counting the number of positions where the letters are identical,
        skipping the blank spaces.
        Returns a numerical score representing the strength of their similarity based on this count.
        Args:
            a (str): First string to compare
            b (str): Second string to compare

        Returns:
            strength (int): Count of similar letters between the strings.
        """
        strength = 0
        for i, s in enumerate(a):
            if i >= len(b):
                break
            if s == ' ':
                continue
            if s == b[i]:
                strength += 1
        return strength

    @measure
    def generate_links_letter(self, node_list: list[dict[str, Any]]) -> dict[str, Any]:
        """
        Creates links of type Similarity between all pair of nodes which share at least one common
        letter in the same position in their names.
        Outputs a dict containing the link reference {node1->node2} as key and the number of
        common letters as value.
        Args:
            node_list (list[dict[str, Any]]): list of nodes

        Returns:
            letter_link_list (dict[str, Any]): dict containing the links as key and strength
                as value.

        """
        links_letter = {}
        for i, _ in enumerate(node_list):
            for j in range(i + 1, len(node_list)):
                if random.random() > self.letter_link_percentage:
                    continue
                key = f'{min(i, j)}->{max(i, j)}'
                strength = TestPerformance.compare_str(node_list[i]['name'], node_list[j]['name'])
                if strength > 0:
                    links_letter[key] = strength
        return links_letter

    @staticmethod
    @measure
    def add_links(
        das: DistributedAtomSpace,
        links: dict[str, Any],
        node_list: list[dict[str, Any]],
        link_type: str,
        strength_divisor: int,
    ) -> None:
        """
        Adds link to an instance of Hyperon DAS.
        Args:
            das (DistributedAtomSpace): Instance of Hyperon DAS
            links (dict[str, Any]): dict containing links to add
            node_list (list[dict[str, Any]]): list of Nodes to retrieve as link's targets
            link_type (str): Type of link
            strength_divisor (int): Divisor number to divide the strength of the link

        Returns:
            None

        """
        for k, v in links.items():
            targets = [node_list[int(i)] for i in k.split('->')]
            das.add_link(
                {
                    'type': link_type,
                    'targets': targets,
                    'strength': v / strength_divisor,
                }
            )
        das.commit_changes()

    @measure
    def count_atoms(
        self, das: DistributedAtomSpace, options: dict[str, Any] | None = None
    ) -> dict[str, int]:
        """
        Retrieve the count of atoms measuring the time for the request.
        Args:
            das (DistributedAtomSpace): Instance of Hyperon DAS
            options (dict[str, Any] | None): Options dict to retrieve atoms

        Returns:
            count (dict[str, int]): dict containing number of atoms, nodes and links.

        """
        return das.count_atoms(options)

    def _load_database(self, das: DistributedAtomSpace) -> dict | None:
        if not TestPerformance.is_database_loaded:
            node_list: list[dict[str, Any]]
            node_list = self.generate_nodes(das)
            das.commit_changes()
            self.node_count = len(node_list)
            links_word = self.generate_links_word(node_list)
            self.link_word_count = len(links_word)
            self.add_links(
                das, links_word, node_list, 'TokenSimilarity', strength_divisor=self.word_size
            )
            links_letter = self.generate_links_letter(node_list)
            self.link_letter_count = len(links_letter)
            self.add_links(
                das,
                links_letter,
                node_list,
                'Similarity',
                strength_divisor=self.letter_size * self.word_size,
            )
            count_atoms_links_nodes: dict[str, int] = self.count_atoms(das, {'precise': True})
            self.count_atoms(das)
            TestPerformance.is_database_loaded = True
            das.create_field_index('link', ['strength', 'named_type'])
            return count_atoms_links_nodes
        return self.count_atoms(das, {'precise': True})

    def test_load_performance(self, database, das: DistributedAtomSpace):
        count_atoms_links_nodes = self._load_database(das)
        self.print_status()
        total = self.node_count + self.link_word_count + self.link_letter_count
        assert total == count_atoms_links_nodes['atom_count']
        assert {
            "node_count": self.node_count,
            "link_count": self.link_word_count + self.link_letter_count,
            "atom_count": total,
        } == count_atoms_links_nodes

    @pytest.mark.parametrize('link_type', (['TokenSimilarity', 'Similarity']))
    def test_query_atom_by_field(self, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        measure_query = measure(das.get_atoms_by_field)
        query_answer = measure_query({'strength': 0.375, 'named_type': link_type})
        assert isinstance(query_answer, list)
        assert query_answer

    @pytest.mark.parametrize('link_type', ['TokenSimilarity', 'Similarity'])
    def test_query_atom_by_field_with_index(self, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        # time.sleep(10)  # Waiting to mongodb reindex the database
        measure_query = measure(das.get_atoms_by_field)
        query_answer = measure_query({'strength': 0.375, 'named_type': link_type})
        assert isinstance(query_answer, list)
        assert query_answer

    def test_query_by_text_field(self, database, repeat, measurement, das: DistributedAtomSpace):
        self._load_database(das)
        measure_query = measure(das.get_atoms_by_text_field)
        query_answer = measure_query(self._create_word(), 'name')
        assert isinstance(query_answer, list)
        assert query_answer

    def test_query_node_by_name_starting_with(
        self, database, repeat, measurement, das: DistributedAtomSpace
    ):
        self._load_database(das)
        measure_query = measure(das.get_node_by_name_starting_with)
        query_answer = measure_query(self.node_type, self._create_word())
        assert isinstance(query_answer, list)
        assert query_answer

    @pytest.mark.parametrize(
        'nodes,link_type',
        [
            (['v1', 'v2'], "TokenSimilarity"),
            (['v1', 'v2'], "Similarity"),
            (['v1', 'v2', 'v3'], "TokenSimilarity"),
            (['v1', 'v2', 'v3'], "Similarity"),
        ],
    )
    def test_query_links_nodes_var(self, nodes, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        queries = []
        for i, node in enumerate(nodes):
            for j in range(i + 1, len(nodes)):
                query = {
                    'atom_type': 'link',
                    'type': 'TokenSimilarity',
                    'targets': [
                        {'atom_type': 'variable', 'name': node},
                        {'atom_type': 'variable', 'name': nodes[j]},
                    ],
                }
                queries.append(query)

        measure_query = measure(das.query)
        query_answers = measure_query(queries)

        def process(d, q, n):
            for qq in q:
                for nn in n:
                    d.get_atom(qq.assignment.mapping[nn])

        measure_process = measure(process)
        measure_process(das, query_answers, nodes)
        assert query_answers
