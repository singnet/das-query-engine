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
from tests.integration.helpers import _db_down, _db_up

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
    test_duration: dict[str, list[float]] = {}

    def _initialize(
        self,
        node_count: str,
        word_count: str,
        word_length: str,
        alphabet_range: str,
        word_link_percentage: float,
        letter_link_percentage: float,
        seed: Any,
        debug: bool,
    ) -> None:
        self.node_type: str = 'Concept'
        self.node_count: int = int(node_count)
        self.word_count: int = int(word_count)
        self.word_length: int = int(word_length)
        self.alphabet_range: list[int] = [int(i) for i in alphabet_range.split('-')]
        self.word_link_percentage: float = float(word_link_percentage)
        self.letter_link_percentage: float = float(letter_link_percentage)
        self.seed: Any = seed
        self.link_word_count: int = 0
        self.link_letter_count: int = 0
        TestPerformance.debug = debug

        if seed:
            random.seed(seed)

    @pytest.fixture(autouse=True)
    def _initialize_fixture(
        self,
        node_count: str,
        word_count: str,
        word_length: str,
        alphabet_range: str,
        word_link_percentage: float,
        letter_link_percentage: float,
        seed: Any,
        mongo_host_port: str,
        mongo_credentials: str,
        redis_host_port: str,
        redis_credentials: str,
        redis_cluster: bool,
        redis_ssl: bool,
    ) -> None:
        self._initialize(
            node_count,
            word_count,
            word_length,
            alphabet_range,
            word_link_percentage,
            letter_link_percentage,
            seed,
            False,
        )
        self.mongo_host, self.mongo_port = mongo_host_port.split(":")
        self.mongo_user, self.mongo_pass = mongo_credentials.split(":")
        self.redis_host, self.redis_port = redis_host_port.split(":")
        self.redis_user, self.redis_pass = redis_credentials.split(":")
        self.redis_cluster = redis_cluster
        self.redis_ssl = redis_ssl

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
            mongo_hostname=self.mongo_host,
            mongo_port=int(self.mongo_port),
            mongo_username=self.mongo_user,
            mongo_password=self.mongo_pass,
            redis_hostname=self.redis_host,
            redis_port=int(self.redis_port),
            redis_username=self.redis_user,
            redis_password=self.redis_pass,
            redis_cluster=self.redis_cluster,
            redis_ssl=self.redis_ssl,
        )

    @pytest.fixture(scope='class', autouse=True)
    def print_results(self):
        yield
        for k, v in self.test_duration.items():
            if len(v) > 1:
                PERFORMANCE_REPORT.append(
                    f'{k}\tAverage: {statistics.mean(v)}\tSTDEV: {statistics.stdev(v)}'
                )
            else:
                PERFORMANCE_REPORT.append(f'{k}\tExecution Time: {v}')

    @pytest.fixture
    def measurement(self, repeat, request):
        # replaces the repeat number to aggregate the test result
        regex = r"-([0-9]+\.[0-9]+)|-[0-9]+"
        test_name = request.node.name
        test_name = re.sub(
            regex,
            lambda m: (
                ""
                if (m.start() == [match.start() for match in re.finditer(regex, test_name)][7])
                else m.group()
            ),
            request.node.name,
        )
        if test_name not in self.test_duration:
            self.test_duration[test_name] = []
        # start_time = time.perf_counter()
        start_time = time.process_time_ns()
        yield
        end_time = time.process_time_ns()
        # end_time = time.perf_counter()
        self.test_duration[test_name].append(end_time - start_time)

    def print_status(self):
        if TestPerformance.debug:
            print()
            print('Word Count:', self.word_count)
            print('Word Length:', self.word_length)
            print('Alphabet Range: ' + ' to '.join((chr(97 + k) for k in self.alphabet_range)))
            print('Nodes: ' + str(self.node_count))
            print('Links Word: ' + str(self.link_word_count))
            print('Links Letter: ' + str(self.link_letter_count))

    def _create_word(self) -> str:
        return ''.join([chr(97 + randint(*self.alphabet_range)) for _ in range(self.word_length)])

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

        for _ in range(self.node_count):
            word_list = [self._create_word() for _ in range(self.word_count)]
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
        strength_divisor: int = 1,
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

    def _load_database(self, das: DistributedAtomSpace) -> dict:
        if not TestPerformance.is_database_loaded:
            node_list: list[dict[str, Any]]
            node_list = self.generate_nodes(das)
            das.commit_changes()
            links_word = self.generate_links_word(node_list)
            self.link_word_count = len(links_word)
            self.add_links(
                das, links_word, node_list, 'TokenSimilarity', strength_divisor=self.word_count
            )
            links_letter = self.generate_links_letter(node_list)
            self.link_letter_count = len(links_letter)
            self.add_links(
                das,
                links_letter,
                node_list,
                'Similarity',
                strength_divisor=self.word_length * self.word_count,
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
        _, links = das.get_links(link_type, no_iterator=True)
        link = random.choice(links)
        measure_query = measure(das.get_atoms_by_field)
        query_answer = measure_query({'strength': link['strength'], 'named_type': link_type})
        assert isinstance(query_answer, list)
        assert query_answer

    @pytest.mark.parametrize('link_type', ['TokenSimilarity', 'Similarity'])
    def test_query_atom_by_field_with_index(self, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        _, links = das.get_links(link_type, no_iterator=True)
        link = random.choice(links)
        measure_query = measure(das.get_atoms_by_field)
        query_answer = measure_query({'strength': link['strength'], 'named_type': link_type})
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
            ('v1,v2', "TokenSimilarity"),
            ('v1,v2', "Similarity"),
            ('v1,v2,v3', "TokenSimilarity"),
            ('v1,v2,v3', "Similarity"),
        ],
    )
    def test_query_links_nodes_var(self, nodes, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        nodes = nodes.split(',')
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

    @pytest.mark.parametrize('link_type', ['TokenSimilarity', 'Similarity'])
    def test_traverse_links(self, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        nodes = das.get_node_by_name_starting_with(self.node_type, self._create_word())
        node = random.choice(nodes)
        cursor = das.get_traversal_cursor(node)
        cursors = {cursor.get()['handle']}

        while True:
            cursor.follow_link(link_type=link_type)
            if cursor.get()['handle'] in cursors:
                break
            cursors.add(cursor.get()['handle'])

        assert len(cursors) > 1

    @pytest.mark.parametrize('link_type', ['TokenSimilarity', 'Similarity'])
    def test_traverse_neighbors(self, link_type, repeat, measurement, request):
        das: DistributedAtomSpace = request.getfixturevalue('das')
        self._load_database(das)
        nodes = das.get_node_by_name_starting_with(self.node_type, self._create_word())
        node = random.choice(nodes)
        cursor = das.get_traversal_cursor(node)
        cursors = {cursor.get()['handle']}
        while True:
            links = []
            for n in cursor.get_neighbors(link_type=link_type, cursor_position=0):
                link = das.get_link(link_type, link_targets=[cursor.get()['handle'], n['handle']])
                links.append(link)
            if not links:
                break
            winner = max(links, key=lambda x: x['strength'])
            next_node = winner['targets'][1]
            cursor = das.get_traversal_cursor(next_node)
            if cursor.get()['handle'] in cursors:
                break
            cursors.add(cursor.get()['handle'])

        assert len(cursors) > 1
