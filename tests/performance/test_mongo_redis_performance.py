# (1) Add N nodes of type "Concept" named using W words of length L using an alphabet (all small caps)
# of size K (2 <= K <= 6). For instance, possible nodes considering W = 3 , L = 4 and K = 5 would be:
#
# (Concept "aabb abed bbbb")
# (Concept "abcd bcde aaee")
# (Concept "bbbe edcb eeaa")
# ...
#
# Make sure a given name is used only once
#
# (2) Add a link of type TokenSimilarity between all pair of nodes which share at least one common word in their names.
# These links should also have a field "strength" which is a float calculated using (number of common words) / W
#
# For instance, considering the same parameters above: abcd abab aaab and abab cbba cccc would have such a link with
# strength = 1/3, because abab appears in both names.
#
# (3) Add a link of type Similarity between all pair of nodes which share at least one common letter in the same
# position in their names. These links should also have a field "strength" which is a float calculated using
# (number of common letters) / (W * L)
#
# For instance, considering the same parameters above: abcd abcd abcd and ebab aeee bacd would have such link with
# strength 4 / 12 because there are 4 matches as shown below:
#
# abcd abcd abcd
# ebab aeee bacd
#  ^   ^      ^^
#
# (4) We should have a test script instrumenting the calls to measure execution time os queries.
#
# (5) In such test script, we need to implement different types of queries to use different types os indexes
# we have in MongoDB
#
# (6) Each query should be called a number of times passing a randomized set of parameters inside a series of
# loops where all parameters are also varying in different ranges. For instance:
#
# NUM_TESTS = 10
#
# for N = {100, 1000, 10000}
#     for W = {2, 3, 5, 10}
#         for L = {2, 5, 10}
#             for K = {2, 4, 6}
#                 create nodes
#                 create links
#                 for i = 1 to NUM_TESTS
#                     randomly select a valid parameter for query 1
#                     run query 1
#                     randomly select a valid parameter for query 2
#                     run query 2
#                     randomly select a valid parameter for query ...
#                     run query ...
#
# (7) Collect execution time of each query call and report the averages properly.
import random
import time
from random import randint
from typing import Any, Callable, Dict

import pytest

from hyperon_das import DistributedAtomSpace
from tests.integration.helpers import _db_down, _db_up, mongo_port, redis_port


def measure(func):
    def wrapper(*args, **kwargs):
        start = time.time()
        values = func(*args, **kwargs)
        end = time.time()
        TestPerformance.time = end - start
        print(f'Elapsed time: {TestPerformance.time}')
        return values

    return wrapper


class TestPerformance:
    time = 0

    @pytest.fixture(autouse=True)
    def _initialize(
        self,
        node_range,
        word_range,
        letter_range,
        alphabet_range,
        word_link_percentage,
        letter_link_percentage,
        seed,
    ):
        self.node_type = 'Concept'
        self.node_range = [int(i) for i in node_range.split('-')]
        self.word_range = [int(i) for i in word_range.split('-')]
        self.letter_range = [int(i) for i in letter_range.split('-')]
        self.alphabet_range = [int(i) for i in alphabet_range.split('-')]
        self.word_link_percentage = word_link_percentage
        self.letter_link_percentage = letter_link_percentage
        self.seed = seed
        if seed:
            random.seed(seed)

    @pytest.fixture(scope="class", autouse=True)
    def database(self):
        _db_up()
        yield
        _db_down()

    @staticmethod
    def combinatorial_loop(d: Dict[str, Any], f: Callable[[int, int], None], percentage: float):
        for v in d.values():
            v = list(v)
            for i, _ in enumerate(v):
                for j in range(i + 1, len(v)):
                    if random.random() > percentage:
                        continue
                    f(v[i], v[j])

    @measure
    def generate_links_word(self, word_dict):
        links_dict_word = {}
        word_count = self.word_range[1] - self.word_range[0]
        for v in word_dict.values():
            v = list(v)
            for i in range(0, len(v)):
                for j in range(i + 1, len(v)):
                    if random.random() > self.word_link_percentage:
                        continue
                    key = f'{min(v[i], v[j])}->{max(v[i], v[j])}'
                    if key in links_dict_word:
                        links_dict_word[key] += 1 / word_count
                    else:
                        links_dict_word[key] = 1 / word_count
        return links_dict_word

    @measure
    def generate_nodes(self, das: DistributedAtomSpace = None):
        node_list = []
        node_names = {}
        word_dict = {}
        for N in range(*self.node_range):
            node_list.append({'name': []})
            word_list = []
            for W in range(*self.word_range):
                node_list[N]['name'].append([])
                node_list[N]['name'][W - self.word_range[0]] = ''.join(
                    [chr(97 + randint(*self.alphabet_range)) for L in range(*self.letter_range)]
                )
                word_list.append(node_list[N]['name'][W - self.word_range[0]])
            node_list[N]['name'] = ' '.join(node_list[N]['name'])
            node_list[N]['type'] = self.node_type
            das.add_node(node_list[N])

            for w in word_list:
                if w in word_dict:
                    word_dict[w].add(N)
                else:
                    word_dict[w] = set([N])

            if node_list[N]['name'] in node_names:
                raise ValueError('Exists')
            node_names[node_list[N]['name']] = 1
        return node_list, word_dict

    @staticmethod
    def compare_str(a, b):
        strength = 0
        for i in range(0, len(a)):
            if a[i] == ' ':
                continue
            if a[i] == b[i]:
                strength += 1
        return strength

    @measure
    def generate_links_letter(self, node_list):
        links_letter = {}
        words_per_letter = (self.word_range[1] - self.word_range[0]) * (
            self.letter_range[1] - self.letter_range[0]
        )
        for i in range(0, len(node_list)):
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
    def add_links(das, links, node_list, link_type=''):
        for k, v in links.items():
            das.add_link(
                {
                    'type': link_type,
                    'targets': [node_list[int(i)] for i in k.split('->')],
                    'strength': v,
                }
            )
        das.commit_changes()

    @measure
    def count_atoms(self, das, options=None):
        return das.count_atoms(options)

    def test_load_performance(self, database):
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
        self.add_links(das, links_word, node_list, 'TokenSimilarity')
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
