# pylint: disable=missing-module-docstring,missing-class-docstring,
# pylint: disable=missing-function-docstring,missing-module-docstring,protected-access

import argparse

from test_mongo_redis_performance import TestPerformance


class DasWrapper:
    def __init__(self, filename):
        self.buffer = []
        self.types = set()
        self.filename = filename

    def add_node(self, node):
        self.types.add(node.get('type'))
        self.buffer.append(node)

    def add_link(self, link):
        self.types.add(link.get('type'))
        self.buffer.append(link)

    def commit_changes(self):
        with open(self.filename, 'a+', encoding='utf8') as f:
            for t in self.types:
                f.write(f'(: {t} Type)\n')
            for v in self.buffer:
                if v.get('targets'):
                    f.write(
                        f"({v['type']} \"{v['targets'][0]['name']}\""
                        f" \"{v['targets'][1]['name']} {v['strength']}\")\n"
                    )
                else:
                    f.write(f"(: \"{v['name']}\" {v['type']})\n")

        self.buffer = []
        self.types = set()

    def count_atoms(self, options):
        pass

    def create_field_index(self, *args, **kwargs):
        pass


def main():
    test_performance = TestPerformance()
    parser = argparse.ArgumentParser(description='Create MeTTa file.')
    parser.add_argument("--filename", default='test.metta', help="Filename full path")
    parser.add_argument("--node_number", default=100, help="Node range, eg: 100")
    parser.add_argument("--word_size", default=8, help="Word range, eg: 2-10")
    parser.add_argument("--letter_size", default=3, help="Letter range, eg: 2-5")
    parser.add_argument("--alphabet_range", default="2-5", help="Alphabet range, eg: 2-5")
    parser.add_argument("--seed", default=11, help="Randon seed")
    parser.add_argument(
        "--word_link_percentage", default=0.1, help="Percentage of links with same word, eg: 0.1"
    )
    parser.add_argument(
        "--letter_link_percentage",
        default=0.1,
        help="Percentage of links with same letters, eg: 0.1",
    )
    args = parser.parse_args()

    test_performance._initialize(
        int(args.node_number),
        int(args.word_size),
        int(args.letter_size),
        args.alphabet_range,
        args.word_link_percentage,
        args.letter_link_percentage,
        args.seed,
        debug=True,
    )

    test_performance._load_database(DasWrapper(args.filename))
    test_performance.print_status()


if __name__ == '__main__':
    main()
