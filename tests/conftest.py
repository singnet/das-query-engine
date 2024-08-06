def pytest_addoption(parser):
    parser.addoption("--node_range", default='0-100', help="Node range, eg: 0-100")
    parser.addoption("--word_range", default="2-10", help="Word range, eg: 2-10")
    parser.addoption("--letter_range", default="2-5", help="Letter range, eg: 2-5")
    parser.addoption("--alphabet_range", default="2-5", help="Alphabet range, eg: 2-5")
    parser.addoption(
        "--word_link_percentage", default=1, help="Percentage of links with same word, eg: 0.1"
    )
    parser.addoption(
        "--letter_link_percentage",
        default=1,
        help="Percentage of links with same letters, eg: 0.1",
    )
    parser.addoption("--seed", default=11, help="Randon seed")


def pytest_generate_tests(metafunc):
    performance_test_params = [
        "node_range",
        "word_range",
        "letter_range",
        "alphabet_range",
        "word_link_percentage",
        "letter_link_percentage",
        "seed",
    ]
    if all((i in metafunc.fixturenames for i in performance_test_params)):
        metafunc.config.getoption("node_range")
        metafunc.parametrize(
            ",".join(performance_test_params),
            [[metafunc.config.getoption(v) for v in performance_test_params]],
        )
