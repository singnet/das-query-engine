from _pytest.terminal import TerminalReporter

PERFORMANCE_REPORT: list[str] = []


def pytest_addoption(parser):
    parser.addoption("--node_count", default=100, help="Node number, eg: 100")
    parser.addoption(
        "--repeat", default=1, action='store', help='Number of times to repeat each test'
    )
    parser.addoption("--word_count", default=8, help="Word range, eg: 2-10")
    parser.addoption("--word_length", default=3, help="Letter range, eg: 2-5")
    parser.addoption("--alphabet_range", default="2-5", help="Alphabet range, eg: 2-5")
    parser.addoption(
        "--word_link_percentage", default=0.1, help="Percentage of links with same word, eg: 0.1"
    )
    parser.addoption(
        "--letter_link_percentage",
        default=0.1,
        help="Percentage of links with same letters, eg: 0.1",
    )
    parser.addoption("--seed", default=11, help="Randon number seed")


def pytest_generate_tests(metafunc):
    performance_test_params = [
        "node_count",
        "word_count",
        "word_length",
        "alphabet_range",
        "word_link_percentage",
        "letter_link_percentage",
        "seed",
    ]
    if all((i in metafunc.fixturenames for i in performance_test_params)):
        metafunc.config.getoption("node_count")
        metafunc.parametrize(
            ",".join(performance_test_params),
            [[metafunc.config.getoption(v) for v in performance_test_params]],
        )
    if metafunc.config.option.repeat is not None:
        if 'repeat' in metafunc.fixturenames:
            metafunc.parametrize('repeat', range(int(metafunc.config.option.repeat)))
    else:
        if 'repeat' in metafunc.fixturenames:
            metafunc.parametrize('repeat', 'no_repeat')


def pytest_terminal_summary(terminalreporter: TerminalReporter, exitstatus, config):
    terminalreporter.write_sep("-", "Performance Report")
    for line in PERFORMANCE_REPORT:
        terminalreporter.write_line(line)
