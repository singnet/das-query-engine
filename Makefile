isort:
	@isort ./hyperon_das ./tests --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=100

black:
	@black ./hyperon_das ./tests --line-length 100 -t py37 --skip-string-normalization

flake8:
	@flake8 ./hyperon_das ./tests --show-source --extend-ignore E501 --exclude ./hyperon_das/grpc/

lint: isort black flake8

unit-tests:
	@py.test -sx -vv ./tests/unit

unit-tests-coverage:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das/ --cov-report=term-missing --cov-fail-under=65

integration-tests:
	@py.test -sx -vv ./tests/integration

performance-tests:
	@bash ./tests/performance/run_perf_tests.sh

benchmark-tests:
	@py.test -sx -vv ./tests/performance $(OPTIONS)

pre-commit: lint unit-tests-coverage unit-tests integration-tests
