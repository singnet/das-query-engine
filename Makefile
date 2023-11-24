ci-tests:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das/ --cov-report=term-missing --cov-fail-under=70

integration-tests:
	@py.test -sx -vv ./tests/integration

unit-tests:
	@py.test -sx -vv ./tests/unit

isort:
	@isort ./hyperon_das ./tests --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=79

black:
	@black ./hyperon_das ./tests --line-length 79 -t py37 --skip-string-normalization

flake8:
	@flake8 --show-source ./hyperon_das ./test

lint: isort black flake8