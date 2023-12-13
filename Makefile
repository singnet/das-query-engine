test-ci:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das/ --cov-report=term-missing --cov-fail-under=70

test-integration:
	@py.test -sx -vv ./tests/integration

test-unit:
	@py.test -sx -vv ./tests/unit

isort:
	@isort ./hyperon_das ./tests --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=100

black:
	@black ./hyperon_das ./tests --line-length 100 -t py37 --skip-string-normalization

flake8:
	@flake8 --show-source ./hyperon_das ./test

lint: isort black flake8