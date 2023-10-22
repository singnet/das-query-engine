

test:
	@pytest -v -s

test-coverage:
	@py.test -sx -vv ./tests --cov=./hyperon_das/ --cov-report=term-missing --cov-fail-under=90

isort:
	@isort ./hyperon_das --multi-line=3 --trailing-comma --force-grid-wrap=0 --use-parentheses --line-width=79

black:
	@black ./hyperon_das --line-length 79 -t py37 --skip-string-normalization

flake8:
	@flake8 --show-source ./hyperon_das

lint: isort black flake8