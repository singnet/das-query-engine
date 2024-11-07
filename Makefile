isort:
	@isort --settings-path .isort.cfg ./hyperon_das ./tests

black:
	@black --config .black.cfg ./hyperon_das ./tests

flake8:
	@flake8 --config .flake8.cfg ./hyperon_das ./tests --exclude ./hyperon_das/grpc/

lint: isort black flake8

unit-tests:
	@py.test -sx -vv ./tests/unit

unit-tests-coverage:
	@py.test -sx -vv ./tests/unit --cov=./hyperon_das/ --cov-report=term-missing --cov-fail-under=65

integration-tests:
	@py.test -sx -vv ./tests/integration $(OPTIONS)

performance-tests:
	@bash ./tests/performance/dasgate/run_perf_tests.sh

benchmark-tests:
	@py.test -sx -vv ./tests/performance $(OPTIONS)

benchmark-tests-metta-file:
	@python ./tests/performance/metta_files_generator.py $(OPTIONS)

pre-commit: lint unit-tests-coverage unit-tests integration-tests
