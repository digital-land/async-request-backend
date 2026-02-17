test: lint test-coverage

lint: black-check flake8

black-check: 
	python -m black --check ./src ./tests

flake8: 
	python -m flake8 ./src ./tests

black:
	python -m black .

test-coverage:: coverage-unit coverage-integration coverage-acceptance

coverage-unit:
	pytest --cov=src tests/unit/

coverage-integration:
	pytest --cov=src --cov-append --cov-fail-under=90 tests/integration/

coverage-acceptance:
	pytest --cov=src --cov-append --cov-fail-under=90 tests/acceptance/
