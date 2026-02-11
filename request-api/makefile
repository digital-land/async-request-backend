test: lint test-coverage

lint:: 
	make black ./src
	python -m flake8 ./src

black-check:
	black --check .

black:
	black .

flake8:
	flake8 .

test-coverage:: coverage-unit coverage-integration coverage-acceptance

coverage-unit:
	pytest --cov=src tests/unit/

coverage-integration:
	pytest --cov=src --cov-append --cov-fail-under=90 tests/integration/

coverage-acceptance:
	pytest --cov=src --cov-append --cov-fail-under=90 tests/acceptance/
