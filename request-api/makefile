test: lint test-coverage

lint:: black-check flake8

black-check:
	black --check .

black:
	black .

flake8:
	flake8 .

test-coverage:: coverage-unit coverage-integration

coverage-unit:
	pytest --random-order --cov=src tests/unit/

coverage-integration:
	pytest --random-order --cov=src --cov-append --cov-fail-under=90 tests/integration/
