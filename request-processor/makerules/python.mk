all:: lint test-coverage

lint: black-check flake8

black-check: 
	python -m black --check ./src ./tests

flake8: 
	python -m flake8 ./src ./tests

black:
	python -m black .

test-coverage:: coverage-unit coverage-integration

coverage-unit:
	pytest --random-order --cov=src tests/unit/

coverage-integration:
	pytest --random-order --cov=src --cov-append --cov-fail-under=80 tests/integration/
