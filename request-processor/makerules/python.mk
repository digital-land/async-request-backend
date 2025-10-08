all:: lint test-coverage

lint:
	make black ./src/application
	python -m flake8 ./src/application

black-check:
	black --check .

black:
	python -m black .

test-coverage:: coverage-unit coverage-integration

coverage-unit:
	pytest --random-order --cov=src tests/unit/

coverage-integration:
	pytest --random-order --cov=src --cov-append --cov-fail-under=85 tests/integration/
