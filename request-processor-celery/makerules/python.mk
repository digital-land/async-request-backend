all:: lint test coverage

lint:
	make black ./src/application
	python3 -m flake8 ./src/application

black-check:
	black --check .

black:
	python3 -m black .

test:: test-unit test-integration

test-unit:
	[ -d tests/unit ] && python -m pytest tests/unit/

test-integration:
	[ -d tests/integration ] && python -m pytest tests/integration/

coverage:: coverage-unit coverage-integration

coverage-unit:
	pytest --cov=src tests/unit/

coverage-integration:
	pytest --cov=src --cov-append --cov-fail-under=90 tests/integration/
