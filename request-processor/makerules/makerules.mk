# deduce the repository
ifeq ($(REPOSITORY),)
REPOSITORY=$(shell basename -s .git `git config --get remote.origin.url`)
endif

ifeq ($(ENVIRONMENT),)
ENVIRONMENT=production
endif

ifeq ($(SOURCE_URL),)
SOURCE_URL=https://raw.githubusercontent.com/digital-land/
endif

ifeq ($(CACHE_DIR),)
CACHE_DIR=var/cache/
endif


# =============================
# Utils
# =============================

# build docker image

# =============================
# Dependencies
# =============================

init::
	python -m pip install pip-tools
	python -m pip install -r requirements/requirements.txt

update-dependencies::
	make dependencies
	make config

dependencies::
	pip-sync requirements/requirements.txt  requirements/requirements.txt

pre-commit-install::
	pre-commit install

ifeq (,$(wildcard ./makerules/specification.mk))
# update local copies of specification files
specification::
	@mkdir -p specification/
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/attribution.csv' > specification/attribution.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/licence.csv' > specification/licence.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/typology.csv' > specification/typology.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/theme.csv' > specification/theme.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/collection.csv' > specification/collection.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/dataset.csv' > specification/dataset.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/dataset-field.csv' > specification/dataset-field.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/field.csv' > specification/field.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/datatype.csv' > specification/datatype.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/prefix.csv' > specification/prefix.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/provision-rule.csv' > specification/provision-rule.csv
	# deprecated ..
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/pipeline.csv' > specification/pipeline.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/dataset-schema.csv' > specification/dataset-schema.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/schema.csv' > specification/schema.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/schema-field.csv' > specification/schema-field.csv
	curl -qfsL '$(SOURCE_URL)/specification/main/specification/issue-type.csv' > specification/issue-type.csv


init::	specification
endif


config:
# local copy of organsiation datapackage
	@mkdir -p $(CACHE_DIR)
	curl -qfs "https://raw.githubusercontent.com/digital-land/organisation-dataset/main/collection/organisation.csv" > $(CACHE_DIR)organisation.csv

init:: config
