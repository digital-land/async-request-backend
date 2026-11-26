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

ifeq ($(DATASTORE_URL),)
DATASTORE_URL=https://files.planning.data.gov.uk/
endif

ifeq ($(SPECIFICATION_URL),)
SPECIFICATION_URL=$(DATASTORE_URL)specification/
endif

ifeq ($(COLLECTION_NAME),)
COLLECTION_NAME=$(shell echo "$(REPOSITORY)"|sed 's/-collection$$//')
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
# update local copies of specification files (S3 in AWS, CDN locally — same as organisation.csv)
SPECIFICATION_CSVS=\
	attribution licence typology theme collection dataset dataset-field \
	field datatype prefix provision-rule pipeline dataset-schema schema schema-field

specification:: $(addprefix specification/,$(addsuffix .csv,$(SPECIFICATION_CSVS)))

specification/%.csv:
	@mkdir -p specification/
ifneq ($(COLLECTION_DATASET_BUCKET_NAME),)
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/specification/$(notdir $@) $@ --no-progress
else
	curl -qfsL '$(SPECIFICATION_URL)$(notdir $@)?version=$(shell date +%s)' -o $@
endif

init::	specification
endif

# local copy of organsiation datapackage
$(CACHE_DIR)organisation.csv:
	@mkdir -p $(CACHE_DIR)
ifneq ($(COLLECTION_DATASET_BUCKET_NAME),)
	aws s3 cp s3://$(COLLECTION_DATASET_BUCKET_NAME)/organisation-collection/dataset/organisation.csv $(CACHE_DIR)organisation.csv
else
	curl -qfs "$(DATASTORE_URL)organisation-collection/dataset/organisation.csv" > $(CACHE_DIR)organisation.csv
endif
init:: config
