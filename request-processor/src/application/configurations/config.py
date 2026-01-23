import os

source_url = "https://raw.githubusercontent.com/digital-land/"
DATASTORE_URL = os.getenv("DATASTORE_URL", "https://files.planning.data.gov.uk/")
CONFIG_URL = f"{source_url}config/refs/heads/main/"


class Directories:
    COLLECTION_DIR = "/opt/collection/"
    ISSUE_DIR = "/opt/issue/"
    COLUMN_FIELD_DIR = "var/column-field/"
    TRANSFORMED_DIR = "/opt/transformed/"
    CONVERTED_DIR = "/opt/converted/"
    PIPELINE_DIR = "/opt/pipeline/"
    SPECIFICATION_DIR = "specification/"
    DATASET_RESOURCE_DIR = "var/dataset-resource/"
    CACHE_DIR = "var/cache"
