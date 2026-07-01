import base64
import csv
import datetime
import hashlib
import json
import os
from pathlib import Path
import socket
import time
import urllib.parse
import urllib.request
from urllib.error import HTTPError, URLError
import warnings

from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding
import yaml

from application.core.utils import (
    detect_encoding,
    load_specification,
    validate_endpoint,
    validate_source,
)
from application.logging.logger import get_logger
from application.core.pipeline import (
    fetch_response_data,
    resource_from_path,
    fetch_add_data_response,
    run_task_pipeline,
)
from application.configurations.config import source_url, CONFIG_URL

logger = get_logger(__name__)

REQUEST_TIMEOUT_SECONDS = 30
_GITHUB_CONFIG_TOKEN_CACHE = {"token": None, "expires_at": 0}


def _base64url_encode(value):
    return base64.urlsafe_b64encode(value).rstrip(b"=").decode("ascii")


def is_github_url(url):
    parsed_url = urllib.parse.urlparse(url)
    return parsed_url.netloc == "raw.githubusercontent.com"


def _github_credentials():
    credentials = {
        "app_id": os.getenv("GITHUB_CONFIG_APP_ID"),
        "installation_id": os.getenv("GITHUB_CONFIG_INSTALLATION_ID"),
        "private_key": os.getenv("GITHUB_CONFIG_PRIVATE_KEY"),
    }
    configured_values = [value for value in credentials.values() if value]

    if not configured_values:
        logger.warning(
            "GitHub credentials not configured. If you want to enable authenticated config downloads, please set GITHUB_CONFIG_APP_ID, GITHUB_CONFIG_INSTALLATION_ID, and GITHUB_CONFIG_PRIVATE_KEY environment variables."
        )
        return None

    if len(configured_values) != len(credentials):
        logger.warning(
            "Incomplete GitHub credentials configuration. All of GITHUB_CONFIG_APP_ID, GITHUB_CONFIG_INSTALLATION_ID, and GITHUB_CONFIG_PRIVATE_KEY must be set to authenticate config downloads."
        )
        raise RuntimeError(
            "GITHUB_CONFIG_APP_ID, GITHUB_CONFIG_INSTALLATION_ID, and "
            "GITHUB_CONFIG_PRIVATE_KEY must all be set to authenticate config downloads"
        )

    return credentials


def _github_config_jwt(app_id, private_key_base64):
    private_key_pem = base64.b64decode(private_key_base64).decode("utf-8")
    private_key = serialization.load_pem_private_key(
        private_key_pem.encode("utf-8"),
        password=None,
    )

    now = int(time.time())
    header = {"alg": "RS256", "typ": "JWT"}
    payload = {
        "iat": now - 60,
        "exp": now + 600,
        "iss": app_id,
    }
    signing_input = ".".join(
        [
            _base64url_encode(json.dumps(header).encode("utf-8")),
            _base64url_encode(json.dumps(payload).encode("utf-8")),
        ]
    ).encode("ascii")
    signature = private_key.sign(signing_input, padding.PKCS1v15(), hashes.SHA256())

    return f"{signing_input.decode('ascii')}.{_base64url_encode(signature)}"


def _github_installation_token(credentials):
    if _GITHUB_CONFIG_TOKEN_CACHE["expires_at"] > time.time() + 300:
        return _GITHUB_CONFIG_TOKEN_CACHE["token"]

    app_jwt = _github_config_jwt(credentials["app_id"], credentials["private_key"])
    request = urllib.request.Request(
        "https://api.github.com/app/installations/"
        f"{credentials['installation_id']}/access_tokens",
        method="POST",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {app_jwt}",
            "User-Agent": "async-request-backend",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )

    try:
        with urllib.request.urlopen(
            request, timeout=REQUEST_TIMEOUT_SECONDS
        ) as response:
            response_data = json.loads(response.read().decode("utf-8"))
    except (socket.timeout, URLError) as e:
        logger.error(f"Failed to retrieve GitHub App installation token: {e}")
        raise

    expires_at = datetime.datetime.fromisoformat(
        response_data["expires_at"].replace("Z", "+00:00")
    ).timestamp()
    _GITHUB_CONFIG_TOKEN_CACHE["token"] = response_data["token"]
    _GITHUB_CONFIG_TOKEN_CACHE["expires_at"] = expires_at

    return response_data["token"]


def _download_headers(url):
    headers = {}

    if is_github_url(url):
        credentials = _github_credentials()
        if not credentials:
            return headers
        token = _github_installation_token(credentials)
        headers["Authorization"] = f"Bearer {token}"
        logger.info(f"Using GitHub App authentication for config download from {url}")

    return headers


def download_file(url, destination):
    request = urllib.request.Request(url, headers=_download_headers(url))
    try:
        with urllib.request.urlopen(
            request, timeout=REQUEST_TIMEOUT_SECONDS
        ) as response:
            with open(destination, "wb") as f:
                f.write(response.read())
    except (socket.timeout, URLError) as e:
        logger.error(f"Failed to download {url}: {e}")
        raise
    return destination


def run_workflow(
    fileName,
    request_id,
    collection,
    dataset,
    organisation,
    geom_type,
    column_mapping,
    directories,
):
    additional_concats = None
    response_data = {}

    try:
        specification = load_specification(directories)
        # pipeline directory structure & download
        pipeline_dir = os.path.join(directories.PIPELINE_DIR, dataset, request_id)

        input_path = os.path.join(directories.COLLECTION_DIR, "resource", request_id)

        file_path = os.path.join(input_path, fileName)
        resource = resource_from_path(file_path)

        fetch_pipeline_csvs(
            collection,
            dataset,
            pipeline_dir,
            geom_type,
            column_mapping,
            resource,
            specification,
        )

        # This manages the core workflow of transforming data to facts
        fetch_response_data(
            dataset,
            organisation,
            request_id,
            directories.COLLECTION_DIR,
            directories.CONVERTED_DIR,
            directories.ISSUE_DIR,
            directories.COLUMN_FIELD_DIR,
            directories.TRANSFORMED_DIR,
            directories.DATASET_RESOURCE_DIR,
            pipeline_dir,
            specification,
            directories.CACHE_DIR,
            additional_col_mappings=column_mapping,
            additional_concats=additional_concats,
        )
        # Need to get the mandatory fields from specification/central place. Hardcoding for MVP
        required_fields_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "../application/configs/mandatory_fields.yaml",
        )

        required_fields = getMandatoryFields(required_fields_path, dataset)
        # Pipeline will only create a converted if not csv format as raw input
        converted_json = []
        if os.path.exists(
            os.path.join(directories.CONVERTED_DIR, request_id, f"{resource}.csv")
        ):
            converted_json = csv_to_json(
                os.path.join(directories.CONVERTED_DIR, request_id, f"{resource}.csv")
            )
        else:
            converted_json = csv_to_json(
                os.path.join(
                    directories.COLLECTION_DIR, "resource", request_id, f"{resource}"
                )
            )

        issue_log_json = csv_to_json(
            os.path.join(directories.ISSUE_DIR, dataset, request_id, f"{resource}.csv")
        )

        # Secondary pipeline to create tasks from issues and column-field mappings, and generate task log summary
        task_log_path = os.path.join(
            directories.ISSUE_DIR, dataset, request_id, f"{resource}-tasks.csv"
        )
        task_log_json = run_task_pipeline(
            task_log_path=task_log_path,
            dataset=dataset,
            organisation=organisation,
            issue_path=os.path.join(
                directories.ISSUE_DIR, dataset, request_id, f"{resource}.csv"
            ),
            column_field_path=os.path.join(
                directories.COLUMN_FIELD_DIR, dataset, request_id, f"{resource}.csv"
            ),
            mandatory_fields=required_fields,
        )

        column_mapping = _get_column_mapping(
            os.path.join(
                directories.COLUMN_FIELD_DIR, dataset, request_id, f"{resource}.csv"
            ),
            dataset,
            required_fields,
            specification,
        )

        transformed_json = csv_to_json(
            os.path.join(
                directories.TRANSFORMED_DIR, dataset, request_id, f"{resource}.csv"
            )
        )
        response_data = {
            "converted-csv": converted_json,
            "issue-log": issue_log_json,
            "transformed-csv": transformed_json,
            "task-log": task_log_json,
            "column-mapping": column_mapping,
        }
    except Exception as e:
        logger.exception(f"An error occurred: {e}")
        response_data = {
            "message": f"An error occurred during workflow processing.",
            "status": 500,
            "exception": type(e).__name__,
        }

    finally:
        clean_up(
            request_id,
            os.path.join(directories.COLLECTION_DIR, "resource"),
            directories.COLLECTION_DIR,
            directories.CONVERTED_DIR,
            os.path.join(directories.ISSUE_DIR, dataset),
            directories.ISSUE_DIR,
            directories.COLUMN_FIELD_DIR,
            os.path.join(directories.TRANSFORMED_DIR, dataset),
            directories.TRANSFORMED_DIR,
            directories.DATASET_RESOURCE_DIR,
            os.path.join(directories.PIPELINE_DIR, dataset),
            directories.PIPELINE_DIR,
        )

    return response_data


# flake8: noqa
# pragma: mccabe-complexity 11
def fetch_pipeline_csvs(
    collection,
    dataset,
    pipeline_dir,
    geom_type,
    column_mapping,
    resource,
    specification,
):
    os.makedirs(pipeline_dir, exist_ok=True)
    pipeline_csvs = ["column.csv", "transform.csv"]
    downloaded = False
    for pipeline_csv in pipeline_csvs:
        try:
            csv_path = os.path.join(pipeline_dir, pipeline_csv)
            print(
                f"{source_url}/{collection + '-collection'}/main/pipeline/{pipeline_csv}"
            )
            download_file(
                f"{source_url}/{collection + '-collection'}/main/pipeline/{pipeline_csv}",
                csv_path,
            )
            downloaded = True
        except HTTPError as e:
            logger.warning(
                f"Failed to retrieve pipeline CSV: {e}. Attempting to download from central config repository"
            )
            logger.info(
                f"{source_url}/{'config'}/main/pipeline/{collection}/{pipeline_csv}"
            )
            try:
                download_file(
                    f"{source_url}/{'config'}/main/pipeline/{collection}/{pipeline_csv}",
                    csv_path,
                )
                downloaded = True
            except HTTPError as e:
                logger.error(f"Failed to retrieve from config repository: {e}")

        if downloaded:
            try:
                if pipeline_csv == "column.csv":
                    if column_mapping:
                        not_mapped_columns = add_extra_column_mappings(
                            csv_path,
                            column_mapping,
                            dataset,
                            resource,
                            specification,
                        )
                        return not_mapped_columns
                    if geom_type:
                        add_geom_mapping(
                            dataset, pipeline_dir, geom_type, resource, pipeline_csv
                        )
            except Exception as e:
                logger.error(f"Error saving new mapping: {e}")
    return {}


def add_geom_mapping(dataset, pipeline_dir, geom_type, resource, pipeline_csv):
    warnings.warn(
        "depreciated, use column_mapping parameter instead",
        DeprecationWarning,
        2,
    )
    if dataset == "tree" and geom_type == "polygon" and pipeline_csv == "column.csv":
        with open(os.path.join(pipeline_dir, pipeline_csv), "r") as csv_file:
            reader = csv.DictReader(csv_file)
            fieldnames = reader.fieldnames
        new_mapping = {}
        for field in fieldnames:
            new_mapping.update({field: ""})
        new_mapping.update(
            {
                "dataset": "tree",
                "resource": resource,
                "column": "WKT",
                "field": "geometry",
            }
        )
        with open(os.path.join(pipeline_dir, pipeline_csv), "a") as csv_file:
            csv_file.write("\n")
            writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
            writer.writerow(new_mapping)


def add_extra_column_mappings(
    column_path,
    column_mapping,
    dataset,
    resource,
    specification,
    endpoint_hash=None,
):
    field_names = specification.dataset_field.get(dataset, [])
    fieldnames = []
    not_mapped_columns = []
    with open(column_path) as f:
        dictreader = csv.DictReader(f)
        fieldnames = dictreader.fieldnames

    if endpoint_hash:
        mappings = {"dataset": dataset, "endpoint": endpoint_hash, "resource": ""}
    else:
        mappings = {"dataset": dataset, "resource": resource}
    column_mapping_dump = json.dumps(column_mapping)
    column_mapping_json = json.loads(column_mapping_dump)
    for key, value in column_mapping_json.items():
        mappings["column"] = key
        mappings["field"] = value
        if mappings["field"] != "IGNORE" and mappings["field"] not in field_names:
            logger.error(
                f"Error: Field '{mappings['field']}' does not exist in dataset-field.csv"
            )
            not_mapped_columns.append(mappings["field"])
        else:
            with open(column_path, "a", newline="") as f:
                f.write("\n")
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writerow(mappings)
    return not_mapped_columns


# def clean_up(*directories):
#     try:
#         for directory in directories:
#             if os.path.exists(directory):
#                 shutil.rmtree(directory)
#     except Exception as e:
#         logger.error(f"An error occurred during cleanup: {e}")


def clean_up(request_id, *directories):
    try:
        for directory in directories:
            dir_path = os.path.join(directory, str(request_id))
            if os.path.exists(dir_path):
                files = os.listdir(dir_path)
                for file in files:
                    file_path = os.path.join(dir_path, file)
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        clean_up(request_id, file_path)
                # Check if the directory is empty after removing files
                if not os.listdir(dir_path):
                    os.rmdir(dir_path)
            if os.path.exists(directory) and not os.listdir(directory):
                os.rmdir(directory)
    except Exception as e:
        logger.error(
            f"An error occurred during cleanup of {directory}: {e}", exc_info=True
        )


def csv_to_json(csv_file):
    json_data = []

    if os.path.isfile(csv_file):
        # Detect .csv encoding
        encoding = detect_encoding(csv_file)
        # Open the CSV file for reading
        try:
            with open(csv_file, "r", encoding=encoding) as csv_input:
                # Read the CSV data
                csv_data = csv.DictReader(csv_input)

                # Convert CSV to a list of dictionaries
                data_list = list(csv_data)

                for row in data_list:
                    json_data.append(row)
        except Exception:
            # TODO: Best way to show this in Sentry?
            logger.exception("Cannot process file as CSV ")

    return json_data


def _is_mandatory(field_name, required_fields):
    for req in required_fields:
        if isinstance(req, list):
            if field_name in req:
                return True
        else:
            if field_name == req:
                return True
    return False


def _get_column_mapping(column_field_path, dataset, required_fields, specification):
    """Build the column-mapping attribute for the response.

    Starts from all fields defined for the dataset in specification/dataset-field.csv,
    then overlays actual column→field mappings recorded by the pipeline in the
    column-field log CSV.  Fields that appear in the log but not the specification
    are appended.  Every entry is annotated with 'mandatory' based on required_fields.
    """
    field_dict = {}
    for field_name in specification.dataset_field.get(dataset, []):
        field_dict[field_name] = {
            "field": field_name,
            "mandatory": _is_mandatory(field_name, required_fields),
        }

    if os.path.isfile(column_field_path):
        with open(column_field_path, "r") as f:
            rows = list(csv.DictReader(f))
        for row in rows:
            field = row.get("field", "")
            column = row.get("column", "")
            if not field:
                continue
            if field in field_dict:
                field_dict[field]["column"] = column
            else:
                field_dict[field] = {
                    "field": field,
                    "column": column,
                    "mandatory": _is_mandatory(field, required_fields),
                }

    return list(field_dict.values())


def getMandatoryFields(required_fields_path, dataset):
    with open(required_fields_path, "r") as f:
        data = yaml.safe_load(f)
    required_fields = data.get(dataset, [])
    return required_fields


def add_data_workflow(
    file_name,
    request_id,
    collection,
    dataset,
    organisation_provider,
    url,
    documentation_url,
    directories,
    licence=None,
    start_date=None,
    plugin=None,
    geom_type=None,
    column_mapping=None,
    github_branch=None,
    endpoint_parameters=None,
    endpoints=None,
):
    """
    Setup directories and download required CSVs to manage add-data pipeline
    Invoke fetch_add_data_response
    Create source csv and endpoint csv summaries
    Clean up directories

    Args:
        file_name (str): Collection resource file name
        request_id (str): Unique request identifier
        collection (str): Collection name (e.g. 'article-4-direction')
        dataset (str): Dataset name (e.g. 'article-4-direction-area')
        organisation_provider (str): Organisation code providing the data
        url (str): Endpoint URL to fetch data from
        documentation_url (str): Documentation URL for the dataset
        directories (Directories): Directories object with required paths
        geom_type (str): Optional geometry type for column mapping
        column_mapping (dict): Optional caller-supplied column mappings to append to column.csv
        github_branch (str): Optional branch name to indicate if the data should be appended to a specific branch
        endpoint_parameters: Optional opaque value stored as the parameters field in the endpoint entry
        endpoints: Optional list of existing endpoint hashes associated with a resource
    """
    response_data = {}

    try:
        specification = load_specification(directories)
        pipeline_dir = os.path.join(directories.PIPELINE_DIR, collection, request_id)
        input_dir = os.path.join(directories.COLLECTION_DIR, "resource", request_id)
        collection_dir = os.path.join(directories.COLLECTION_DIR, request_id)
        output_path = os.path.join(directories.TRANSFORMED_DIR, request_id, file_name)
        converted_path = Path(
            os.path.join(directories.CONVERTED_DIR, request_id, f"{file_name}.csv")
        )
        if not os.path.exists(output_path):
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
        os.makedirs(converted_path.parent, exist_ok=True)

        resource = resource_from_path(os.path.join(input_dir, file_name))
        endpoint_hash = hashlib.sha256(url.encode("utf-8")).hexdigest() if url else None
        pipeline_endpoint = endpoints or endpoint_hash

        # Loads csvs for Pipeline and Config
        if not fetch_add_data_pipeline_csvs(
            collection,
            pipeline_dir,
            column_mapping=column_mapping,
            geom_type=geom_type,
            resource=resource,
            dataset=dataset,
            specification=specification,
            endpoint_hash=endpoint_hash,
            github_branch=github_branch,
        ):
            response_data[
                "message"
            ] = f"Unable to find lookups for collection '{collection}', dataset '{dataset}'"
            return response_data
        if not fetch_add_data_collection_csvs(
            collection, collection_dir, github_branch=github_branch
        ):
            response_data[
                "message"
            ] = f"Unable to find lookups for collection '{collection}', dataset '{dataset}'"
            return response_data

        # All processes around transforming the data and generating pipeline summary
        pipeline_summary = fetch_add_data_response(
            dataset=dataset,
            organisation_provider=organisation_provider,
            pipeline_dir=pipeline_dir,
            input_dir=input_dir,
            output_path=output_path,
            specification=specification,
            cache_dir=directories.CACHE_DIR,
            endpoint=pipeline_endpoint,
            converted_path=converted_path,
        )

        # Create endpoint and source summaries in workflow
        endpoint_summary = validate_endpoint(
            url,
            collection_dir,
            plugin,
            start_date=start_date,
            endpoint_parameters=endpoint_parameters,
        )
        source_summary = validate_source(
            documentation_url,
            collection_dir,
            collection,
            organisation_provider,
            dataset,
            endpoint_summary,
            start_date=start_date,
            licence=licence,
        )

        pipeline_issues = pipeline_summary.pop("pipeline-issues", [])
        response_data = {
            "pipeline-summary": pipeline_summary,
            "pipeline-issues": pipeline_issues,
            "endpoint-summary": endpoint_summary,
            "source-summary": source_summary,
            "converted-csv": (
                csv_to_json(str(converted_path))
                if converted_path.exists()
                else csv_to_json(os.path.join(input_dir, file_name))
            ),
            "transformed-csv": csv_to_json(output_path),
        }

        logger.info(f"add data response is for id {request_id} : {response_data}")

    except Exception as e:
        logger.warning(
            f"An error occurred in add_data_workflow: {e} for request id {request_id}"
        )
        response_data["message"] = f"An error occurred in add_data_workflow: {e}"

    finally:
        clean_up(
            request_id,
            os.path.join(directories.COLLECTION_DIR, "resource", request_id),
            os.path.join(directories.COLLECTION_DIR, request_id),
            directories.COLLECTION_DIR,
            os.path.join(directories.CONVERTED_DIR, request_id),
            directories.CONVERTED_DIR,
            os.path.join(directories.TRANSFORMED_DIR, request_id),
            directories.TRANSFORMED_DIR,
            os.path.join(directories.PIPELINE_DIR, collection),
            directories.PIPELINE_DIR,
        )

    return response_data


def fetch_add_data_pipeline_csvs(
    collection,
    pipeline_dir,
    column_mapping=None,
    geom_type=None,
    resource=None,
    dataset=None,
    specification=None,
    endpoint_hash=None,
    github_branch=None,
):
    """Download pipeline CSVs into pipeline_dir. Returns False if any errors occur.
    If column_mapping is provided, appends extra mappings to column.csv after download.
    When endpoint_hash is provided, mappings are keyed by endpoint hash rather than resource hash.
    When github_branch is provided, the pipeline CSVs are downloaded from a specific branch. (if exists, if not falls back to main branch
    """
    os.makedirs(pipeline_dir, exist_ok=True)
    pipeline_csvs = [
        "column.csv",
        "combine.csv",
        "concat.csv",
        "default-value.csv",
        "default.csv",
        "entity-organisation.csv",
        "expect.csv",
        "filter.csv",
        "lookup.csv",
        "old-entity.csv",
        "patch.csv",
        "skip.csv",
        "transform.csv",
    ]
    if github_branch:
        try:
            for csv_name in pipeline_csvs:
                csv_path = os.path.join(pipeline_dir, csv_name)
                branch_url = (
                    f"{source_url}config/refs/heads/{github_branch}/"
                    f"pipeline/{collection}/{csv_name}"
                )
                download_file(branch_url, csv_path)
                logger.info(
                    f"Downloaded {csv_name} from branch '{github_branch}': {branch_url}"
                )
        except HTTPError as e:
            if e.code != 404:
                raise
            logger.warning(f"Branch '{github_branch}' not found, falling back to main")
        else:
            column_csv_path = os.path.join(pipeline_dir, "column.csv")
            try:
                if column_mapping and resource and dataset and specification:
                    add_extra_column_mappings(
                        column_csv_path,
                        column_mapping,
                        dataset,
                        resource,
                        specification,
                        endpoint_hash=endpoint_hash,
                    )
                elif geom_type and resource and dataset:
                    add_geom_mapping(
                        dataset, pipeline_dir, geom_type, resource, "column.csv"
                    )
            except Exception as e:
                logger.error(f"Error saving column mappings to column.csv: {e}")
            return True

    for csv_name in pipeline_csvs:
        csv_path = os.path.join(pipeline_dir, csv_name)
        url = f"{CONFIG_URL}pipeline/{collection}/{csv_name}"
        try:
            download_file(url, csv_path)
            logger.info(f"Downloaded {csv_name} from {url} to {csv_path}")
        except HTTPError as e:
            logger.warning(f"Failed to retrieve {csv_name}: {e}")
            continue

        if csv_name == "column.csv":
            try:
                if column_mapping and resource and dataset and specification:
                    add_extra_column_mappings(
                        csv_path,
                        column_mapping,
                        dataset,
                        resource,
                        specification,
                        endpoint_hash=endpoint_hash,
                    )
                elif geom_type and resource and dataset:
                    add_geom_mapping(
                        dataset, pipeline_dir, geom_type, resource, csv_name
                    )
            except Exception as e:
                logger.error(f"Error saving column mappings to column.csv: {e}")

    return True


def fetch_add_data_collection_csvs(collection, config_dir, github_branch=None):
    """Download config CSVs (endpoint.csv, source.csv) into config_dir. Returns False if any errors occur."""
    os.makedirs(config_dir, exist_ok=True)
    config_csvs = ["endpoint.csv", "source.csv"]

    if github_branch:
        try:
            for csv_name in config_csvs:
                csv_path = os.path.join(config_dir, csv_name)
                branch_url = (
                    f"{source_url}config/refs/heads/{github_branch}/"
                    f"collection/{collection}/{csv_name}"
                )
                download_file(branch_url, csv_path)
                logger.info(
                    f"Downloaded {csv_name} from branch '{github_branch}': {branch_url}"
                )
            return True
        except HTTPError as e:
            if e.code != 404:
                raise
            logger.warning(f"Branch '{github_branch}' not found, falling back to main")

    for csv_name in config_csvs:
        csv_path = os.path.join(config_dir, csv_name)
        url = f"{CONFIG_URL}collection/{collection}/{csv_name}"
        try:
            download_file(url, csv_path)
            logger.info(f"Downloaded {csv_name} from {url} to {csv_path}")
        except HTTPError as e:
            logger.warning(f"Failed to retrieve {csv_name}: {e}")
            return False
    return True
