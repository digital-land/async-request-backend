import datetime
import os
import csv
from pathlib import Path
import urllib
import yaml
from urllib.error import HTTPError
from application.core.utils import detect_encoding, extract_dataset_field_rows
from application.logging.logger import get_logger
from application.core.pipeline import (
    fetch_response_data,
    resource_from_path,
    fetch_add_data_response,
)
from application.configurations.config import source_url, CONFIG_URL
from collections import defaultdict
import json
import warnings

logger = get_logger(__name__)


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

    try:
        response_data = {}
        # pipeline directory structure & download
        pipeline_dir = os.path.join(directories.PIPELINE_DIR, dataset, request_id)

        input_path = os.path.join(directories.COLLECTION_DIR, "resource", request_id)

        file_path = os.path.join(input_path, fileName)
        resource = resource_from_path(file_path)

        not_mapped_columns = fetch_pipeline_csvs(
            collection,
            dataset,
            pipeline_dir,
            geom_type,
            column_mapping,
            resource,
            directories.SPECIFICATION_DIR,
        )

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
            directories.SPECIFICATION_DIR,
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
        column_field_json = csv_to_json(
            os.path.join(
                directories.COLUMN_FIELD_DIR, dataset, request_id, f"{resource}.csv"
            )
        )
        transformed_json = csv_to_json(
            os.path.join(
                directories.TRANSFORMED_DIR, dataset, request_id, f"{resource}.csv"
            )
        )
        updateColumnFieldLog(column_field_json, required_fields)
        summary_data = error_summary(
            issue_log_json, column_field_json, not_mapped_columns
        )

        response_data = {
            "converted-csv": converted_json,
            "issue-log": issue_log_json,
            "column-field-log": column_field_json,
            "error-summary": summary_data,
            "transformed-csv": transformed_json,
        }
        # logger.info("Error Summary: %s", summary_data)
    except Exception as e:
        logger.exception(f"An error occurred: {e}")

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
    specification_dir,
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
            urllib.request.urlretrieve(
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
                urllib.request.urlretrieve(
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
                            specification_dir,
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
    column_path, column_mapping, dataset, resource, specification_dir
):
    filtered_rows = extract_dataset_field_rows(specification_dir, dataset)
    fieldnames = []
    not_mapped_columns = []
    with open(column_path) as f:
        dictreader = csv.DictReader(f)
        fieldnames = dictreader.fieldnames

    mappings = {"dataset": dataset, "resource": resource}
    column_mapping_dump = json.dumps(column_mapping)
    column_mapping_json = json.loads(column_mapping_dump)
    for key, value in column_mapping_json.items():
        mappings["column"] = key
        mappings["field"] = value
        if filtered_rows is not None:
            if mappings["field"] not in [row["field"] for row in filtered_rows]:
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
            logger.error("Cannot process file as CSV ")

    return json_data


def updateColumnFieldLog(column_field_log, required_fields):
    # # Updating all the column field entries to missing:False
    for entry in column_field_log:
        entry.setdefault("missing", False)

    for field in required_fields:
        found = any(entry["field"] in field for entry in column_field_log)
        if not found:
            # Check if the field is a list, and if so, check if at least one element is present
            if isinstance(field, list):
                for f in field:
                    column_field_log.append({"field": f, "missing": True})
            else:
                column_field_log.append({"field": field, "missing": True})


def getMandatoryFields(required_fields_path, dataset):
    with open(required_fields_path, "r") as f:
        data = yaml.safe_load(f)
    required_fields = data.get(dataset, [])
    return required_fields


def load_mappings():
    mappings_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "../application/configs/mapping.yaml",
    )
    with open(mappings_file_path, "r") as yaml_file:
        mappings_data = yaml.safe_load(yaml_file)

    mappings = mappings_data.get("mappings", [])
    mapping_dict = {
        (mapping["field"], mapping["issue-type"]): mapping for mapping in mappings
    }
    return mapping_dict


def error_summary(issue_log, column_field, not_mapped_columns):
    error_issues = [
        issue
        for issue in issue_log
        if issue["severity"] == "error" and issue["responsibility"] == "external"
    ]
    missing_columns = [field for field in column_field if field["missing"]]
    # Count occurrences for each issue-type and field
    error_summary = defaultdict(int)
    for issue in error_issues:
        field = issue["field"]
        issue_type = issue["issue-type"]
        error_summary[(issue_type, field)] += 1

    # fetch missing columns
    for column in missing_columns:
        field = column["field"]
        error_summary[("missing", field)] = True

    for col in not_mapped_columns:
        error_summary[("mapping_missing", col)] = True

    # Convert error summary to JSON with formatted messages
    json_data = convert_error_summary_to_json(error_summary)
    return json_data


def convert_error_summary_to_json(error_summary):
    mappings = load_mappings()

    json_data = []
    for key, count in error_summary.items():
        if isinstance(key, tuple):
            issue_type, field = key
            mapping = mappings.get((field, issue_type))
            if mapping:
                summary_template = mapping.get("summary-singular", "")
                summary_template_plural = mapping.get("summary-plural", "")
                if summary_template or summary_template_plural:
                    summary_template_to_use = (
                        summary_template_plural if count > 1 else summary_template
                    )
                    message = summary_template_to_use.format(
                        count=count, issue_type=issue_type, field=field
                    )
                    json_data.append(message)
            elif "missing" in key:
                message = f"{field.capitalize()} column missing"
                json_data.append(message)
            elif "mapping_missing" in key:
                message = f"{field.capitalize()} not found in specification"
                json_data.append(message)
            else:
                json_data.append(f"{count} {key}")
                logger.warning(f"Mapping not found for: {key}")
    return json_data


def add_data_workflow(
    file_name,
    request_id,
    collection,
    dataset,
    organisation,
    url,
    documentation_url,
    directories,
):
    pipeline_dir = os.path.join(directories.PIPELINE_DIR, collection, request_id)
    logger.info(f"pipeline_dir is : {pipeline_dir}")
    input_path = os.path.join(directories.COLLECTION_DIR, "resource", request_id)
    file_path = os.path.join(input_path, file_name)
    resource = resource_from_path(file_path)
    logger.info(f"resource is : {resource}")
    fetch_csv = fetch_add_data_csvs(collection, pipeline_dir)
    logger.info(f"files fetched are : {fetch_csv}")

    response_data = fetch_add_data_response(
        collection,
        dataset,
        organisation,
        pipeline_dir,
        input_path,
        directories.SPECIFICATION_DIR,
        directories.CACHE_DIR,
        url,
        documentation_url,
    )
    logger.info(f"add data response is : {response_data}")

    return response_data


def fetch_add_data_csvs(collection, pipeline_dir):
    os.makedirs(pipeline_dir, exist_ok=True)
    add_data_csvs = ["lookup.csv", "endpoint.csv", "source.csv"]
    fetched_files = []
    for csv_name in add_data_csvs:
        csv_path = os.path.join(pipeline_dir, csv_name)
        if csv_name == "lookup.csv":
            url = f"{CONFIG_URL}pipeline/{collection}/{csv_name}"
        else:
            url = f"{CONFIG_URL}collection/{collection}/{csv_name}"
        try:
            urllib.request.urlretrieve(url, csv_path)
            logger.info(f"Downloaded {csv_name} from {url} to {csv_path}")
        except HTTPError as e:
            logger.warning(f"Failed to retrieve {csv_name}: {e}.")
    return fetched_files
