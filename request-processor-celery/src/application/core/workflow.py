import os
import csv
import urllib
import yaml
from urllib.error import HTTPError
import shutil
from application.core.utils import detect_encoding
from application.logging.logger import get_logger
from application.core.pipeline import fetch_response_data, resource_from_path
from application.configurations.config import source_url
from collections import defaultdict


logger = get_logger(__name__)


def run_workflow(fileName, collection, dataset, organisation, geom_type, directories):
    additional_column_mappings = None
    additional_concats = None

    try:
        # pipeline directory structure & download
        pipeline_dir = os.path.join(directories.PIPELINE_DIR)

        input_path = os.path.join(directories.COLLECTION_DIR, "resource")
        # List all files in the "resource" directory
        # files_in_resource = os.listdir(input_path)

        # for file_name in files_in_resource:
        #     file_path = os.path.join(input_path, file_name)

        file_path = os.path.join(input_path, fileName)
        resource = resource_from_path(file_path)

        fetch_pipeline_csvs(collection, dataset, pipeline_dir, geom_type, resource)

        fetch_response_data(
            dataset,
            organisation,
            directories.COLLECTION_DIR,
            directories.CONVERTED_DIR,
            directories.ISSUE_DIR,
            directories.COLUMN_FIELD_DIR,
            directories.TRANSFORMED_DIR,
            directories.FLATTENED_DIR,
            directories.DATASET_DIR,
            directories.DATASET_RESOURCE_DIR,
            directories.PIPELINE_DIR,
            directories.SPECIFICATION_DIR,
            directories.CACHE_DIR,
            additional_col_mappings=additional_column_mappings,
            additional_concats=additional_concats,
        )
        # Need to get the mandatory fields from specification/central place. Hardcoding for MVP
        required_fields_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "../application/configs/mandatory_fields.yaml",
        )

        required_fields = getMandatoryFields(required_fields_path, dataset)
        converted_json = []
        if os.path.exists(os.path.join(directories.CONVERTED_DIR, f"{resource}.csv")):
            converted_json = csv_to_json(
                os.path.join(directories.CONVERTED_DIR, f"{resource}.csv")
            )
        else:
            converted_json = csv_to_json(
                os.path.join(directories.COLLECTION_DIR, "resource", f"{resource}")
            )

        issue_log_json = csv_to_json(
            os.path.join(directories.ISSUE_DIR, dataset, f"{resource}.csv")
        )
        column_field_json = csv_to_json(
            os.path.join(directories.COLUMN_FIELD_DIR, dataset, f"{resource}.csv")
        )
        updateColumnFieldLog(column_field_json, required_fields)
        summary_data = error_summary(issue_log_json, column_field_json)

        # flattened_json = csv_to_json(
        #     os.path.join(directories.FLATTENED_DIR, dataset, f"{dataset}.csv")
        # )
        flattened_json = []
        response_data = {
            "converted-csv": converted_json,
            "issue-log": issue_log_json,
            "column-field-log": column_field_json,
            "flattened-csv": flattened_json,
            "error-summary": summary_data,
        }
        # logger.info("Error Summary: %s", summary_data)
        # print("***********response data****************", response_data)
    except Exception as e:
        logger.exception(f"An error occurred: {e}")

    finally:
        clean_up(
            directories.COLLECTION_DIR,
            directories.CONVERTED_DIR,
            directories.ISSUE_DIR,
            directories.COLUMN_FIELD_DIR,
            directories.TRANSFORMED_DIR,
            directories.FLATTENED_DIR,
            directories.DATASET_DIR,
            directories.DATASET_RESOURCE_DIR,
            directories.PIPELINE_DIR,
        )
    return response_data


def fetch_pipeline_csvs(collection, dataset, pipeline_dir, geom_type, resource):
    os.makedirs(pipeline_dir, exist_ok=True)
    pipeline_csvs = [
        "column.csv",
    ]
    downloaded = False
    for pipeline_csv in pipeline_csvs:
        try:
            print(
                f"{source_url}/{collection + '-collection'}/main/pipeline/{pipeline_csv}"
            )
            urllib.request.urlretrieve(
                f"{source_url}/{collection + '-collection'}/main/pipeline/{pipeline_csv}",
                os.path.join(pipeline_dir, pipeline_csv),
            )
            downloaded = True
        except HTTPError as e:
            logger.error(
                f"Failed to retrieve pipeline CSV: {e}. Attempting to download from central config repository"
            )
            logger.info(
                f"{source_url}/{'config'}/main/pipeline/{collection}/{pipeline_csv}"
            )
            try:
                urllib.request.urlretrieve(
                    f"{source_url}/{'config'}/main/pipeline/{collection}/{pipeline_csv}",
                    os.path.join(pipeline_dir, pipeline_csv),
                )
                downloaded = True
            except HTTPError as e:
                logger.error(f"Failed to retrieve from config repository: {e}")

        if downloaded:
            try:
                if (
                    dataset == "tree"
                    and geom_type == "polygon"
                    and pipeline_csv == "column.csv"
                ):
                    new_mapping = f"tree,,{resource},WKT,geometry"
                    with open(
                        os.path.join(pipeline_dir, pipeline_csv), "a"
                    ) as csv_file:
                        csv_file.write("\n" + new_mapping)
            except Exception as e:
                logger.error(f"Error saving new mapping: {e}")


def clean_up(*directories):
    try:
        for directory in directories:
            if os.path.exists(directory):
                shutil.rmtree(directory)
    except Exception as e:
        logger.error(f"An error occurred during cleanup: {e}")


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


def load_mappings(file_path):
    with open(file_path, "r") as yaml_file:
        mappings_data = yaml.safe_load(yaml_file)

    mappings = mappings_data.get("mappings", [])
    mapping_dict = {
        (mapping["field"], mapping["issue-type"]): mapping for mapping in mappings
    }
    return mapping_dict


def error_summary(issue_log, column_field):
    error_issues = [issue for issue in issue_log if issue["severity"] == "error"]
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

    # Convert error summary to JSON with formatted messages
    json_data = convert_error_summary_to_json(error_summary)
    return json_data


def convert_error_summary_to_json(error_summary):
    # Load mappings
    mappings_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "../application/configs/mapping.yaml",
    )
    mappings = load_mappings(mappings_file_path)

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
            else:
                json_data.append(str(count) + " " + str(key))
                logger.error("Mapping not found")
    return json_data
