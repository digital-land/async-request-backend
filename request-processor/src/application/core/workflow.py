import os
import csv
import urllib
import yaml
from urllib.error import HTTPError
from pathlib import Path
from application.core.utils import detect_encoding, extract_dataset_field_rows
from application.logging.logger import get_logger
from application.core.pipeline import fetch_response_data, resource_from_path
from application.configurations.config import source_url
from application.core.utils import append_endpoint, append_source
from collections import defaultdict
import json
import warnings
import pandas as pd

logger = get_logger(__name__)

def load_valid_organisations(organisation_csv_path):
    """
    Loads valid organisation CURIEs from organisation.csv.
    Returns a set of valid organisation strings (lowercased).
    """
    valid_orgs = set()
    try:
        if not os.path.exists(organisation_csv_path):
            logger.warning(f"organisation.csv not found at {organisation_csv_path}")
            return valid_orgs
        with open(organisation_csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                org = row.get("organisation")
                if org:
                    valid_orgs.add(org.strip().lower())
        logger.info(f"Loaded {len(valid_orgs)} valid organisations from {organisation_csv_path}")
    except Exception as e:
        logger.error(f"Failed to load organisations from {organisation_csv_path}: {e}")
    return valid_orgs


def log_lookup_csv_stats(lookup_csv_path, message_prefix=""):
    try:
        logger.info(f"Checking stats for {lookup_csv_path}")
        size_mb = os.path.getsize(lookup_csv_path) / (1024 * 1024)
        with open(lookup_csv_path, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            row_count = sum(1 for _ in reader)
        logger.info(f"{message_prefix}lookup.csv size: {size_mb:.2f} MB, rows: {row_count}")
        return size_mb, row_count
    except Exception as e:
        logger.error(f"Failed to log stats for {lookup_csv_path}: {e}")
        return 0, 0

def append_to_lookup_csv(lookup_csv_path, new_rows):
    if not new_rows:
        return
    with open(lookup_csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=new_rows[0].keys())
        for row in new_rows:
            writer.writerow(row)

def run_preview_workflow(request_id, request_data, directories, existing_response_data):
    """
    Run a simplified workflow for an 'add_data' preview.
    This does NOT re-run the pipeline. It re-uses the original response,
    fetches the latest lookup.csv, and allows save_response_to_db to
    re-calculate the entity summary.
    """
    logger.info(f"Running preview workflow for request {request_id}")
    collection = request_data.collection
    dataset = request_data.dataset
    organisation = request_data.organisation

    # We still need to fetch the latest pipeline configs to get the latest lookup.csv
    pipeline_dir = os.path.join(directories.PIPELINE_DIR, dataset, request_id)
    fetch_pipeline_csvs(collection, dataset, pipeline_dir, None, None, None, directories.SPECIFICATION_DIR, organisation, directories.CACHE_DIR)

    # Get the latest list of existing entities
    lookup_csv_path = os.path.join(pipeline_dir, "lookup.csv")
    latest_existing_entities = get_existing_entities_from_lookup(lookup_csv_path, dataset, organisation)

    # Create a new response object by taking the original and updating the existing entities list
    new_response_data = existing_response_data.copy()
    new_response_data["existing-entities"] = latest_existing_entities

    # The 'save_response_to_db' function will now correctly re-calculate the summary
    return new_response_data

def run_workflow(
    fileName,
    request_id,
    collection,
    dataset,
    organisation,
    directories,
    request_data=None,
    cleanup: bool = False,
):
    additional_concats = None

    try:
        geom_type = getattr(request_data, "geom_type", "")
        column_mapping = getattr(request_data, "column_mapping", {})
        url_to_check = getattr(request_data, "url", None)
        response_data = {}
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
            organisation
            
        )

        existing_entities = set()
        
        lookup_csv_path = os.path.join(pipeline_dir, "lookup.csv")

        if os.path.exists(lookup_csv_path):
            existing_entities = get_existing_entities_from_lookup(lookup_csv_path, dataset, organisation)
            # Convert set of tuples to list of dicts
           # existing_entities = [{"reference": ref, "entity": entity} for ref, entity in existing_entities]
           
        new_lookup_rows = fetch_response_data(
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
            lookup_csv_path=lookup_csv_path,  # pass this to pipeline.py
        )
        # Append and log after assignment
        if new_lookup_rows:
            append_to_lookup_csv(lookup_csv_path, new_lookup_rows)
            logger.info(f"Appended {len(new_lookup_rows)} new rows to lookup.csv")
        log_lookup_csv_stats(lookup_csv_path, message_prefix="After assignment: ")
        
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
            #"existing-entities": sorted(list(existing_entities)),
            "existing-entities": get_existing_entities_from_lookup(lookup_csv_path, dataset, organisation),
        }

        # If a URL was provided, perform the endpoint validation check
        if url_to_check:
            endpoint_csv_path = os.path.join(pipeline_dir, "endpoint.csv")
            endpoint_url_validation = check_endpoint_url_in_csv(endpoint_csv_path, url_to_check)
            response_data["endpoint_url_validation"] = endpoint_url_validation
            if endpoint_url_validation.get("found_in_endpoint_csv"):
                msg = (
                    f"Endpoint URL is already present in endpoint.csv "
                    f"(entry-date: {endpoint_url_validation.get('entry_date')})"
                )
                response_data.setdefault("error-summary", []).append(msg)
            else:
                entry_date = pd.Timestamp.now().isoformat()
                start_date = ""  
                end_date = ""    
                logger.info(f"[DL-ASYNC] Appending new endpoint to endpoint.csv: {url_to_check}")
                endpoint_key, new_endpoint_row = append_endpoint(endpoint_csv_path, url_to_check, entry_date, start_date, end_date)
                logger.info(f"[DL-ASYNC] Appended endpoint with key: {endpoint_key}")
                
                source_csv_path = os.path.join(pipeline_dir, "source.csv")
                attribution = getattr(request_data, "attribution", "")
                documentation_url = str(getattr(request_data, "documentation_url", "") or "")
                licence = getattr(request_data, "licence", "")
                pipelines = dataset  # Use the dataset name for the pipelines field
                logger.info(f"[DL-ASYNC] Appending new source to source.csv for endpoint: {endpoint_key}, collection: {collection}, organisation: {organisation}")
                source_key, new_source_row = append_source(
                    source_csv_path,
                    collection,
                    organisation,
                    endpoint_key,
                    attribution,
                    documentation_url,
                    licence,
                    pipelines,
                    entry_date,
                    start_date,
                    end_date
                )
                logger.info(f"[DL-ASYNC] Appended source with key: {source_key}")
                # Add the new rows to the validation response
                response_data["endpoint_url_validation"]["new_endpoint_entry"] = new_endpoint_row
                response_data["endpoint_url_validation"]["new_source_entry"] = new_source_row


    except Exception as e:
        logger.exception(f"An error occurred: {e}")

    finally:
        if cleanup:
            logger.info("Cleanup flag is True. Cleaning up directories.")
            clean_up(
                request_id,
                directories.COLLECTION_DIR + "resource",
                directories.COLLECTION_DIR,
                directories.CONVERTED_DIR,
                directories.ISSUE_DIR + dataset,
                directories.ISSUE_DIR,
                directories.COLUMN_FIELD_DIR,
                directories.TRANSFORMED_DIR + dataset,
                directories.TRANSFORMED_DIR,
                directories.DATASET_RESOURCE_DIR,
                directories.PIPELINE_DIR + dataset,
                directories.PIPELINE_DIR,
            )

        else:
            logger.info("Cleanup flag is False. Skipping cleanup.")

    return response_data

def fetch_pipeline_csvs(
    collection,
    dataset,
    pipeline_dir,
    geom_type,
    column_mapping,
    resource,
    specification_dir,
    organisation,
    cache_dir=None
):
    import urllib
    os.makedirs(pipeline_dir, exist_ok=True)
    if cache_dir:
        os.makedirs(cache_dir, exist_ok=True)

    # --- Download lookup.csv from CDN for the collection and log stats ---
    try:
        lookup_url = f"https://files.planning.data.gov.uk/config/pipeline/{collection}/lookup.csv"
        lookup_csv_local = os.path.join(pipeline_dir, "lookup.csv")
        urllib.request.urlretrieve(lookup_url, lookup_csv_local)
        logger.info(f"Downloaded lookup.csv from {lookup_url} to {lookup_csv_local}")
        log_lookup_csv_stats(lookup_csv_local, message_prefix="Before assignment: ")
        lookup_csv_path = os.path.join(pipeline_dir, "lookup.csv")
        logger.info(f"BEFORE get_existing_entities_from_lookup:")
        logger.info(f"  lookup_csv_local = {lookup_csv_local}")
        logger.info(f"  dataset = {dataset}")
        logger.info(f"  organisation = {organisation}")
        logger.info(f"  lookup file exists = {os.path.exists(lookup_csv_local)}")
        existing_entities = get_existing_entities_from_lookup(lookup_csv_path, dataset, organisation)
        logger.info("After exsisting entities")
        try:
            existing_entities = get_existing_entities_from_lookup(lookup_csv_local, dataset, organisation)
            if existing_entities:
                logger.info(
                    f"Found {len(existing_entities)} existing entities in lookup.csv: "
                    f"{[e['reference'] for e in existing_entities[:10]]}"
                    f"{' ...' if len(existing_entities) > 10 else ''}"
                )
            else:
                logger.info("No existing entities found in lookup.csv for this dataset and organisation.")
        except Exception as e:
            logger.warning(f"Could not check existing entities in lookup.csv: {e}")
    except Exception as e:
        logger.error(f"Failed to download lookup.csv from CDN: {e}")

    # --- Optionally overwrite lookup.csv with S3 version if env var is set ---
    try:
        from s3_transfer_manager import download_with_default_configuration
        bucket_name = os.environ.get("LOOKUP_BUCKET_NAME")
        object_key = "lookup.csv"
        lookup_csv_local = os.path.join(pipeline_dir, "lookup.csv")
        file_size_mb = 1  # Adjust if you know the size
        if bucket_name:
            download_with_default_configuration(
                bucket_name, object_key, lookup_csv_local, file_size_mb
            )
            logger.info(f"Downloaded lookup.csv from S3 to {lookup_csv_local}")
        else:
            logger.warning("LOOKUP_BUCKET_NAME environment variable not set. Skipping lookup.csv download from S3.")
    except Exception as e:
        logger.error(f"Failed to download lookup.csv from S3: {e}")

    # --- Download organisation.csv from CDN or S3 ---
    try:
        org_url = "http://files.planning.data.gov.uk/organisation-collection/dataset/organisation.csv"
        org_csv_local = os.path.join(cache_dir or pipeline_dir, "organisation.csv")
        # Try S3 first
        try:
            from s3_transfer_manager import download_with_default_configuration
            org_bucket = os.environ.get("ORG_BUCKET_NAME")
            org_object_key = "organisation.csv"
            file_size_mb = 1
            if org_bucket:
                download_with_default_configuration(
                    org_bucket, org_object_key, org_csv_local, file_size_mb
                )
                logger.info(f"Downloaded organisation.csv from S3 to {org_csv_local}")
            else:
                logger.warning("ORG_BUCKET_NAME environment variable not set. Skipping organisation.csv download from S3.")
                raise Exception("ORG_BUCKET_NAME not set")
        except Exception as s3e:
            logger.warning(f"Falling back to CDN for organisation.csv due to: {s3e}")
            urllib.request.urlretrieve(org_url, org_csv_local)
            logger.info(f"Downloaded organisation.csv from {org_url} to {org_csv_local}")
    except Exception as e:
        logger.error(f"Failed to download organisation.csv: {e}")

    # --- Download endpoint.csv and source.csv from CDN or S3 ---
    for csv_name in ["endpoint.csv", "source.csv"]:
        csv_url = f"http://files.planning.data.gov.uk/config/collection/{collection}/{csv_name}"
        csv_local = os.path.join(pipeline_dir, csv_name)
        # Try S3 first
        try:
            from s3_transfer_manager import download_with_default_configuration
            pipeline_bucket = os.environ.get("PIPELINE_BUCKET_NAME")
            object_key = f"{collection}/{csv_name}"
            file_size_mb = 1
            if pipeline_bucket:
                download_with_default_configuration(
                    pipeline_bucket, object_key, csv_local, file_size_mb
                )
                logger.info(f"Downloaded {csv_name} from S3 to {csv_local}")
            else:
                logger.warning(f"PIPELINE_BUCKET_NAME environment variable not set. Skipping {csv_name} download from S3.")
                raise Exception("PIPELINE_BUCKET_NAME not set")
        except Exception as s3e:
            logger.warning(f"Falling back to CDN for {csv_name} due to: {s3e}")
            try:
                urllib.request.urlretrieve(csv_url, csv_local)
                logger.info(f"Downloaded {csv_name} from {csv_url} to {csv_local}")
            except Exception as cdne:
                logger.error(f"Failed to download {csv_name} from CDN: {cdne}")

    # --- Download column.csv and transform.csv from CDN or S3 ---
    pipeline_csvs = ["column.csv", "transform.csv"]
    for pipeline_csv in pipeline_csvs:
        csv_path = os.path.join(pipeline_dir, pipeline_csv)
        # Try S3 first
        try:
            from s3_transfer_manager import download_with_default_configuration
            pipeline_bucket = os.environ.get("PIPELINE_BUCKET_NAME")
            object_key = f"{collection}/{pipeline_csv}"
            file_size_mb = 1
            if pipeline_bucket:
                download_with_default_configuration(
                    pipeline_bucket, object_key, csv_path, file_size_mb
                )
                logger.info(f"Downloaded {pipeline_csv} from S3 to {csv_path}")
            else:
                logger.warning(f"PIPELINE_BUCKET_NAME environment variable not set. Skipping {pipeline_csv} download from S3.")
                raise Exception("PIPELINE_BUCKET_NAME not set")
        except Exception as s3e:
            logger.warning(f"Falling back to CDN for {pipeline_csv} due to: {s3e}")
            try:
                logger.info(f"Ensuring {pipeline_csv} is present at {csv_path}")
                urllib.request.urlretrieve(
                    f"{source_url}/{collection + '-collection'}/main/pipeline/{pipeline_csv}",
                    csv_path,
                )
                logger.info(f"Downloaded {pipeline_csv} from {source_url}/{collection + '-collection'}/main/pipeline/{pipeline_csv} to {csv_path}")
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
                    logger.info(f"Downloaded {pipeline_csv} from central config repository to {csv_path}")
                except HTTPError as e:
                    logger.error(f"Failed to retrieve from config repository: {e}")

        # --- Existing logic for column mapping and geom_type ---
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

def get_existing_entities_from_lookup(lookup_csv_path, dataset, organisation):
    logger.info("inside the function")
    if not os.path.exists(lookup_csv_path):
        logger.info("lookup_csv_path does not exist")
        return []
    df = pd.read_csv(lookup_csv_path, dtype=str, keep_default_na=False)
    logger.info(f"lookup.csv columns: {df.columns.tolist()}")
    logger.info(f"First 5 rows of lookup.csv:\n{df.head()}")

    # Standardize column names (strip whitespace)
    df.columns = [col.strip() for col in df.columns]

    # Filter for dataset and organisation
    filtered = df[
        (df.get("prefix", "") == dataset) &
        (df.get("organisation", "") == organisation)
    ]

    # Remove header rows and rows with missing/empty reference or entity
    filtered = filtered[
        (filtered["reference"].str.lower() != "reference") &
        (filtered["entity"].str.lower() != "entity") &
        (filtered["reference"].str.strip() != "") &
        (filtered["entity"].str.strip() != "")
    ]

    logger.info(f"Filtered {len(filtered)} rows for dataset={dataset} and organisation={organisation}")
    if not filtered.empty:
        logger.info(f"First filtered row as dict: {filtered.iloc[0].to_dict()}")

    # Return only valid rows
    return [
        {"reference": row["reference"].strip(), "entity": row["entity"].strip()}
        for _, row in filtered.iterrows()
    ]

def check_endpoint_url_in_csv(endpoint_csv_path, url_to_check):
    """
    Checks if the given URL is present in endpoint.csv.
    Returns a dict with validation details.
    """
    import csv

    result = {
        "url": url_to_check,
        "found_in_endpoint_csv": False,
        "endpoint_csv_path": endpoint_csv_path,
        "entry_date": None,
    }

    if not os.path.exists(endpoint_csv_path):
        logger.warning(f"endpoint.csv not found at {endpoint_csv_path}")
        return result

    with open(endpoint_csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            endpoint_url = (row.get("endpoint-url") or "").strip()
            if endpoint_url == url_to_check.strip():
                entry_date = row.get("entry-date", "")
                logger.info(
                    f"Endpoint URL '{url_to_check}' is already present in {endpoint_csv_path} with entry-date: {entry_date}"
                )
                result["found_in_endpoint_csv"] = True
                result["entry_date"] = entry_date
                break
    if not result["found_in_endpoint_csv"]:
        logger.info(f"Endpoint URL '{url_to_check}' not found in {endpoint_csv_path}")
    return result