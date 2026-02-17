import os
import csv
from application.logging.logger import get_logger
from digital_land.specification import Specification
from digital_land.organisation import Organisation
from digital_land.api import API

from digital_land.pipeline import Pipeline, Lookups
from digital_land.commands import get_resource_unidentified_lookups
from pathlib import Path

logger = get_logger(__name__)


def fetch_response_data(
    dataset,
    organisation,
    request_id,
    collection_dir,
    converted_dir,
    issue_dir,
    column_field_dir,
    transformed_dir,
    dataset_resource_dir,
    pipeline_dir,
    specification_dir,
    cache_dir,
    additional_col_mappings,
    additional_concats,
):
    # define variables for Pipeline Execution
    specification = Specification(specification_dir)
    pipeline = Pipeline(pipeline_dir, dataset, specification=specification)
    api = API(specification=specification)

    input_path = os.path.join(collection_dir, "resource", request_id)
    # List all files in the "resource" directory
    files_in_resource = os.listdir(input_path)
    os.makedirs(os.path.join(issue_dir, dataset, request_id), exist_ok=True)
    try:
        for file_name in files_in_resource:
            file_path = os.path.join(input_path, file_name)
            # retrieve unnassigned entities and assign, TODO: Is this necessary here?
            assign_entries(
                resource_path=file_path,
                dataset=dataset,
                organisation=organisation,
                pipeline_dir=pipeline_dir,
                specification=specification,
                cache_dir=cache_dir,
                endpoints=[],
            )
    except Exception as err:
        logger.error("An exception occured during assign_entries process: %s", str(err))

    # Create directories if they don't exist
    for directory in [
        collection_dir,
        issue_dir,
        column_field_dir,
        transformed_dir,
    ]:
        os.makedirs(directory, exist_ok=True)

    os.makedirs(os.path.join(transformed_dir, dataset, request_id), exist_ok=True)

    # Access each file in the "resource" directory
    for file_name in files_in_resource:
        file_path = os.path.join(input_path, file_name)

        os.makedirs(os.path.join(issue_dir, dataset, request_id), exist_ok=True)
        os.makedirs(os.path.join(column_field_dir, dataset, request_id), exist_ok=True)
        os.makedirs(
            os.path.join(dataset_resource_dir, dataset, request_id), exist_ok=True
        )
        try:
            resource = resource_from_path(file_path)
            issue_log = pipeline.transform(
                input_path=file_path,
                output_path=os.path.join(
                    transformed_dir, dataset, request_id, f"{resource}.csv"
                ),
                organisation=Organisation(
                    os.path.join(cache_dir, "organisation.csv"), Path(pipeline.path)
                ),
                resource=resource,
                valid_category_values=api.get_valid_category_values(dataset, pipeline),
                converted_path=os.path.join(
                    converted_dir, request_id, f"{resource}.csv"
                ),
                disable_lookups=True,
            )
            # Issue log needs severity column added, so manually added and saved here
            issue_log.add_severity_column(
                os.path.join(specification_dir, "issue-type.csv")
            )
            issue_log.save(
                os.path.join(issue_dir, dataset, request_id, resource + ".csv")
            )
            pipeline.save_logs(
                column_field_path=os.path.join(
                    column_field_dir, dataset, request_id, resource + ".csv"
                ),
                dataset_resource_path=os.path.join(
                    dataset_resource_dir, dataset, request_id, resource + ".csv"
                ),
            )
        except Exception as err:
            logger.error("An exception occured during Pipeline Transform: ", str(err))


def resource_from_path(path):
    return Path(path).stem


def default_output_path(command, input_path):
    directory = "" if command in ["harmonised", "transformed"] else "var/"
    return f"{directory}{command}/{resource_from_path(input_path)}.csv"


def assign_entries(
    resource_path,
    dataset,
    organisation,
    pipeline_dir,
    specification,
    cache_dir,
    endpoints=None,
):
    pipeline = Pipeline(pipeline_dir, dataset)
    resource_lookups = get_resource_unidentified_lookups(
        resource_path,
        dataset,
        organisations=[organisation],
        pipeline=pipeline,
        specification=specification,
        org_csv_path=f"{cache_dir}/organisation.csv",
        endpoints=endpoints,
    )

    unassigned_entries = []
    unassigned_entries.append(resource_lookups)

    lookups = Lookups(pipeline_dir)
    # Check if the lookups file exists, create it if not
    if not os.path.exists(lookups.lookups_path):
        with open(lookups.lookups_path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["prefix", "resource", "organisation", "reference", "entity"]
            )

    lookups.load_csv()

    # Track which entries are new by checking before adding
    new_entries_added = []
    for new_lookup in unassigned_entries:
        for idx, entry in enumerate(new_lookup):
            lookups.add_entry(entry[0])
            new_entries_added.append(entry[0])

    # save edited csvs
    max_entity_num = lookups.get_max_entity(pipeline.name, specification)
    lookups.entity_num_gen.state["current"] = max_entity_num
    lookups.entity_num_gen.state["range_max"] = specification.get_dataset_entity_max(
        dataset
    )
    lookups.entity_num_gen.state["range_min"] = specification.get_dataset_entity_min(
        dataset
    )

    newly_assigned = lookups.save_csv()

    # Filter to return only the entries we just added
    if newly_assigned:
        new_lookups = [
            lookup
            for lookup in newly_assigned
            if any(
                lookup.get("reference") == entry.get("reference")
                and lookup.get("organisation") == entry.get("organisation")
                for entry in new_entries_added
            )
        ]
        return new_lookups

    return []


def fetch_add_data_response(
    dataset,
    organisation_provider,
    pipeline_dir,
    input_dir,
    output_path,
    specification_dir,
    cache_dir,
    endpoint,
):
    try:
        specification = Specification(specification_dir)
        pipeline = Pipeline(pipeline_dir, dataset, specification=specification)
        organisation = Organisation(
            os.path.join(cache_dir, "organisation.csv"), Path(pipeline.path)
        )
        api = API(specification=specification)
        valid_category_values = api.get_valid_category_values(dataset, pipeline)

        files_in_resource = os.listdir(input_dir)

        existing_entities = []
        new_entities = []
        entity_org_mapping = []
        issues_log = None

        for idx, resource_file in enumerate(files_in_resource):
            resource_file_path = os.path.join(input_dir, resource_file)
            logger.info(
                f"Processing file {idx + 1}/{len(files_in_resource)}: {resource_file}"
            )
            try:
                # Try add data with pipeline transform to see if no entities found
                issues_log = pipeline.transform(
                    input_path=resource_file_path,
                    output_path=output_path,
                    organisation=organisation,
                    organisations=[organisation_provider],
                    resource=resource_from_path(resource_file_path),
                    valid_category_values=valid_category_values,
                    disable_lookups=False,
                    endpoints=[endpoint],
                )

                existing_entities.extend(
                    _map_transformed_entities(output_path, pipeline_dir)
                )

                # Check if there are unknown entity issues in the log
                unknown_issue_types = {
                    "unknown entity",
                    "unknown entity - missing reference",
                }
                has_unknown = any(
                    row.get("issue-type") in unknown_issue_types
                    for row in issues_log.rows
                    if isinstance(row, dict)
                )

                if has_unknown:
                    new_lookups = assign_entries(
                        resource_path=resource_file_path,
                        dataset=dataset,
                        organisation=organisation_provider,
                        pipeline_dir=pipeline_dir,
                        specification=specification,
                        cache_dir=cache_dir,
                        endpoints=[endpoint] if endpoint else None,
                    )
                    logger.info(
                        f"Found {len(new_lookups)} unidentified lookups in {resource_file}"
                    )
                    new_entities.extend(new_lookups)

                    # Default create a entity-organisation mapping, front end can use the 'authoritative' flag
                    entity_org_mapping = create_entity_organisation(
                        new_lookups, dataset, organisation_provider
                    )
                    # TODO, save to pipeline as well for rerun?

                    # Reload pipeline to pick up newly saved lookups
                    pipeline = Pipeline(
                        pipeline_dir, dataset, specification=specification
                    )

                    # Now re-run transform to check and return issue log
                    issues_log = pipeline.transform(
                        input_path=resource_file_path,
                        output_path=output_path,
                        organisation=organisation,
                        organisations=[organisation_provider],
                        resource=resource_from_path(resource_file_path),
                        valid_category_values=valid_category_values,
                        disable_lookups=False,
                        endpoints=[endpoint],
                    )
                else:
                    logger.info(f"No unidentified lookups found in {resource_file}")

            except Exception as err:
                logger.error(f"Error processing {resource_file}: {err}")
                logger.exception("Full traceback: ")

        new_entities_breakdown = _get_entities_breakdown(new_entities)
        existing_entities_breakdown = _get_existing_entities_breakdown(
            existing_entities
        )

        pipeline_summary = {
            "new-in-resource": len(new_entities),
            "existing-in-resource": len(existing_entities),
            "new-entities": new_entities_breakdown,
            "existing-entities": existing_entities_breakdown,
            "entity-organisation": entity_org_mapping,
            "pipeline-issues": [dict(issue) for issue in issues_log.rows]
            if issues_log
            else [],
        }

        return pipeline_summary

    except FileNotFoundError as e:
        logger.exception(f"File not found: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise


def _get_entities_breakdown(new_entities):
    """
    Convert newly assigned entities to the breakdown format for response.
    """
    logger.info(
        f"[get_entity_breakdown] Creating breakdown for {len(new_entities)} entities"
    )

    breakdown = []

    for entity_entry in new_entities:
        breakdown_entry = {
            "entity": str(entity_entry.get("entity", "")),
            "prefix": entity_entry.get("prefix", ""),
            "end-date": "",
            "endpoint": "",
            "resource": entity_entry.get("resource", ""),
            "reference": entity_entry.get("reference", ""),
            "entry-date": "",
            "start-date": "",
            "entry-number": "",
            "organisation": entity_entry.get("organisation", ""),
        }
        breakdown.append(breakdown_entry)

    return breakdown


def _get_existing_entities_breakdown(existing_entities):
    """
    Convert existing entities to the simplified format for response.
    """
    unique_entities = {}
    for entity_entry in existing_entities:
        entity = str(entity_entry.get("entity", "")).strip()
        reference = str(entity_entry.get("reference", "")).strip()

        if entity and reference:
            key = f"{entity}|{reference}"
            if key not in unique_entities:
                unique_entities[key] = {"entity": entity, "reference": reference}

    breakdown = list(unique_entities.values())
    return breakdown


def create_entity_organisation(new_entities, dataset, organisation):
    """
    Create entity-organisation mapping from new entities.

    Args:
        new_entities: List of entity dicts with 'entity' key
        dataset: Dataset name
        organisation: Organisation identifier

    Returns:
        List with single dict containing dataset, entity-minimum, entity-maximum, organisation
    """
    if not new_entities:
        return []

    entity_values = [
        entry.get("entity") for entry in new_entities if entry.get("entity")
    ]

    if not entity_values:
        return []

    return [
        {
            "dataset": dataset,
            "entity-minimum": min(entity_values),
            "entity-maximum": max(entity_values),
            "organisation": organisation,
        }
    ]


def _map_transformed_entities(transformed_csv_path, pipeline_dir):  # noqa: C901
    """Extract unique entities from transformed CSV and lookup their details in lookup.csv."""

    mapped_entities = []

    if not os.path.exists(transformed_csv_path):
        logger.warning(f"Transformed CSV not found: {transformed_csv_path}")
        return mapped_entities

    # Extract unique entity values from transformed CSV
    unique_entities = set()
    try:
        with open(transformed_csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                entity_val = row.get("entity", "").strip()
                if entity_val:  # Skip empty entities
                    unique_entities.add(entity_val)
    except Exception as e:
        logger.error(f"Error reading transformed CSV: {e}")
        return mapped_entities

    if not unique_entities:
        return mapped_entities

    # Load lookup.csv to get entity details
    lookup_path = os.path.join(pipeline_dir, "lookup.csv")
    if not os.path.exists(lookup_path):
        logger.warning(f"Lookup CSV not found: {lookup_path}")
        return mapped_entities

    entity_lookup_map = {}
    with open(lookup_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            entity_lookup_map[str(row.get("entity", ""))] = row

    # Map entities to their full details
    for entity_id in unique_entities:
        row = entity_lookup_map.get(entity_id, {})
        if row:  # Only add if found in lookup
            mapped_entities.append(
                {
                    "entity": entity_id,
                    "reference": row.get("reference", ""),
                    "prefix": row.get("prefix", ""),
                    "resource": row.get("resource", ""),
                    "organisation": row.get("organisation", ""),
                }
            )

    return mapped_entities
