import os
import csv
import json
import yaml
from datetime import date
from application.logging.logger import get_logger
from digital_land.organisation import Organisation
from digital_land.api import API

from digital_land.pipeline import Pipeline, Lookups
from digital_land.pipeline.task import TaskPipeline, TaskPipelineStatus
from digital_land.commands import get_resource_unidentified_lookups
from pathlib import Path

from application.core.duplicates import find_duplicate_redirect_candidates

logger = get_logger(__name__)


def _reference_set(references):
    """Return normalised reference values from an optional request list."""
    if isinstance(references, str):
        references = [references]
    return {
        str(
            reference.get("reference") if isinstance(reference, dict) else reference
        ).strip()
        for reference in references or []
        if reference
    }


def _filter_selected_entities(new_entities, excluded_references):
    """
    Return entities not listed in excluded_references.

    This mirrors the add-data request contract: excluded_references=None
    or [] means every new entity is selected. A non-empty list removes only
    entities with matching references from selected outputs.
    """
    excluded_references = _reference_set(excluded_references)
    if not excluded_references:
        return new_entities

    return [
        entity
        for entity in new_entities
        if str(entity.get("reference", "")).strip() not in excluded_references
    ]


def _create_old_entity_redirects(
    new_entities,
    selected_redirects,
    excluded_references=None,
    duplicate_candidates=None,
):
    """
    Build old-entity rows from explicit redirect selections.

    selected_redirects is a list of objects containing a new entity reference
    and old entity number. Duplicate candidates provide the evidence note.
    References excluded by excluded_references cannot be redirected.
    """
    if not selected_redirects:
        return []

    selected_new_entities = _filter_selected_entities(new_entities, excluded_references)
    selected_entity_ids = {
        str(entity.get("entity", "")).strip() for entity in selected_new_entities
    }

    old_entity_rows = []
    seen = set()
    evidence_by_redirect = {
        (
            str(candidate.get("new_reference", "")).strip(),
            str(candidate.get("old_entity", "")).strip(),
            str(candidate.get("entity", "")).strip(),
        ): str(candidate.get("evidence", "") or "")
        for candidate in duplicate_candidates or []
    }

    entity_by_reference = {
        str(entity.get("reference", "")).strip(): str(entity.get("entity", "")).strip()
        for entity in selected_new_entities
    }

    for redirect in selected_redirects:
        reference = str(redirect.get("reference", "")).strip()
        old_entity = str(redirect.get("old_entity_number", "")).strip()
        entity = entity_by_reference.get(reference, "")
        if not old_entity or not entity:
            continue

        notes = evidence_by_redirect.get((reference, old_entity, entity), "")
        row = _old_entity_row(
            old_entity,
            entity,
            notes=notes,
        )
        if not row:
            continue

        row_key = (row["old-entity"], row["entity"])
        if row_key in seen:
            continue
        seen.add(row_key)
        old_entity_rows.append(row)

    return old_entity_rows


def _old_entity_row(old_entity, entity, notes=""):
    """Return a valid old-entity redirect row, or None for incomplete ids."""
    old_entity = str(old_entity or "").strip()
    entity = str(entity or "").strip()
    if not old_entity or not entity:
        return None

    return {
        "old-entity": old_entity,
        "status": "301",
        "entity": entity,
        "entry-date": date.today().isoformat(),
        "notes": str(notes or ""),
    }


def _name_similarity_score(candidate):
    """Parse a duplicate candidate name similarity value like '86%'."""
    similarity = str(candidate.get("name_similarity", "") or "").strip()
    if similarity.endswith("%"):
        similarity = similarity[:-1]
    try:
        return int(float(similarity))
    except ValueError:
        return 0


def _should_auto_redirect(candidate):
    """Return True for duplicate candidates safe enough to redirect automatically."""
    match_type = candidate.get("match_type")
    if match_type == "complete_match":
        return True
    return match_type == "single_match" and _name_similarity_score(candidate) > 85


def _create_auto_old_entity_redirects(duplicate_candidates, selected_entity_ids=None):
    """
    Build old-entity rows for duplicate matches that meet the auto-redirect rules.

    Complete matches always qualify. Single matches qualify only above 85%.
    Existing redirects are skipped, and selected_entity_ids limits redirects to
    the user's selected new entities when a non-empty selection was supplied.
    """
    old_entity_rows = []
    for candidate in duplicate_candidates:
        entity = str(candidate.get("entity", "")).strip()
        if selected_entity_ids is not None and entity not in selected_entity_ids:
            continue
        if candidate.get("old_entity_redirects"):
            continue
        if not _should_auto_redirect(candidate):
            continue

        row = _old_entity_row(
            candidate.get("old_entity"),
            entity,
            notes=candidate.get("evidence", ""),
        )
        if row:
            old_entity_rows.append(row)

    return old_entity_rows


def _merge_old_entity_rows(*row_groups):
    """Merge old-entity row groups, keeping the first row for each old/new pair."""
    rows = []
    seen = set()
    for row_group in row_groups:
        for row in row_group:
            key = (row.get("old-entity"), row.get("entity"))
            if key in seen:
                continue
            seen.add(key)
            rows.append(row)
    return rows


def load_mappings():
    """Load task summary templates keyed by field and issue type."""
    mappings_file_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)),
        "../application/configs/mapping.yaml",
    )
    with open(mappings_file_path, "r") as yaml_file:
        mappings_data = yaml.safe_load(yaml_file)
    mappings = mappings_data.get("mappings", [])
    return {(mapping["field"], mapping["issue-type"]): mapping for mapping in mappings}


def _format_task_summary(details_str, task_source, mappings):
    """Create a human-readable task summary from task log detail JSON."""
    try:
        details = json.loads(details_str)
    except (json.JSONDecodeError, TypeError):
        return ""

    issue_type = details.get("issue_type", "")
    field = details.get("field", "")
    count = details.get("count", 1)

    if task_source == "column-field":
        return f"{field.capitalize()} column missing" if field else ""

    mapping = mappings.get((field, issue_type))
    if mapping:
        template = (
            mapping.get("summary-plural", "")
            if count > 1
            else mapping.get("summary-singular", "")
        )
        if template:
            return template.format(count=count, issue_type=issue_type, field=field)

    return ""


def run_task_pipeline(
    task_log_path,
    dataset,
    organisation,
    issue_path,
    column_field_path=None,
    mandatory_fields=None,
):
    """Run the task pipeline and attach generated summary text to each task row."""
    task_pipeline = TaskPipeline()
    status = task_pipeline.run(
        output_path=task_log_path,
        dataset=dataset,
        organisation=organisation,
        issue_path=issue_path,
        column_field_path=column_field_path,
        mandatory_fields=mandatory_fields,
    )
    if status == TaskPipelineStatus.FAILED:
        raise RuntimeError(f"TaskPipeline failed for dataset '{dataset}'")

    mappings = load_mappings()
    task_log = []
    if os.path.isfile(task_log_path):
        with open(task_log_path, "r") as f:
            task_log = list(csv.DictReader(f))

    for task in task_log:
        task["summary"] = _format_task_summary(
            task.get("details", ""), task.get("task-source", ""), mappings
        )
    return task_log


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
    specification,
    cache_dir,
    additional_col_mappings,
    additional_concats,
):
    """
    Run the standard request pipeline and write transformed data plus issue logs.

    This path is used by non-add-data workflows. It assigns unknown entities
    before transforming, then saves issue, column-field, and dataset-resource
    logs for each uploaded resource.
    """
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
            _assign_entries(
                resource_path=file_path,
                dataset=dataset,
                organisation=organisation,
                pipeline_dir=pipeline_dir,
                specification=specification,
                cache_dir=cache_dir,
                endpoints=[],
            )
    except Exception as err:
        logger.exception("An exception occurred when assigning entries: %s", err)
        raise

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
                output_path=Path(
                    os.path.join(
                        transformed_dir, dataset, request_id, f"{resource}.csv"
                    )
                ),
                organisation=Organisation(
                    os.path.join(cache_dir, "organisation.csv"), Path(pipeline.path)
                ),
                resource=resource,
                valid_category_values=api.get_valid_category_values(dataset, pipeline),
                converted_path=Path(
                    os.path.join(converted_dir, request_id, f"{resource}.csv")
                ),
                disable_lookups=True,
            )
            # Issue log needs severity column added, so manually added and saved here
            issue_log.add_severity_column(severity_mapping=specification.issue_type)
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
            logger.exception("An exception occurred during Pipeline Transform: %s", err)
            raise


def resource_from_path(path):
    """Return the resource hash/name from an uploaded resource file path."""
    return Path(path).stem


def _assign_entries(
    resource_path,
    dataset,
    organisation,
    pipeline_dir,
    specification,
    cache_dir,
    endpoints=None,
    excluded_references=None,
):
    """
    Assign entity numbers for unidentified lookups in a resource.

    Rows not listed in excluded_references are added to lookup.csv first so
    selected rows receive the lowest new entity numbers. Excluded rows are
    still added afterwards; selection affects ordering, not whether a row is
    assigned.
    """
    excluded_references = _reference_set(excluded_references)
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
    selected_entries = []
    other_entries = []
    for new_lookup in unassigned_entries:
        for entry in new_lookup:
            entry_reference = str(entry[0].get("reference", "")).strip()
            if entry_reference in excluded_references:
                other_entries.append(entry[0])
            else:
                selected_entries.append(entry[0])

    for entry in selected_entries:
        lookups.add_entry(entry)
        new_entries_added.append(entry)
    for entry in other_entries:
        lookups.add_entry(entry)
        new_entries_added.append(entry)

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


def _transform_add_data_resource(
    *,
    pipeline,
    resource_file_path,
    output_path,
    organisation,
    organisations,
    valid_category_values,
    converted_path,
    endpoints,
    dataset,
    pipeline_dir,
    specification,
    cache_dir,
    excluded_references,
):
    """
    Transform one add-data resource and assign entities when unknown rows exist.

    All new rows are assigned entity numbers, but rows not listed in
    excluded_references are prioritised and used for the
    entity-organisation summary.
    """
    issues_log = pipeline.transform(
        input_path=resource_file_path,
        output_path=output_path,
        organisation=organisation,
        organisations=organisations,
        resource=resource_from_path(resource_file_path),
        valid_category_values=valid_category_values,
        disable_lookups=False,
        endpoints=endpoints,
        converted_path=converted_path,
    )

    existing_entities = _map_transformed_entities(output_path, pipeline_dir)
    unknown_issue_types = {
        "unknown entity",
        "unknown entity - missing reference",
    }
    has_unknown = any(
        row.get("issue-type") in unknown_issue_types
        for row in issues_log.rows
        if isinstance(row, dict)
    )

    if not has_unknown:
        return pipeline, issues_log, existing_entities, [], []

    new_lookups = _assign_entries(
        resource_path=resource_file_path,
        dataset=dataset,
        organisation=organisations[0],
        pipeline_dir=pipeline_dir,
        specification=specification,
        cache_dir=cache_dir,
        endpoints=endpoints if endpoints else None,
        excluded_references=excluded_references,
    )
    entity_org_mapping = _create_entity_organisation(
        _filter_selected_entities(new_lookups, excluded_references),
        dataset,
        organisations[0],
        pipeline_dir,
    )

    # Reload pipeline to pick up newly saved lookups before rerunning transform.
    pipeline = Pipeline(pipeline_dir, dataset, specification=specification)
    issues_log = pipeline.transform(
        input_path=resource_file_path,
        output_path=output_path,
        organisation=organisation,
        organisations=organisations,
        resource=resource_from_path(resource_file_path),
        valid_category_values=valid_category_values,
        disable_lookups=False,
        endpoints=endpoints,
        converted_path=converted_path,
    )

    return pipeline, issues_log, existing_entities, new_lookups, entity_org_mapping


def _process_add_data_resource(resource_file, **kwargs):
    """Wrap one add-data resource transform with resource-specific logging."""
    try:
        return _transform_add_data_resource(**kwargs)
    except Exception as err:
        logger.exception(f"Error processing {resource_file}: {err}")
        raise


def _find_duplicate_candidates(
    dataset,
    specification,
    output_path,
    redirect_lookups,
    organisation_provider,
    organisation_index,
):
    """
    Find possible redirects for the transformed add-data output.

    Duplicate analysis should not block add-data processing, so failures are
    logged and returned as an empty candidate list.
    """
    try:
        return find_duplicate_redirect_candidates(
            dataset=dataset,
            specification=specification,
            transformed_csv_path=output_path,
            redirect_lookups=redirect_lookups,
            organisation_provider=organisation_provider,
            organisation_index=organisation_index,
        )
    except Exception as err:
        logger.exception("Duplicate analysis failed for dataset %s: %s", dataset, err)
        return []


def fetch_add_data_response(
    dataset,
    organisation_provider,
    pipeline_dir,
    input_dir,
    output_path,
    specification,
    cache_dir,
    endpoint,
    converted_path=None,
    excluded_references=None,
    selected_redirects=None,
):
    """
    Run the add-data pipeline transform and build the pipeline summary response.

    This is reached via POST /requests with type "add_data" through the
    AddDataTask and add_data_workflow. Processing exceptions are re-raised so
    add_data_workflow can return them in the standard async error response.

    excluded_references controls summary filtering and assignment
    order: None or [] means all new entities are reported as new-entities,
    while a non-empty list excludes those references from selected outputs.
    selected_redirects is a list of reference/old entity number objects used
    to create explicit old-entity redirect rows; None or [] means no manual
    redirects.
    """
    try:
        pipeline = Pipeline(pipeline_dir, dataset, specification=specification)
        organisation = Organisation(
            os.path.join(cache_dir, "organisation.csv"), Path(pipeline.path)
        )
        api = API(specification=specification)
        valid_category_values = api.get_valid_category_values(dataset, pipeline)

        files_in_resource = os.listdir(input_dir)
        organisations = (
            organisation_provider
            if isinstance(organisation_provider, list)
            else [organisation_provider]
        )
        organisations = [organisation for organisation in organisations if organisation]
        if not organisations:
            raise ValueError("At least one organisation is required for add_data")

        endpoints = endpoint if isinstance(endpoint, list) else [endpoint]
        endpoints = [endpoint for endpoint in endpoints if endpoint]

        existing_entities = []
        new_entities = []
        entity_org_mapping = []
        issues_log = None

        for idx, resource_file in enumerate(files_in_resource):
            resource_file_path = os.path.join(input_dir, resource_file)
            logger.info(
                f"Processing file {idx + 1}/{len(files_in_resource)}: {resource_file}"
            )
            (
                pipeline,
                issues_log,
                transformed_entities,
                new_lookups,
                new_entity_org_mapping,
            ) = _process_add_data_resource(
                resource_file,
                pipeline=pipeline,
                resource_file_path=resource_file_path,
                output_path=output_path,
                organisation=organisation,
                organisations=organisations,
                valid_category_values=valid_category_values,
                converted_path=converted_path,
                endpoints=endpoints,
                dataset=dataset,
                pipeline_dir=pipeline_dir,
                specification=specification,
                cache_dir=cache_dir,
                excluded_references=excluded_references,
            )

            existing_entities.extend(transformed_entities)
            if new_lookups:
                logger.info(
                    f"Found {len(new_lookups)} unidentified lookups in {resource_file}"
                )
                new_entities.extend(new_lookups)
                entity_org_mapping = new_entity_org_mapping
            else:
                logger.info(f"No unidentified lookups found in {resource_file}")

        selected_new_entities = _filter_selected_entities(
            new_entities, excluded_references
        )
        new_entities_breakdown = _get_entities_breakdown(selected_new_entities)
        existing_entities_breakdown = _get_existing_entities_breakdown(
            existing_entities
        )
        duplicate_candidates = _find_duplicate_candidates(
            dataset,
            specification,
            output_path,
            pipeline.redirect_lookups(),
            organisations[0],
            organisation,
        )
        selected_entity_ids = (
            {str(entity.get("entity")) for entity in selected_new_entities}
            if excluded_references is not None and excluded_references != []
            else None
        )
        old_entity_rows = _merge_old_entity_rows(
            _create_auto_old_entity_redirects(
                duplicate_candidates, selected_entity_ids=selected_entity_ids
            ),
            _create_old_entity_redirects(
                new_entities,
                selected_redirects,
                excluded_references,
                duplicate_candidates=duplicate_candidates,
            ),
        )

        if issues_log:
            issues_log.add_severity_column(severity_mapping=specification.issue_type)

        pipeline_summary = {
            "new-in-resource": len(new_entities),
            "existing-in-resource": len(existing_entities),
            "new-entities": new_entities_breakdown,
            "existing-entities": existing_entities_breakdown,
            "entity-organisation": entity_org_mapping,
            "old-entity": old_entity_rows,
            "duplicate-candidates": duplicate_candidates,
            "pipeline-issues": (
                [dict(issue) for issue in issues_log.rows] if issues_log else []
            ),
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


def _create_entity_organisation(  # noqa: C901
    new_entities, dataset, organisation, pipeline_dir
):
    """
    Create entity-organisation mapping from new entities.

    checks whether the new entities already fall within an existing entity-minimum/
    entity-maximum range for this dataset. If the CSV can't be loaded,
    processing continues but the returned mapping is flagged with error.

    """
    if not new_entities:
        return []

    entity_values = [
        int(entry.get("entity"))
        for entry in new_entities
        if entry.get("entity") is not None
    ]

    if not entity_values:
        return []

    entity_org_csv_path = os.path.join(pipeline_dir, "entity-organisation.csv")
    try:
        with open(entity_org_csv_path, "r", encoding="utf-8") as f:
            existing_rows = list(csv.DictReader(f))
        error = False
    except (OSError, csv.Error) as err:
        logger.warning(f"Unable to load entity-organisation.csv: {err}")
        existing_rows = []
        error = True

    overlap = False
    if not error:
        for row in existing_rows:
            if row.get("dataset") != dataset:
                continue
            try:
                row_min = int(row.get("entity-minimum"))
                row_max = int(row.get("entity-maximum"))
            except (TypeError, ValueError):
                continue
            if all(row_min <= value <= row_max for value in entity_values):
                overlap = True
                break

    entry = {
        "dataset": dataset,
        "organisation": organisation,
        "overlap": overlap,
        "error": error,
    }
    # Omit the range when it can't be trusted, so a downstream consumer
    # can't blindly commit it to entity-organisation.csv
    if not overlap and not error:
        entry["entity-minimum"] = min(entity_values)
        entry["entity-maximum"] = max(entity_values)

    return [entry]


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
