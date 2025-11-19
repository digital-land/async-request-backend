import os
import csv
from application.logging.logger import get_logger
from digital_land.specification import Specification
from digital_land.log import DatasetResourceLog, IssueLog, ColumnFieldLog
from digital_land.organisation import Organisation
from digital_land.phase.combine import FactCombinePhase
from digital_land.phase.concat import ConcatFieldPhase
from digital_land.phase.convert import ConvertPhase
from digital_land.phase.default import DefaultPhase
from digital_land.phase.factor import FactorPhase
from digital_land.phase.filter import FilterPhase
from digital_land.phase.harmonise import HarmonisePhase
from digital_land.phase.lookup import (
    EntityLookupPhase,
    FactLookupPhase,
)

from digital_land.phase.map import MapPhase
from digital_land.phase.migrate import MigratePhase
from digital_land.phase.normalise import NormalisePhase
from digital_land.phase.organisation import OrganisationPhase
from digital_land.phase.parse import ParsePhase
from digital_land.phase.patch import PatchPhase
from digital_land.phase.pivot import PivotPhase
from digital_land.phase.prefix import EntityPrefixPhase
from digital_land.phase.prune import FieldPrunePhase, FactPrunePhase
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.save import SavePhase
from digital_land.phase.priority import PriorityPhase
from digital_land.pipeline import run_pipeline, Pipeline, Lookups
from digital_land.commands import get_resource_unidentified_lookups
from digital_land.check import duplicate_reference_check
from digital_land.api import API

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
    # define variables for Pipeline and specification
    pipeline = Pipeline(pipeline_dir, dataset)
    specification = Specification(specification_dir)

    input_path = os.path.join(collection_dir, "resource", request_id)
    # List all files in the "resource" directory
    files_in_resource = os.listdir(input_path)
    os.makedirs(os.path.join(issue_dir, dataset, request_id), exist_ok=True)
    try:
        for file_name in files_in_resource:
            file_path = os.path.join(input_path, file_name)
            # retrieve unnassigned entities and assign
            assign_entries(
                resource_path=file_path,
                dataset=dataset,
                organisation=organisation,
                pipeline_dir=pipeline_dir,
                specification=specification,
                cache_dir=cache_dir,
            )
    except Exception as err:
        logger.error("An exception occured during assign_entries process: ", str(err))

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
            pipeline_run(
                dataset=dataset,
                pipeline=pipeline,
                request_id=request_id,
                specification_dir=specification_dir,
                input_path=file_path,
                output_path=os.path.join(
                    transformed_dir, dataset, request_id, f"{file_name}.csv"
                ),
                issue_dir=os.path.join(issue_dir, dataset, request_id),
                column_field_dir=os.path.join(column_field_dir, dataset, request_id),
                dataset_resource_dir=os.path.join(
                    dataset_resource_dir, dataset, request_id
                ),
                organisation_path=os.path.join(cache_dir, "organisation.csv"),
                save_harmonised=False,
                organisations=[organisation],
                converted_dir=converted_dir,
            )
        except Exception as err:
            logger.error("An exception occured during pipeline_run: ", str(err))


def pipeline_run(
    dataset,
    pipeline,
    request_id,
    specification_dir,
    input_path,
    output_path,
    organisations,
    converted_dir,
    null_path=None,  # TBD: remove this
    issue_dir=None,
    organisation_path=None,
    save_harmonised=False,
    column_field_dir=None,
    dataset_resource_dir=None,
    endpoints=[],
    entry_date="",
):
    resource = resource_from_path(input_path)

    specification = Specification(specification_dir)
    schema = specification.pipeline[pipeline.name]["schema"]
    intermediate_fieldnames = specification.intermediate_fieldnames(pipeline)
    issue_log = IssueLog(dataset=dataset, resource=resource)
    column_field_log = ColumnFieldLog(dataset=dataset, resource=resource)
    dataset_resource_log = DatasetResourceLog(dataset=dataset, resource=resource)

    api = API(specification=specification)
    # load pipeline configuration
    skip_patterns = pipeline.skip_patterns(resource)
    columns = pipeline.columns(resource, endpoints=endpoints)
    concats = pipeline.concatenations(resource, endpoints=endpoints)
    patches = pipeline.patches(resource=resource)
    lookups = pipeline.lookups(resource=resource)
    default_fields = pipeline.default_fields(resource=resource)
    default_values = pipeline.default_values(endpoints=endpoints)
    combine_fields = pipeline.combine_fields(endpoints=endpoints)

    # load organisations
    organisation = Organisation(organisation_path, Path(pipeline.path))

    severity_csv_path = os.path.join(specification_dir, "issue-type.csv")

    # Load valid category values
    valid_category_values = api.get_valid_category_values(dataset, pipeline)
    # resource specific default values
    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
            output_path=os.path.join(converted_dir, request_id, f"{resource}.csv"),
        ),
        NormalisePhase(skip_patterns=skip_patterns, null_path=null_path),
        ParsePhase(),
        ConcatFieldPhase(concats=concats, log=column_field_log),
        MapPhase(
            fieldnames=intermediate_fieldnames,
            columns=columns,
            log=column_field_log,
        ),
        FilterPhase(filters=pipeline.filters(resource)),
        PatchPhase(
            issues=issue_log,
            patches=patches,
        ),
        HarmonisePhase(
            field_datatype_map=specification.get_field_datatype_map(),
            issues=issue_log,
            dataset=dataset,
            valid_category_values=valid_category_values,
        ),
        DefaultPhase(
            default_fields=default_fields,
            default_values=default_values,
            issues=issue_log,
        ),
        # TBD: move migrating columns to fields to be immediately after map
        # this will simplify harmonisation and remove intermediate_fieldnames
        # but effects brownfield-land and other pipelines which operate on columns
        MigratePhase(
            fields=specification.schema_field[schema],
            migrations=pipeline.migrations(),
        ),
        OrganisationPhase(organisation=organisation, issues=issue_log),
        FieldPrunePhase(fields=specification.current_fieldnames(schema)),
        EntityReferencePhase(
            dataset=dataset,
            prefix=specification.dataset_prefix(dataset),
        ),
        EntityPrefixPhase(dataset=dataset),
        EntityLookupPhase(lookups),
        SavePhase(
            default_output_path("harmonised", input_path),
            fieldnames=intermediate_fieldnames,
            enabled=save_harmonised,
        ),
        PriorityPhase(config=None),
        PivotPhase(),
        FactCombinePhase(issue_log=issue_log, fields=combine_fields),
        FactorPhase(),
        FactReferencePhase(
            field_typology_map=specification.get_field_typology_map(),
            field_prefix_map=specification.get_field_prefix_map(),
        ),
        FactLookupPhase(
            lookups=lookups,
            odp_collections=specification.get_odp_collections(),
        ),
        FactPrunePhase(),
        SavePhase(
            output_path,
            fieldnames=specification.factor_fieldnames(),
        ),
    )

    issue_log = duplicate_reference_check(issues=issue_log, csv_path=output_path)

    # Add the 'severity' and 'description' column based on the mapping
    issue_log.add_severity_column(severity_csv_path)

    issue_log.save(os.path.join(issue_dir, resource + ".csv"))
    column_field_log.save(os.path.join(column_field_dir, resource + ".csv"))
    dataset_resource_log.save(os.path.join(dataset_resource_dir, resource + ".csv"))


def resource_from_path(path):
    return Path(path).stem


def default_output_path(command, input_path):
    directory = "" if command in ["harmonised", "transformed"] else "var/"
    return f"{directory}{command}/{resource_from_path(input_path)}.csv"


def assign_entries(
    resource_path, dataset, organisation, pipeline_dir, specification, cache_dir
):
    pipeline = Pipeline(pipeline_dir, dataset)
    resource_lookups = get_resource_unidentified_lookups(
        resource_path,
        dataset,
        organisations=[organisation],
        pipeline=pipeline,
        specification=specification,
        org_csv_path=f"{cache_dir}/organisation.csv",
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
    for new_lookup in unassigned_entries:
        for idx, entry in enumerate(new_lookup):
            lookups.add_entry(entry[0])

    # save edited csvs
    max_entity_num = lookups.get_max_entity(pipeline.name, specification)
    lookups.entity_num_gen.state["current"] = max_entity_num
    lookups.entity_num_gen.state["range_max"] = specification.get_dataset_entity_max(
        dataset
    )
    lookups.entity_num_gen.state["range_min"] = specification.get_dataset_entity_min(
        dataset
    )

    lookups.save_csv()


def fetch_add_data_response( dataset, organisation, pipeline_dir, input_path, specification_dir, cache_dir):
    try:
        specification = Specification(specification_dir)

        if not os.path.exists(input_path):
            error_msg = f"Input path does not exist: {input_path}"
            logger.error(f"ERROR: {error_msg}")
            raise FileNotFoundError(error_msg)

        files_in_resource = os.listdir(input_path)
        logger.info(f"Total files: {len(files_in_resource)}")

        response_data = {}

        if not files_in_resource:
            logger.warning(f"No files found in {input_path}")
            return response_data

        new_entities = []
        existing_entities = []
        lookup_path = os.path.join(pipeline_dir, 'lookup.csv')

        for idx, resource_file in enumerate(files_in_resource):
            resource_file_path = os.path.join(input_path, resource_file)
            logger.info(
                f"Processing file {idx + 1}/{len(files_in_resource)}: {resource_file}")
            try:
                unidentified_lookups = _add_data_read_entities(
                    resource_file_path,
                    dataset,
                    organisation,
                    specification
                )

                if not unidentified_lookups:
                    logger.info(f"No references found in {resource_file}")
                    continue

                new_lookups, existing_lookups = _check_existing_entities(unidentified_lookups, lookup_path)
                existing_entities.extend(existing_lookups)

                if not new_lookups:
                    logger.info(f"All lookups already exist for {resource_file}")
                    continue

                newly_assigned = _assign_entity_numbers(
                    new_lookups,
                    pipeline_dir,
                    dataset,
                    specification
                )

                new_entities.extend(newly_assigned)
                logger.info(
                    f"Assigned {len(newly_assigned)} new entities for {resource_file}")

            except Exception as err:
                logger.error(f"Error processing {resource_file}: {err}")
                logger.exception(f"Full traceback: ")

        new_entities_breakdown = _get_entities_breakdown(new_entities)
        existing_entities_breakdown = _get_existing_entities_breakdown(existing_entities)

        entity_summary = {
            "new-in-resource": len(new_entities),
            "existing-in-resource": len(existing_entities),
            "new-entities": new_entities_breakdown,
            "existing-entities": existing_entities_breakdown
        }

        response_data = {
            "entity-summary": entity_summary,
        }

        return response_data

    except FileNotFoundError as e:
        logger.exception(f"File not found: {e}")
        raise
    except Exception as e:
        logger.exception(f"Unexpected error: {e}")
        raise

def _add_data_read_entities(resource_path, dataset, organisation, specification):

        unidentified_lookups = []
        dataset_prefix = specification.dataset_prefix(dataset)

        try:
            with open(resource_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)

                for idx, row in enumerate(reader, start=1):
                    reference = row.get('reference', '').strip()

                    if not reference:
                        logger.warning(f"[read_resource_references] Row {idx} has no reference, skipping")
                        continue

                    lookup_entry = {
                        'prefix': dataset_prefix,
                        'organisation': organisation,
                        'reference': reference,
                        'resource': Path(resource_path).stem,
                        'entity': ''
                    }

                    unidentified_lookups.append(lookup_entry)

            logger.info(f"Found {len(unidentified_lookups)} references")

        except Exception as e:
            logger.error(f"Error reading resource: {e}")
            raise

        return unidentified_lookups


def _check_existing_entities(unidentified_lookups, lookup_path):
        """
        Check which lookups already exist in lookup file
        """
        existing_lookup_map = {}

        if os.path.exists(lookup_path):
            try:
                with open(lookup_path, 'r', encoding='utf-8') as f:
                    reader = csv.DictReader(f)

                    for row in reader:
                        prefix = row.get('prefix', '').strip()
                        org = row.get('organisation', '').strip()
                        ref = row.get('reference', '').strip()
                        entity = row.get('entity', '').strip()

                        if prefix and org and ref and entity:
                            key = f"{prefix},{org},{ref}"
                            existing_lookup_map[key] = {
                                "entity": entity,
                                "reference": ref
                            }

            except Exception as e:
                logger.error(f"Error reading lookup file: {e}")
        else:
            logger.info(f"lookup file does not exist yet")

        new_lookups = []
        existing_lookups = []
        for lookup in unidentified_lookups:
            key = f"{lookup['prefix']},{lookup['organisation']},{lookup['reference']}"

            if key in existing_lookup_map:
                existing_lookups.append(existing_lookup_map[key])
            else:
                new_lookups.append(lookup)

        logger.info(
            f"Found {len(new_lookups)} new lookups and {len(existing_lookups)} existing lookups")

        return new_lookups, existing_lookups

def _assign_entity_numbers(new_lookups, pipeline_dir, dataset, specification):
        """
        Assign entity numbers to new lookup entries and save to lookup file
        """

        lookups = Lookups(pipeline_dir)

        if not os.path.exists(lookups.lookups_path):
            os.makedirs(os.path.dirname(lookups.lookups_path), exist_ok=True)
            with open(lookups.lookups_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow(['prefix', 'resource', 'organisation', 'reference', 'entity'])

        lookups.load_csv()

        for lookup in new_lookups:
            lookups.add_entry(lookup)

        max_entity_num = lookups.get_max_entity(dataset, specification)
        logger.info(f"Max existing entity: {max_entity_num}")

        lookups.entity_num_gen.state['current'] = max_entity_num
        lookups.entity_num_gen.state['range_max'] = specification.get_dataset_entity_max(dataset)
        lookups.entity_num_gen.state['range_min'] = specification.get_dataset_entity_min(dataset)

        logger.info(
            f"Entity range: {lookups.entity_num_gen.state['range_min']} - {lookups.entity_num_gen.state['range_max']}")

        newly_assigned = lookups.save_csv()

        return newly_assigned

def _get_entities_breakdown(new_entities):
        """
        Convert newly assigned entities to the breakdown format for response.
        """
        logger.info(f"[get_entity_breakdown] Creating breakdown for {len(new_entities)} entities")

        breakdown = []

        for entity_entry in new_entities:
            breakdown_entry = {
                "entity": str(entity_entry.get('entity', '')),
                "prefix": entity_entry.get('prefix', ''),
                "end-date": "",
                "endpoint": "",
                "resource": entity_entry.get('resource', ''),
                "reference": entity_entry.get('reference', ''),
                "entry-date": "",
                "start-date": "",
                "entry-number": "",
                "organisation": entity_entry.get('organisation', '')
            }
            breakdown.append(breakdown_entry)

        return breakdown

def _get_existing_entities_breakdown(existing_entities):
        """
        Convert existing entities to the simplified format for response.
        """
        unique_entities = {}
        for entity_entry in existing_entities:
            entity = str(entity_entry.get('entity', '')).strip()
            reference = str(entity_entry.get('reference', '')).strip()

            if entity and reference:
                key = f"{entity}|{reference}"
                if key not in unique_entities:
                    unique_entities[key] = {
                        "entity": entity,
                        "reference": reference
                    }

        breakdown = list(unique_entities.values())
        return breakdown
