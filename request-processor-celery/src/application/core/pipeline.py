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
from digital_land.phase.prune import FieldPrunePhase, EntityPrunePhase, FactPrunePhase
from digital_land.phase.reference import EntityReferencePhase, FactReferencePhase
from digital_land.phase.save import SavePhase
from digital_land.pipeline import run_pipeline, Pipeline, Lookups
from digital_land.commands import get_resource_unidentified_lookups

from pathlib import Path

logger = get_logger(__name__)


def fetch_response_data(
    dataset,
    organisation,
    collection_dir,
    issue_dir,
    column_field_dir,
    transformed_dir,
    flattened_dir,
    dataset_dir,
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

    input_path = os.path.join(collection_dir, "resource")
    # List all files in the "resource" directory
    files_in_resource = os.listdir(input_path)
    os.makedirs(os.path.join(issue_dir, dataset), exist_ok=True)
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
                cache_dir=cache_dir
            )
    except Exception as err:
        logger.error("An exception occured during assign_entries process: ", str(err))

    # Create directories if they don't exist
    for directory in [
        collection_dir,
        issue_dir,
        column_field_dir,
        transformed_dir,
        flattened_dir,
        dataset_dir,
    ]:
        os.makedirs(directory, exist_ok=True)

    os.makedirs(os.path.join(transformed_dir, dataset))
    os.makedirs(os.path.join(flattened_dir, dataset))

    # Access each file in the "resource" directory
    for file_name in files_in_resource:
        file_path = os.path.join(input_path, file_name)

        os.makedirs(os.path.join(issue_dir, dataset), exist_ok=True)
        os.makedirs(os.path.join(column_field_dir, dataset), exist_ok=True)
        os.makedirs(os.path.join(dataset_resource_dir, dataset), exist_ok=True)
        try:
            pipeline_run(
                dataset=dataset,
                pipeline=pipeline,
                specification_dir=specification_dir,
                input_path=file_path,
                output_path=os.path.join(transformed_dir, dataset, f"{file_name}.csv"),
                issue_dir=os.path.join(issue_dir, dataset),
                column_field_dir=os.path.join(column_field_dir, dataset),
                dataset_resource_dir=os.path.join(dataset_resource_dir, dataset),
                organisation_path=os.path.join(cache_dir, "organisation.csv"),
                save_harmonised=False,
                organisations=[organisation],
                custom_temp_dir=os.path.join(cache_dir),
            )
        except Exception as err:
            logger.error("An exception occured during pipeline_run: ", str(err))


def pipeline_run(
    dataset,
    pipeline,
    specification_dir,
    input_path,
    output_path,
    organisations,
    null_path=None,  # TBD: remove this
    issue_dir=None,
    organisation_path=None,
    save_harmonised=False,
    column_field_dir=None,
    dataset_resource_dir=None,
    custom_temp_dir=None,  # TBD: rename to "tmpdir"
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
    # resource specific default values
    if len(organisations) == 1:
        default_values["organisation"] = organisations[0]

    run_pipeline(
        ConvertPhase(
            path=input_path,
            dataset_resource_log=dataset_resource_log,
            custom_temp_dir=custom_temp_dir,
            output_path=os.path.join("converted", f"{resource}.csv"),
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
        HarmonisePhase(specification=specification, issues=issue_log, dataset=dataset),
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
            specification=specification,
        ),
        EntityPrefixPhase(dataset=dataset),
        EntityLookupPhase(lookups),
        SavePhase(
            default_output_path("harmonised", input_path),
            fieldnames=intermediate_fieldnames,
            enabled=save_harmonised,
        ),
        EntityPrunePhase(dataset_resource_log=dataset_resource_log),
        PivotPhase(),
        FactCombinePhase(issue_log=issue_log, fields=combine_fields),
        FactorPhase(),
        FactReferencePhase(dataset=dataset, specification=specification),
        FactLookupPhase(lookups),
        FactPrunePhase(),
        SavePhase(
            output_path,
            fieldnames=specification.factor_fieldnames(),
        ),
    )

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


def assign_entries(resource_path, dataset, organisation, pipeline_dir, specification, cache_dir):
    pipeline = Pipeline(pipeline_dir, dataset)
    resource_lookups = get_resource_unidentified_lookups(
        resource_path,
        dataset,
        organisations=[organisation],
        pipeline=pipeline,
        specification=specification,
        org_csv_path=f"{cache_dir}/organisation.csv"
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
    max_entity_num = lookups.get_max_entity(pipeline.name)
    lookups.entity_num_gen.state["current"] = max_entity_num
    lookups.entity_num_gen.state["range_max"] = specification.get_dataset_entity_max(
        dataset
    )
    lookups.entity_num_gen.state["range_min"] = specification.get_dataset_entity_min(
        dataset
    )

    lookups.save_csv()
