import csv
import json
from types import SimpleNamespace

import pytest

from src.application.core import workflow


def test_dataset_checks_preserve_rows_and_merge_shared_issues(monkeypatch, tmp_path):
    rows = [
        {"Plan type": "local-plan", "reference": "local"},
        {"Plan type": "minerals-plan;waste-plan", "reference": "shared"},
        {"Plan type": "minerals-plan", "reference": "minerals"},
        {"Plan type": "", "reference": "missing"},
        {"Plan type": "unrecognised", "reference": "unknown"},
    ]
    facts = [
        {
            "entry-number": str(index),
            "line-number": str(index + 1),
            "field": "datasets",
            "value": row["Plan type"],
        }
        for index, row in enumerate(rows, 1)
    ]
    specification = SimpleNamespace(
        dataset={
            slug: {"collection": "local-plan"}
            for slug in ("local-plan", "minerals-plan", "waste-plan")
        }
    )
    directories = SimpleNamespace(
        CACHE_DIR=str(tmp_path), SPECIFICATION_DIR=str(tmp_path), S3_SPEC=""
    )
    checked = {}

    def check(
        resource,
        request_id,
        collection,
        dataset,
        organisation,
        geom_type,
        column_mapping,
        directories,
        _check_datasets,
    ):
        assert not _check_datasets
        assert collection == "local-plan"
        assert column_mapping == {"Plan type": "datasets"}
        with open(
            f"{directories.COLLECTION_DIR}/resource/{request_id}/{resource}"
        ) as f:
            subset = list(csv.DictReader(f))
        checked[dataset] = [row["reference"] for row in subset]
        issues = []
        transformed = []
        for entry, row in enumerate(subset, 1):
            transformed.append(
                {
                    "entry-number": str(entry),
                    "line-number": str(entry + 1),
                    "field": "datasets",
                    "value": row["Plan type"],
                }
            )
            if row["reference"] == "shared":
                issues.append(
                    {
                        "dataset": dataset,
                        "resource": resource,
                        "entry-number": str(entry),
                        "line-number": str(entry + 1),
                        "issue-type": "invalid date",
                        "field": "entry-date",
                        "severity": "error",
                        "responsibility": "external",
                        "value": "bad-date",
                    }
                )
        return {
            "converted-csv": subset,
            "transformed-csv": transformed,
            "issue-log": issues,
            "task-log": [],
            "column-mapping": [
                {"field": "datasets", "column": "Plan type", "mandatory": True},
                {
                    "field": "minerals-and-waste-planning-authorities",
                    "column": "minerals-and-waste-planning-authorities",
                    "mandatory": dataset != "local-plan",
                },
            ],
        }

    monkeypatch.setattr(workflow, "run_workflow", check)
    result = workflow._check_resource_datasets(
        rows,
        facts,
        "resource",
        "request",
        "local-plan",
        "local-plan",
        "org",
        "",
        {"Plan type": "datasets"},
        directories,
        specification,
    )
    assert checked == {
        "local-plan": ["local", "missing", "unknown"],
        "minerals-plan": ["shared", "minerals"],
        "waste-plan": ["shared"],
    }
    assert result["converted-csv"] == rows
    assert len(result["transformed-csv"]) == 5
    assert len(result["issue-log"]) == 1
    assert result["issue-log"][0]["entry-number"] == "2"
    assert result["issue-log"][0]["line-number"] == "3"
    assert len(result["task-log"]) == 1
    assert json.loads(result["task-log"][0]["details"])["count"] == 1
    assert result["column-mapping"][1]["mandatory"] is True


@pytest.mark.parametrize(
    "value", ["", "local-plan", "unknown", "minerals-plan,waste-plan"]
)
def test_single_dataset_keeps_existing_check(value):
    assert (
        workflow._check_resource_datasets(
            [{"dataset": value, "reference": "row"}],
            [{"entry-number": "1", "field": "datasets", "value": value}],
            "resource",
            "request",
            "local-plan",
            "local-plan",
            "org",
            "",
            {},
            None,
            SimpleNamespace(dataset={"local-plan": {}}),
        )
        is None
    )


def test_singular_dataset_does_not_trigger_multiple_checks():
    assert (
        workflow._check_resource_datasets(
            [{"dataset": "minerals-plan;waste-plan"}],
            [
                {
                    "entry-number": "1",
                    "field": "dataset",
                    "value": "minerals-plan;waste-plan",
                }
            ],
            "resource",
            "request",
            "local-plan",
            "local-plan",
            "org",
            "",
            {},
            None,
            SimpleNamespace(
                dataset={"local-plan": {}, "minerals-plan": {}, "waste-plan": {}}
            ),
        )
        is None
    )


@pytest.mark.parametrize("dataset", ["minerals-plan", "tree"])
def test_child_failure_fails_whole_check(monkeypatch, tmp_path, dataset):
    error = {"status": 500, "exception": "RuntimeError"}
    monkeypatch.setattr(workflow, "run_workflow", lambda *args, **kwargs: error)
    result = workflow._check_resource_datasets(
        [{"dataset": dataset}],
        [{"entry-number": "1", "field": "datasets", "value": dataset}],
        "resource",
        "request",
        "local-plan",
        "local-plan",
        "org",
        "",
        {},
        SimpleNamespace(
            CACHE_DIR=str(tmp_path), SPECIFICATION_DIR=str(tmp_path), S3_SPEC=""
        ),
        SimpleNamespace(dataset={dataset: {}}),
    )
    assert result == error


@pytest.mark.parametrize("missing_authority", [False, True])
def test_mixed_resource_uses_each_schema(monkeypatch, tmp_path, missing_authority):
    """Exercise the real mapping, harmonisation and task pipelines offline."""
    from pathlib import Path
    from application.core import pipeline

    project = Path(workflow.__file__).resolve().parents[3]
    paths = {
        name: str(tmp_path / name)
        for name in (
            "COLLECTION_DIR",
            "CONVERTED_DIR",
            "ISSUE_DIR",
            "COLUMN_FIELD_DIR",
            "TRANSFORMED_DIR",
            "DATASET_RESOURCE_DIR",
            "PIPELINE_DIR",
            "CACHE_DIR",
        )
    }
    for path in paths.values():
        Path(path).mkdir()
    paths.update(SPECIFICATION_DIR=str(project / "specification"), S3_SPEC="")
    directories = SimpleNamespace(**paths)
    (Path(paths["CACHE_DIR"]) / "organisation.csv").write_text(
        "organisation,name,entity\nlocal-authority:ADU,Adur,1\n"
    )
    monkeypatch.setattr(pipeline, "_assign_entries", lambda **kwargs: None)
    monkeypatch.setattr(pipeline.API, "get_valid_category_values", lambda *args: {})

    def configure(
        collection, dataset, path, geom_type, columns, resource, specification
    ):
        Path(path).mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(workflow, "fetch_pipeline_csvs", configure)
    resource_path = Path(paths["COLLECTION_DIR"]) / "resource" / "request"
    resource_path.mkdir(parents=True)
    rows = [
        {
            "reference": "local",
            "name": "Local",
            "datasets": "local-plan",
            "local-planning-authorities": "E60000229",
            "minerals-and-waste-planning-authorities": "",
            "document-count": "",
            "entry-date": "2026-01-01",
        },
        {
            "reference": "shared",
            "name": "Shared",
            "datasets": "minerals-plan;waste-plan",
            "local-planning-authorities": "",
            "minerals-and-waste-planning-authorities": "E60000229",
            "document-count": "1",
            "entry-date": "invalid",
        },
    ]
    rows.insert(1, {field: "" for field in rows[0]})
    if missing_authority:
        for row in rows:
            del row["minerals-and-waste-planning-authorities"]
    workflow._write_check_csv(resource_path / "resource", rows, list(rows[0]))
    result = workflow.run_workflow(
        "resource",
        "request",
        "local-plan",
        "plan",
        "local-authority:ADU",
        "",
        {},
        directories,
    )
    assert "status" not in result, result
    assert result["converted-csv"] == rows
    assert [
        fact["value"]
        for fact in result["transformed-csv"]
        if fact["field"] == "datasets"
    ] == ["local-plan", "minerals-plan;waste-plan"]
    assert any(
        fact["field"] == "minerals-and-waste-planning-authorities"
        and fact["value"] == "E60000229"
        and fact["entry-number"] == "3"
        for fact in result["transformed-csv"]
    ) == (not missing_authority)
    missing = [
        json.loads(task["details"])["field"]
        for task in result["task-log"]
        if task["task-source"] == "column-field"
    ]
    assert missing.count("minerals-and-waste-planning-authorities") == int(
        missing_authority
    )
    assert "document-count" not in missing
    invalid_dates = [
        issue for issue in result["issue-log"] if issue["issue-type"] == "invalid date"
    ]
    assert len(invalid_dates) == 1
    assert invalid_dates[0]["entry-number"] == "3"
    assert invalid_dates[0]["line-number"] == "4"
    date_tasks = [
        json.loads(task["details"])
        for task in result["task-log"]
        if json.loads(task["details"]).get("issue_type") == "invalid date"
    ]
    assert len(date_tasks) == 1
    assert date_tasks[0]["count"] == 1
