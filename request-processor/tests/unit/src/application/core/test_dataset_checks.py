import csv
import json
from types import SimpleNamespace

import pytest

from src.application.core import workflow


@pytest.mark.parametrize("dataset_field", ["dataset", "datasets"])
def test_dataset_checks_preserve_rows_and_merge_shared_issues(
    monkeypatch, tmp_path, dataset_field
):
    rows = [
        {"Plan type": "local-plan", "reference": "local"},
        {"Plan type": "minerals-plan;waste-plan", "reference": "shared"},
        {"Plan type": "minerals-plan;tree", "reference": "minerals"},
        {"Plan type": "", "reference": "missing"},
        {"Plan type": "tree", "reference": "unknown"},
    ]
    facts = [
        {
            "entry-number": str(index),
            "line-number": str(index + 1),
            "field": dataset_field,
            "value": row["Plan type"],
        }
        for index, row in enumerate(rows, 1)
    ]
    specification = SimpleNamespace(
        dataset={
            slug: {"collection": "local-plan"}
            for slug in ("local-plan", "minerals-plan", "waste-plan", "tree")
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
        assert column_mapping == {"Plan type": dataset_field}
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
                    "field": dataset_field,
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
                {"field": dataset_field, "column": "Plan type", "mandatory": True},
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
        {"Plan type": dataset_field},
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


def test_unrelated_field_does_not_trigger_multiple_checks():
    assert (
        workflow._check_resource_datasets(
            [{"dataset": "minerals-plan;waste-plan"}],
            [
                {
                    "entry-number": "1",
                    "field": "prefix",
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


@pytest.mark.parametrize(
    "selected,dataset",
    [("local-plan", "minerals-plan"), ("tree-preservation-zone", "tree")],
)
def test_child_failure_fails_whole_check(monkeypatch, tmp_path, selected, dataset):
    error = {"status": 500, "exception": "RuntimeError"}
    monkeypatch.setattr(workflow, "run_workflow", lambda *args, **kwargs: error)
    result = workflow._check_resource_datasets(
        [{"dataset": dataset}],
        [{"entry-number": "1", "field": "datasets", "value": dataset}],
        "resource",
        "request",
        "local-plan",
        selected,
        "org",
        "",
        {},
        SimpleNamespace(
            CACHE_DIR=str(tmp_path), SPECIFICATION_DIR=str(tmp_path), S3_SPEC=""
        ),
        SimpleNamespace(dataset={dataset: {}}),
    )
    assert result == error


@pytest.fixture
def run_plan_check(monkeypatch, tmp_path):
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

    def run(rows, dataset="plan"):
        workflow._write_check_csv(resource_path / "resource", rows, list(rows[0]))
        result = workflow.run_workflow(
            "resource",
            "request",
            "local-plan",
            dataset,
            "local-authority:ADU",
            "",
            {},
            directories,
        )
        assert "status" not in result, result
        return result

    return run


@pytest.mark.parametrize("missing_authority", [False, True])
@pytest.mark.parametrize("separator", [";", ":", ","])
def test_mixed_resource_uses_each_schema(run_plan_check, missing_authority, separator):
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
            "datasets": separator.join(["minerals-plan", "waste-plan"]),
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
    result = run_plan_check(rows)
    assert result["converted-csv"] == rows
    assert [
        fact["value"]
        for fact in result["transformed-csv"]
        if fact["field"] == "datasets"
    ] == ["local-plan", separator.join(["minerals-plan", "waste-plan"])]
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


@pytest.mark.parametrize("field", ["dataset", "datasets"])
@pytest.mark.parametrize(
    "value",
    [
        "supplementary-plan-cda-design-code",
        "tree",
        "minerals-plan-waste-plan",
        ";",
        ":",
        ",",
        " ; :, ",
    ],
)
def test_invalid_plan_membership_creates_critical_task(run_plan_check, field, value):
    selected = "plan" if field == "datasets" else "supplementary-plan"
    rows = [{field: "", "reference": ""}, {field: value, "reference": "bad"}]
    result = run_plan_check(rows, selected)
    issues = [
        i for i in result["issue-log"] if i["issue-type"] == "invalid category value"
    ]
    assert len(issues) == 1
    assert issues[0]["severity"] == "critical"
    assert issues[0]["value"] == value.strip()
    assert issues[0]["line-number"] == "3"
    tasks = [
        t
        for t in result["task-log"]
        if json.loads(t["details"]).get("issue_type") == "invalid category value"
    ]
    assert len(tasks) == 1
    assert tasks[0]["severity"] == "critical"
    assert json.loads(tasks[0]["details"])["field"] == field


@pytest.mark.parametrize("field", ["dataset", "datasets"])
@pytest.mark.parametrize("separator", [";", ":", ","])
def test_supported_plan_combinations(run_plan_check, field, separator):
    result = run_plan_check(
        [
            {
                field: separator.join(["minerals-plan", "waste-plan"]),
                "reference": "shared",
            }
        ],
        "plan" if field == "datasets" else "minerals-plan",
    )
    assert not [
        i for i in result["issue-log"] if i["issue-type"] == "invalid category value"
    ]


def test_mixed_valid_and_invalid_memberships(run_plan_check):
    result = run_plan_check([{"datasets": "local-plan;invalid", "reference": "mixed"}])
    issues = [
        i for i in result["issue-log"] if i["issue-type"] == "invalid category value"
    ]
    assert len(issues) == 1
    assert issues[0]["value"] == "local-plan;invalid"
    assert issues[0]["severity"] == "critical"


def test_non_plan_check_does_not_validate_plan_memberships(run_plan_check, monkeypatch):
    from unittest.mock import Mock
    from application.core import pipeline

    validate = Mock(side_effect=AssertionError("Plan validation must not run"))
    monkeypatch.setattr(pipeline, "validate_plan_datasets", validate)
    run_plan_check([{"reference": "tree", "dataset": "tree"}], "tree")
    validate.assert_not_called()


@pytest.mark.parametrize("value", [None, ""])
def test_absent_plan_membership_is_not_an_invalid_category(value):
    from digital_land.log import IssueLog
    from application.core.plan_datasets import validate_plan_datasets

    issues = IssueLog()
    validate_plan_datasets(
        [{"field": "dataset", "value": value, "entry-number": "1"}],
        [{"dataset": value, "reference": "row"}],
        issues,
    )
    assert issues.rows == []


def test_invalid_plan_is_enriched_when_pipeline_has_no_other_issues(
    run_plan_check, monkeypatch
):
    from application.core import pipeline

    transform = pipeline.Pipeline.transform

    def transform_without_other_issues(*args, **kwargs):
        issue_log = transform(*args, **kwargs)
        issue_log.rows = []
        # A fresh, empty issue log has not acquired severity columns.
        issue_log.fieldnames = [
            "dataset",
            "resource",
            "line-number",
            "entry-number",
            "field",
            "entity",
            "issue-type",
            "value",
            "message",
        ]
        return issue_log

    monkeypatch.setattr(pipeline.Pipeline, "transform", transform_without_other_issues)
    result = run_plan_check([{"datasets": "invalid", "reference": "bad"}])
    assert len(result["issue-log"]) == 1
    issue = result["issue-log"][0]
    assert issue["issue-type"] == "invalid category value"
    assert issue["severity"] == "critical"
    assert issue["responsibility"] == "external"
    assert issue["description"]
    assert issue["line-number"] == "2"
    tasks = [task for task in result["task-log"] if task["task-source"] == "issue"]
    assert len(tasks) == 1
    assert tasks[0]["severity"] == "critical"
