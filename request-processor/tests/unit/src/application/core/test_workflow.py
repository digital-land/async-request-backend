import pytest
from src.application.core.workflow import (
    updateColumnFieldLog,
    error_summary,
    csv_to_json,
    fetch_pipeline_csvs,
    add_data_workflow,
    fetch_add_data_csvs,
)
import csv
import os
from pathlib import Path
import urllib
from urllib.error import HTTPError


@pytest.mark.parametrize(
    "column_field_log, expected_length, expected_missing_fields",
    [
        (
            [
                {
                    "dataset": "conservation-area",
                    "column": "documentation-url",
                    "field": "documentation-url",
                },
                {"dataset": "conservation-area", "column": "name", "field": "name"},
            ],
            4,
            ["reference", "geometry"],
        ),
        (
            [
                {
                    "dataset": "conservation-area",
                    "column": "documentation-url",
                    "field": "documentation-url",
                },
                {
                    "dataset": "conservation-area",
                    "column": "geometry",
                    "field": "geometry",
                },
                {
                    "dataset": "conservation-area",
                    "column": "reference",
                    "field": "reference",
                },
            ],
            3,
            [],
        ),
    ],
)
def test_updateColumnFieldLog(
    column_field_log, expected_length, expected_missing_fields
):
    required_fields = ["reference", "geometry"]
    updateColumnFieldLog(column_field_log, required_fields)
    assert len(column_field_log) == expected_length
    for field in expected_missing_fields:
        assert any(
            entry["field"] == field and entry["missing"] for entry in column_field_log
        )


def test_error_summary():
    internal_issue = "invalid organisation"
    issue_log = [
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "2",
            "entry-number": "1",
            "field": "geometry",
            "issue-type": "OSGB out of bounds of England",
            "value": "",
            "severity": "error",
            "description": "Geometry must be in England",
            "responsibility": "external",
        },
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "3",
            "entry-number": "2",
            "field": "geometry",
            "issue-type": "OSGB out of bounds of England",
            "value": "",
            "severity": "error",
            "description": "Geometry must be in England",
            "responsibility": "external",
        },
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "3",
            "entry-number": "2",
            "field": "start-date",
            "issue-type": "invalid date",
            "value": "40/04/2024",
            "severity": "error",
            "description": "Start date must be a real date",
            "responsibility": "external",
        },
        {
            "dataset": "conservation-area",
            "resource": "d5b003b74563bb5bcf06742ee27f9dd573a47a123f8f5d975d9e04187fa58eff",
            "line-number": "3",
            "entry-number": "2",
            "field": "start-date",
            "issue-type": internal_issue,
            "value": "40/04/2024",
            "severity": "error",
            "description": "Start date must be a real date",
            "responsibility": "internal",
        },
    ]
    column_field_log = [{"field": "reference", "missing": True}]
    not_mapped_columns = ["test"]
    json_data = error_summary(issue_log, column_field_log, not_mapped_columns)
    expected_messages = [
        "2 geometries must be in England",
        "1 start date must be a real date",
        "Reference column missing",
        "test not found in specification",
    ]

    assert any(message in json_data for message in expected_messages)
    assert any(internal_issue not in message for message in json_data)


def test_csv_to_json_with_valid_file(test_dir):
    # Prepare a CSV file
    row1 = {
        "dataset": "conservation-area",
        "column": "documentation-url",
        "field": "documentation-url",
    }
    row2 = {
        "dataset": "article-4-direction",
        "column": "name",
        "field": "name",
    }
    mock_csv = os.path.join(test_dir, "test.csv")
    fieldnames = row1.keys()
    with open(mock_csv, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row1)
        dictwriter.writerow(row2)

    # Patch the detect_encoding function
    # mocker.patch("application.core.util.detect_encoding", mock_detect_encoding)

    # Test the function
    json_data = csv_to_json(mock_csv)

    # Assertions
    assert len(json_data) == 2
    assert json_data[0]["dataset"] == "conservation-area"
    assert json_data[1]["field"] == "name"


@pytest.mark.parametrize(
    "dataset, geom_type, column_mapping, expected_row, expected_rows_before, expected_rows_after",
    [  # Parameters for test_fetch_pipelines
        (
            "tree",
            "",
            {},
            {
                "dataset": "tree",
                "": "",
                "resource": "",
                "column": "id",
                "field": "reference",
            },
            None,
            None,
        ),
        (  # Parameters for test_fetch_pipelines_for_tree
            "tree",
            "polygon",
            {},
            None,
            [
                {
                    "dataset": "tree",
                    "": "",
                    "resource": "",
                    "column": "id",
                    "field": "reference",
                }
            ],
            [
                {
                    "dataset": "tree",
                    "": "",
                    "resource": "",
                    "column": "id",
                    "field": "reference",
                },
                {
                    "dataset": "tree",
                    "": "",
                    "resource": "",
                    "column": "WKT",
                    "field": "geometry",
                },
            ],
        ),
        (  # Parameters for test_fetch_pipelines_with_column_mapping
            "conservation-area",
            "",
            {"add-date": "entry-date", "WKT": "geometry"},
            None,
            [
                {
                    "dataset": "conservation-area",
                    "": "",
                    "resource": "",
                    "column": "id",
                    "field": "reference",
                }
            ],
            [
                {
                    "dataset": "conservation-area",
                    "": "",
                    "resource": "",
                    "column": "WKT",
                    "field": "geometry",
                },
                {
                    "dataset": "conservation-area",
                    "": "",
                    "resource": "",
                    "column": "add-date",
                    "field": "entry-date",
                },
            ],
        ),
    ],
)
def test_fetch_pipelines(
    mocker,
    mock_directories,
    mock_fetch_pipeline_csvs,
    mock_extract_dataset_field_rows,
    dataset,
    geom_type,
    column_mapping,
    expected_row,
    expected_rows_before,
    expected_rows_after,
):
    request_id = "xyz123"
    collection = "test_collection"
    pipeline_dir = os.path.join(mock_directories.PIPELINE_DIR, dataset, request_id)
    resource = ""

    # Mock fetch_pipeline_csvs
    mock_fetch_pipeline_csvs(dataset, request_id)

    if column_mapping:
        # Mock extract_dataset_field_rows if column mapping is provided (original test 3)
        mock_extract_dataset_field_rows(dataset)

    # Mock urllib.request.urlretrieve (common to all tests)
    mocked_urlretrieve = mocker.patch("urllib.request.urlretrieve")

    # Call the function (common to all tests)
    fetch_pipeline_csvs(
        collection,
        dataset,
        pipeline_dir,
        geom_type,
        column_mapping if column_mapping else {},
        resource,
        mock_directories.SPECIFICATION_DIR,
    )

    # Check that urlretrieve was called with the expected URL and file path
    source_url = "https://raw.githubusercontent.com/digital-land//"
    expected_url = f"{source_url}{collection + '-collection'}/main/pipeline/column.csv"
    expected_file_path = os.path.join(pipeline_dir, "column.csv")
    mocked_urlretrieve.assert_any_call(expected_url, expected_file_path)
    assert (
        Path(pipeline_dir) / "transform.csv"
    ).exists(), "transform.csv not downloaded"

    if expected_row:  # test_fetch_pipelines
        with open(os.path.join(pipeline_dir, "column.csv"), newline="") as csv_file:
            reader = csv.DictReader(csv_file)
            csv_rows = list(reader)
            assert expected_row in csv_rows

    csv_file_path = os.path.join(pipeline_dir, "column.csv")

    for expected_rows in [expected_rows_before, expected_rows_after]:
        if expected_rows:
            if os.path.exists(csv_file_path):
                with open(csv_file_path, newline="") as csv_file:
                    reader = csv.DictReader(csv_file)
                    csv_rows = list(reader)

                for row in expected_rows:
                    assert row in csv_rows


def test_add_data_workflow(monkeypatch):
    file_name = "test.csv"
    request_id = "req-001"
    collection = "test-collection"
    dataset = "test-dataset"
    organisation = "test-org"
    url = "http://example.com/url"
    documentation_url = "http://example.com/doc"

    class DummyDirectories:
        PIPELINE_DIR = "/tmp/pipeline"
        COLLECTION_DIR = "/tmp/collection"
        TRANSFORMED_DIR = "/tmp/transformed"
        SPECIFICATION_DIR = "/tmp/specification"
        CACHE_DIR = "/tmp/cache"

    directories = DummyDirectories()

    expected_response = {"status": "success", "data": "test"}

    monkeypatch.setattr(
        "src.application.core.workflow.resource_from_path", lambda path: "resource-hash"
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_csvs",
        lambda col, pdir: ["/tmp/pipeline/lookup.csv"],
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_response",
        lambda *args, **kwargs: expected_response,
    )

    result = add_data_workflow(
        file_name,
        request_id,
        collection,
        dataset,
        organisation,
        url,
        documentation_url,
        directories,
    )

    assert result == expected_response


def test_add_data_workflow_calls(monkeypatch):
    file_name = "test.csv"
    request_id = "req-002"
    collection = "test-collection"
    dataset = "test-dataset"
    organisation = "test-org"
    url = "http://example.com/url"
    documentation_url = "http://example.com/doc"

    class DummyDirectories:
        PIPELINE_DIR = "/tmp/pipeline"
        COLLECTION_DIR = "/tmp/collection"
        TRANSFORMED_DIR = "/tmp/transformed"
        SPECIFICATION_DIR = "/tmp/specification"
        CACHE_DIR = "/tmp/cache"

    directories = DummyDirectories()

    called = {}

    def fake_fetch_add_data_csvs(col, pdir):
        called["fetch_add_data_csvs"] = (col, pdir)
        return ["/tmp/pipeline/lookup.csv"]

    def fake_fetch_add_data_response(
        collection,
        dataset,
        organisation_provider,
        pipeline_dir,
        input_dir,
        output_path,
        specification_dir,
        cache_dir,
        url,
        documentation_url,
    ):
        called["fetch_add_data_response"] = {
            "collection": collection,
            "dataset": dataset,
            "organisation": organisation_provider,
            "pipeline_dir": pipeline_dir,
            "input_dir": input_dir,
            "output_path": output_path,
            "specification_dir": specification_dir,
            "cache_dir": cache_dir,
            "url": url,
            "documentation_url": documentation_url,
        }
        return {"result": "ok"}

    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_csvs", fake_fetch_add_data_csvs
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_response",
        fake_fetch_add_data_response,
    )

    add_data_workflow(
        file_name,
        request_id,
        collection,
        dataset,
        organisation,
        url,
        documentation_url,
        directories,
    )

    expected_pipeline_dir = os.path.join(
        directories.PIPELINE_DIR, collection, request_id
    )
    expected_input_dir = os.path.join(
        directories.COLLECTION_DIR, "resource", request_id
    )
    expected_output_path = os.path.join(
        directories.TRANSFORMED_DIR, request_id, file_name
    )

    assert called["fetch_add_data_csvs"] == (collection, expected_pipeline_dir)
    assert called["fetch_add_data_response"]["dataset"] == dataset
    assert called["fetch_add_data_response"]["organisation"] == organisation
    assert called["fetch_add_data_response"]["pipeline_dir"] == expected_pipeline_dir
    assert called["fetch_add_data_response"]["input_dir"] == expected_input_dir
    assert called["fetch_add_data_response"]["output_path"] == expected_output_path
    assert (
        called["fetch_add_data_response"]["specification_dir"]
        == directories.SPECIFICATION_DIR
    )
    assert called["fetch_add_data_response"]["cache_dir"] == directories.CACHE_DIR
    assert called["fetch_add_data_response"]["url"] == url
    assert called["fetch_add_data_response"]["documentation_url"] == documentation_url


def test_fetch_add_data_csvs_from_url(monkeypatch, tmp_path):
    collection = "test-collection"
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir_str = str(pipeline_dir)
    monkeypatch.setattr(
        "src.application.core.workflow.CONFIG_URL", "http://example.com/config/"
    )

    # Patch urllib.request.urlretrieve to simulate download
    downloads = []

    def fake_urlretrieve(url, path):
        downloads.append((url, path))
        with open(path, "w") as f:
            f.write("dummy data")

    monkeypatch.setattr("urllib.request.urlretrieve", fake_urlretrieve)

    files = fetch_add_data_csvs(collection, pipeline_dir_str)

    assert os.path.exists(pipeline_dir_str)
    assert any("lookup.csv" in path for url, path in downloads)
    assert files == []


def test_fetch_add_data_csvs_handles_http_error(monkeypatch, tmp_path):
    collection = "test-collection"
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir_str = str(pipeline_dir)
    monkeypatch.setattr(
        "src.application.core.workflow.CONFIG_URL", "http://example.com/config/"
    )

    def raise_http_error(url, path):
        raise HTTPError(url, 404, "Not Found", None, None)

    monkeypatch.setattr("urllib.request.urlretrieve", raise_http_error)

    files = fetch_add_data_csvs(collection, pipeline_dir_str)

    assert os.path.exists(pipeline_dir_str)
    assert files == []
