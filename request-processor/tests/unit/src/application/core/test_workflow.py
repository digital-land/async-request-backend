import pytest
from src.application.core.workflow import (
    run_workflow,
    csv_to_json,
    fetch_pipeline_csvs,
    add_data_workflow,
    fetch_add_data_pipeline_csvs,
    add_extra_column_mappings,
    _get_column_mapping,
)
import csv
import hashlib
import os
from pathlib import Path
from urllib.error import HTTPError


def test_get_column_mapping(test_dir, tmp_path):
    dataset = "conservation-area"
    required_fields = ["reference", "geometry"]

    spec_dir = tmp_path / "specification"
    spec_dir.mkdir()
    dataset_field_csv = spec_dir / "dataset-field.csv"
    with open(dataset_field_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "field"])
        writer.writeheader()
        writer.writerow({"dataset": dataset, "field": "reference"})
        writer.writerow({"dataset": dataset, "field": "geometry"})
        writer.writerow({"dataset": dataset, "field": "name"})
        writer.writerow({"dataset": "other-dataset", "field": "other-field"})

    col_field_csv = tmp_path / "column-field.csv"
    with open(col_field_csv, "w", newline="") as f:
        writer = csv.DictWriter(
            f, fieldnames=["dataset", "resource", "column", "field"]
        )
        writer.writeheader()
        writer.writerow(
            {
                "dataset": dataset,
                "resource": "abc",
                "column": "RefCode",
                "field": "reference",
            }
        )
        writer.writerow(
            {
                "dataset": dataset,
                "resource": "abc",
                "column": "ExtraCol",
                "field": "extra-field",
            }
        )

    result = _get_column_mapping(
        str(col_field_csv), dataset, required_fields, str(spec_dir)
    )

    by_field = {entry["field"]: entry for entry in result}

    assert by_field["reference"]["column"] == "RefCode"
    assert by_field["reference"]["mandatory"] is True

    assert by_field["geometry"]["mandatory"] is True
    assert "column" not in by_field["geometry"]

    assert by_field["name"]["mandatory"] is False
    assert "column" not in by_field["name"]

    assert by_field["extra-field"]["column"] == "ExtraCol"
    assert by_field["extra-field"]["mandatory"] is False

    assert "other-field" not in by_field


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


def test_run_workflow_returns_error_response_when_pipeline_fails(
    monkeypatch, mock_directories
):
    def raise_pipeline_error(*args, **kwargs):
        raise RuntimeError("assign entries failed")

    monkeypatch.setattr(
        "src.application.core.workflow.fetch_pipeline_csvs", lambda *args: {}
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_response_data", raise_pipeline_error
    )
    monkeypatch.setattr("src.application.core.workflow.clean_up", lambda *args: None)

    result = run_workflow(
        "data.csv",
        "request-123",
        "test-collection",
        "test-dataset",
        "test-org",
        "",
        {},
        mock_directories,
    )

    assert result == {
        "message": "An error occurred during workflow processing.",
        "status": 500,
        "exception": "RuntimeError",
    }


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
        CONVERTED_DIR = "/tmp/converted"
        TRANSFORMED_DIR = "/tmp/transformed"
        SPECIFICATION_DIR = "/tmp/specification"
        CACHE_DIR = "/tmp/cache"

    directories = DummyDirectories()

    pipeline_response = {"status": "success", "data": "test"}
    expected_response = {
        "pipeline-summary": pipeline_response,
        "pipeline-issues": [],
        "endpoint-summary": {"endpoint_summary": "mocked"},
        "source-summary": {"source_summary": "mocked"},
        "converted-csv": [],
        "transformed-csv": [],
    }

    monkeypatch.setattr(
        "src.application.core.workflow.resource_from_path", lambda path: "resource-hash"
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_pipeline_csvs",
        lambda col, pdir, **kwargs: True,
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_collection_csvs",
        lambda col, cdir, **kwargs: True,
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_response",
        lambda *args, **kwargs: pipeline_response,
    )
    monkeypatch.setattr(
        "src.application.core.workflow.validate_endpoint",
        lambda *args, **kwargs: {"endpoint_summary": "mocked"},
    )
    monkeypatch.setattr(
        "src.application.core.workflow.validate_source",
        lambda *args, **kwargs: {"source_summary": "mocked"},
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
        CONVERTED_DIR = "/tmp/converted"
        TRANSFORMED_DIR = "/tmp/transformed"
        SPECIFICATION_DIR = "/tmp/specification"
        CACHE_DIR = "/tmp/cache"

    directories = DummyDirectories()

    called = {}

    def fake_fetch_add_data_pipeline_csvs(col, pdir, **_):
        called["fetch_add_data_pipeline_csvs"] = (col, pdir)
        return True

    def fake_fetch_add_data_collection_csvs(col, cdir, **_):
        called["fetch_add_data_collection_csvs"] = (col, cdir)
        return True

    def fake_fetch_add_data_response(
        dataset,
        organisation_provider,
        pipeline_dir,
        input_dir,
        output_path,
        specification_dir,
        cache_dir,
        endpoint,
        converted_path=None,
    ):
        called["fetch_add_data_response"] = {
            "dataset": dataset,
            "organisation": organisation_provider,
            "pipeline_dir": pipeline_dir,
            "input_dir": input_dir,
            "output_path": output_path,
            "specification_dir": specification_dir,
            "cache_dir": cache_dir,
            "endpoint": endpoint,
        }
        return {"result": "ok"}

    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_pipeline_csvs",
        fake_fetch_add_data_pipeline_csvs,
    )
    monkeypatch.setattr(
        "src.application.core.workflow.fetch_add_data_collection_csvs",
        fake_fetch_add_data_collection_csvs,
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

    expected_config_dir = os.path.join(directories.COLLECTION_DIR, request_id)
    assert called["fetch_add_data_pipeline_csvs"] == (collection, expected_pipeline_dir)
    assert called["fetch_add_data_collection_csvs"] == (collection, expected_config_dir)
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
    expected_endpoint_hash = hashlib.sha256(url.encode("utf-8")).hexdigest()
    assert called["fetch_add_data_response"]["endpoint"] == expected_endpoint_hash


def test_fetch_add_data_pipeline_csvs_from_url(monkeypatch, tmp_path):
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

    fetch_add_data_pipeline_csvs(collection, pipeline_dir_str)

    assert os.path.exists(pipeline_dir_str)
    assert any("lookup.csv" in path for url, path in downloads)


def test_fetch_add_data_pipeline_csvs_handles_http_error(monkeypatch, tmp_path):
    collection = "test-collection"
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir_str = str(pipeline_dir)
    monkeypatch.setattr(
        "src.application.core.workflow.CONFIG_URL", "http://example.com/config/"
    )

    def raise_http_error(url, path):
        raise HTTPError(url, 404, "Not Found", None, None)

    monkeypatch.setattr("urllib.request.urlretrieve", raise_http_error)

    fetch_add_data_pipeline_csvs(collection, pipeline_dir_str)

    assert os.path.exists(pipeline_dir_str)


COLUMN_CSV_FIELDNAMES = ["dataset", "resource", "column", "field"]


def _write_column_csv(path, rows=None):
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=COLUMN_CSV_FIELDNAMES)
        writer.writeheader()
        for row in rows or []:
            writer.writerow(row)


def test_add_extra_column_mappings_ignore_field_not_in_not_mapped(tmp_path):
    """IGNORE field should be written to column.csv but not flagged as not_mapped."""
    column_csv = tmp_path / "column.csv"
    spec_dir = tmp_path / "spec"
    spec_dir.mkdir()
    dataset_field_csv = spec_dir / "dataset-field.csv"
    with open(dataset_field_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "field"])
        writer.writeheader()
        writer.writerow({"dataset": "test-dataset", "field": "name"})
    _write_column_csv(column_csv)

    not_mapped = add_extra_column_mappings(
        str(column_csv),
        {"MyColumn": "IGNORE"},
        "test-dataset",
        "resource-hash",
        str(spec_dir),
    )

    assert "IGNORE" not in not_mapped
    with open(column_csv, newline="") as f:
        rows = list(csv.DictReader(f))
    assert any(r["field"] == "IGNORE" for r in rows)


def test_add_extra_column_mappings_mix_valid_ignore_invalid(tmp_path):
    """Only invalid fields flagged; IGNORE and valid fields written to CSV."""
    column_csv = tmp_path / "column.csv"
    spec_dir = tmp_path / "spec"
    spec_dir.mkdir()
    dataset_field_csv = spec_dir / "dataset-field.csv"
    with open(dataset_field_csv, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "field"])
        writer.writeheader()
        writer.writerow({"dataset": "test-dataset", "field": "name"})
    _write_column_csv(column_csv)

    not_mapped = add_extra_column_mappings(
        str(column_csv),
        {"ColA": "name", "ColB": "IGNORE", "ColC": "nonexistent-field"},
        "test-dataset",
        "resource-hash",
        str(spec_dir),
    )

    assert not_mapped == ["nonexistent-field"]


def test_add_extra_column_mappings_ignore_with_no_filtered_rows(tmp_path):
    """When filtered_rows is None (spec dir missing), IGNORE field causes no crash."""
    column_csv = tmp_path / "column.csv"
    _write_column_csv(column_csv)
    empty_spec_dir = tmp_path / "spec"
    empty_spec_dir.mkdir()

    not_mapped = add_extra_column_mappings(
        str(column_csv),
        {"MyColumn": "IGNORE"},
        "test-dataset",
        "resource-hash",
        str(empty_spec_dir),
    )

    assert not_mapped == []
