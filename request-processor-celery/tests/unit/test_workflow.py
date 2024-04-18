from src.application.core.workflow import (
    updateColumnFieldLog,
    error_summary,
    csv_to_json,
    fetch_pipeline_csvs,
)
import csv
import os


def test_updateColumnFieldLog():
    column_field_log = [
        {
            "dataset": "conservation-area",
            "column": "documentation-url",
            "field": "documentation-url",
        },
        {
            "dataset": "conservation-area",
            "column": "name",
            "field": "name",
        },
    ]

    required_fields = ["reference", "geometry"]

    assert len(column_field_log) == 2
    updateColumnFieldLog(column_field_log, required_fields)

    assert len(column_field_log) == 4  # Two new entries added
    assert any(
        entry["field"] == "reference" and entry["missing"] for entry in column_field_log
    )
    assert any(
        entry["field"] == "geometry" and entry["missing"] for entry in column_field_log
    )
    assert any(
        entry["field"] == "documentation-url" and not entry["missing"]
        for entry in column_field_log
    )
    assert any(
        entry["field"] == "name" and not entry["missing"] for entry in column_field_log
    )


def test_updateColumnFieldLog_no_missing_fields():
    column_field_log = [
        {
            "dataset": "conservation-area",
            "column": "documentation-url",
            "field": "documentation-url",
        },
        {"dataset": "conservation-area", "column": "geometry", "field": "geometry"},
        {"dataset": "conservation-area", "column": "reference", "field": "reference"},
    ]
    required_fields = ["reference", "geometry"]
    updateColumnFieldLog(column_field_log, required_fields)
    assert len(column_field_log) == 3
    assert any(
        entry["field"] == "geometry" and not entry["missing"]
        for entry in column_field_log
    )
    assert any(
        entry["field"] == "reference" and not entry["missing"]
        for entry in column_field_log
    )


def test_error_summary():
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
        },
    ]
    column_field_log = [{"field": "reference", "missing": True}]
    json_data = error_summary(issue_log, column_field_log)
    expected_messages = [
        "2 geometries must be in England",
        "1 start date must be a real date",
        "Reference column missing",
    ]

    assert any(message in json_data for message in expected_messages)


def test_csv_to_json_with_valid_file(mocker, test_dir):
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


def test_fetch_pipelines(mocker, mock_directories, mock_fetch_pipeline_csvs):
    collection = "test_collection"
    dataset = "tree"
    pipeline_dir = mock_directories.PIPELINE_DIR
    geom_type = ""
    resource = ""

    # Mock urllib.request.urlretrieve
    mocked_urlretrieve = mocker.patch("urllib.request.urlretrieve")

    # Call the function
    fetch_pipeline_csvs(collection, dataset, pipeline_dir, geom_type, resource)

    source_url = "https://raw.githubusercontent.com/digital-land//"
    expected_url = f"{source_url}{collection + '-collection'}/main/pipeline/column.csv"
    expected_file_path = os.path.join(pipeline_dir, "column.csv")
    mocked_urlretrieve.assert_called_with(expected_url, expected_file_path)

    expected_row = {
        "dataset": "tree",
        "": "",
        "resource": "",
        "column": "id",
        "field": "reference",
    }
    with open(os.path.join(pipeline_dir, "column.csv"), newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        csv_rows = list(reader)

    print(csv_rows)
    assert expected_row in csv_rows


def test_fetch_pipelines_for_tree(mocker, mock_directories, mock_fetch_pipeline_csvs):
    collection = "test_collection"
    dataset = "tree"
    pipeline_dir = mock_directories.PIPELINE_DIR
    geom_type = "polygon"
    resource = ""

    # Mock urllib.request.urlretrieve
    mocker.patch("urllib.request.urlretrieve")

    expected_row_before_execution = {
        "dataset": "tree",
        "": "",
        "resource": "",
        "column": "id",
        "field": "reference",
    }
    expected_row_after_execution = {
        "dataset": "tree",
        "": "",
        "resource": "",
        "column": "WKT",
        "field": "geometry",
    }

    with open(os.path.join(pipeline_dir, "column.csv"), newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        csv_rows = list(reader)

    assert expected_row_before_execution in csv_rows
    # Call the function
    fetch_pipeline_csvs(collection, dataset, pipeline_dir, geom_type, resource)

    with open(os.path.join(pipeline_dir, "column.csv"), newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        csv_rows_after = list(reader)
    assert expected_row_before_execution in csv_rows_after
    assert expected_row_after_execution in csv_rows_after
