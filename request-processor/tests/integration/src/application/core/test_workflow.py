import pytest
import shutil
import os
import csv
from src.application.core.workflow import run_workflow


valid_geometry = "MULTIPOLYGON (((-1.799316 53.717797, -1.790771 53.717797, -1.790771 53.721066, -1.799316 53.721066, -1.799316 53.717797)))"
invalid_geometry = "MULTIPOLYGON (((-1.790771 53.717797, -1.790771 53.721066, -1.799316 53.721066, -1.799316 53.717797)))"

@pytest.fixture(scope="module")
def test_data_dir(test_dir):
    return os.path.realpath(f"{test_dir}/../../../../data")


@pytest.fixture
def uploaded_csv(mock_directories):
    collection_dir = mock_directories.COLLECTION_DIR
    resource_dir = os.path.join(collection_dir, "resource", "xyz123")
    os.makedirs(resource_dir, exist_ok=True)
    mock_csv = os.path.join(resource_dir, "7e9a0e71f3ddfe")
    
    rows = [{
        "geometry": valid_geometry,
        "ref": "4",
        "name": "South Jesmond",
    },
    {
        "geometry": valid_geometry,
        "ref": "4",
        "name": "South Jesmond duplicate ref",
    },
    {
        "geometry": invalid_geometry,
        "ref": "invalid wkt",
        "name": "invalid wkt",
    },
    {
        "geometry": valid_geometry,
        "ref": "invalid date",
        "name": "invalid date",
        "start_date": "invalid date here"
    },
    {
        "ref": "column_field_test",
        "geometry": valid_geometry,
        "name": "column_field_test"
    }]
    fieldnames = ["geometry", "ref", "name", "organisation", "start_date"]
    with open(mock_csv, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(rows)

    return mock_csv


def test_run_workflow(
    mocker, mock_directories, mock_fetch_pipeline_csvs, mock_extract_dataset_field_rows, test_data_dir, uploaded_csv
):
    collection = "tree-preservation-order"
    dataset = "tree"
    organisation = "local-authority:CTY"
    geom_type = ""
    column_mapping = {"ref": "reference"}
    fileName = uploaded_csv
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    request_id = "xyz123"
    shutil.copy(source_organisation_csv, destination_organisation_csv)

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs(dataset, request_id),
    )


    mock_extract_dataset_field_rows(dataset)

    response_data = run_workflow(
        fileName,
        request_id,
        collection,
        dataset,
        organisation,
        geom_type,
        column_mapping,
        mock_directories,
    )

    assert "converted-csv" in response_data
    assert "issue-log" in response_data
    assert "column-field-log" in response_data
    assert "error-summary" in response_data

    # Check converted csv is in the form we expect
    assert all("ref" in x for x in response_data["converted-csv"])
    assert response_data["converted-csv"][0]["ref"] == "4"
    assert all("geometry" in x for x in response_data["converted-csv"])
    assert response_data["converted-csv"][0]["geometry"] == valid_geometry
    assert all("name" in x for x in response_data["converted-csv"])
    assert response_data["converted-csv"][0]["name"] == "South Jesmond"
    
    # Check issue log
    assert any(x["issue-type"] == "invalid WKT" for x in response_data["issue-log"])
    assert any(x["issue-type"] == "invalid date" for x in response_data["issue-log"])
    assert any(x["issue-type"] == "reference values are not unique" for x in response_data["issue-log"])

    # Check column field log contains additional column mappings
    assert any(x["column"] == "ref" and x["field"] == "reference" for x in response_data["column-field-log"])

    # Check invalid WKT error has been generated and passed through in error summary
    assert any("1 geometry" in error for error in response_data["error-summary"])
    assert any("1 start date" in error for error in response_data["error-summary"])

def test_run_workflow_geom_type_polygon(
    mocker, mock_directories, mock_fetch_pipeline_csvs, mock_extract_dataset_field_rows, test_data_dir, uploaded_csv
):
    collection = "tree-preservation-order"
    dataset = "tree"
    organisation = "local-authority:CTY"
    geom_type = "polygon"
    column_mapping = {}
    fileName = uploaded_csv
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    request_id = "xyz123"
    shutil.copy(source_organisation_csv, destination_organisation_csv)

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs(dataset, request_id),
    )


    mock_extract_dataset_field_rows(dataset)

    response_data = run_workflow(
        fileName,
        request_id,
        collection,
        dataset,
        organisation,
        geom_type,
        column_mapping,
        mock_directories,
    )
    
    assert "converted-csv" in response_data
    assert "issue-log" in response_data
    assert "column-field-log" in response_data
    assert "error-summary" in response_data

    # Check converted csv is in the form we expect
    assert all("ref" in x for x in response_data["converted-csv"])
    assert response_data["converted-csv"][0]["ref"] == "4"
    assert all("geometry" in x for x in response_data["converted-csv"])
    assert response_data["converted-csv"][0]["geometry"] == valid_geometry
    assert all("name" in x for x in response_data["converted-csv"])
    assert response_data["converted-csv"][0]["name"] == "South Jesmond"
    
    # Check issue log
    assert any(x["issue-type"] == "invalid WKT" for x in response_data["issue-log"])
    assert any(x["issue-type"] == "invalid date" for x in response_data["issue-log"])

    # Check invalid WKT error has been generated and passed through in error summary
    assert any("1 geometry" in error for error in response_data["error-summary"])
    assert any("1 start date" in error for error in response_data["error-summary"])

def test_run_workflow_no_dataset_field_entries(
    mocker, mock_directories, mock_fetch_pipeline_csvs, mock_extract_dataset_field_rows, test_data_dir, uploaded_csv, caplog
):
    collection = "article-4-direction"
    dataset = "article-4-direction-area"
    organisation = "local-authority:CTY"
    geom_type = ""
    field = "reference"
    column_mapping = {"ref": field}
    fileName = uploaded_csv
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    request_id = "xyz123"
    shutil.copy(source_organisation_csv, destination_organisation_csv)

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs(dataset, request_id),
    )

    mock_extract_dataset_field_rows("tree")

    response_data = run_workflow(
        fileName,
        request_id,
        collection,
        dataset,
        organisation,
        geom_type,
        column_mapping,
        mock_directories,
    )

    assert f"Field '{field}' does not exist in dataset-field.csv" in caplog.text

