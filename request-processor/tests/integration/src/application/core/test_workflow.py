import pytest
import shutil
import os
import csv
from src.application.core.workflow import run_workflow


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
        "geometry": "MULTIPOLYGON (((-1.799316 53.717797, "
        "-1.790771 53.717797, -1.790771 53.721066, -1.799316 53.721066, -1.799316 53.717797)))",
        "reference": "4",
        "name": "South Jesmond",
        "organisation": "local-authority:CTY",
    },
    {
        "geometry": "MULTIPOLYGON (((-1.799316 53.717797, "
        "-1.790771 53.717797, -1.790771 53.721066, -1.799316 53.721066, -1.799316 53.717797)))",
        "reference": "4",
        "name": "South Jesmond duplicate",
        "organisation": "local-authority:CTY",
    }]
    fieldnames = rows[0].keys()
    with open(mock_csv, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerows(rows)

    return mock_csv


def test_run_workflow(
    mocker, mock_directories, mock_fetch_pipeline_csvs, test_data_dir, uploaded_csv
):
    collection = "tree-preservation-order"
    dataset = "tree"
    organisation = ""
    geom_type = ""
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

    print("response data", response_data)
    assert "converted-csv" in response_data
    assert "issue-log" in response_data
    assert "column-field-log" in response_data
    assert "error-summary" in response_data
    
    assert any(x["issue-type"] == "reference values are not unique" for x in response_data["issue-log"])
    # assert False
