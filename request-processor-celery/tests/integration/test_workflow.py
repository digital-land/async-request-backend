import pytest
import shutil
import os
import csv
from application.core.workflow import run_workflow


@pytest.fixture
def uploaded_csv(mock_directories):
    collection_dir = mock_directories.COLLECTION_DIR
    resource_dir = os.path.join(collection_dir, "resource")
    os.makedirs(resource_dir, exist_ok=True)
    mock_csv = os.path.join(resource_dir, "7e9a0e71f3ddfe")
    row = {
        "geometry": "MULTIPOLYGON (((-1.799316 53.717797, "
        "-1.790771 53.717797, -1.790771 53.721066, -1.799316 53.721066, -1.799316 53.717797)))",
        "reference": "4",
        "name": "South Jesmond",
    }
    fieldnames = row.keys()
    with open(mock_csv, "w") as f:
        dictwriter = csv.DictWriter(f, fieldnames=fieldnames)
        dictwriter.writeheader()
        dictwriter.writerow(row)

    return mock_csv


def test_run_workflow(
    mocker, mock_directories, mock_fetch_pipeline_csvs, test_data_dir, uploaded_csv
):
    collection = "article-4-direction"
    dataset = "article-4-direction-area"
    organisation = ""
    geom_type = ""
    fileName = uploaded_csv
    source_organisation_csv = f"{test_data_dir}/csvs/organisation.csv"
    destination_organisation_csv = os.path.join(
        mock_directories.CACHE_DIR, "organisation.csv"
    )
    shutil.copy(source_organisation_csv, destination_organisation_csv)

    mocker.patch(
        "application.core.workflow.fetch_pipeline_csvs",
        side_effect=mock_fetch_pipeline_csvs,
    )

    response_data = run_workflow(
        fileName, collection, dataset, organisation, geom_type, mock_directories
    )

    assert "converted-csv" in response_data
    assert "issue-log" in response_data
    assert "column-field-log" in response_data
    assert "flattened-csv" in response_data
    assert "error-summary" in response_data
