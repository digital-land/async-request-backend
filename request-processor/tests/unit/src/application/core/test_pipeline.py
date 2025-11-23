import pytest
import os
import csv
from unittest.mock import MagicMock
from src.application.core.pipeline import (
    fetch_add_data_response,
    _add_data_read_entities,
    _check_existing_entities,
    _assign_entity_numbers,
    _get_entities_breakdown,
    _get_existing_entities_breakdown,
    _validate_endpoint,
)

def test_fetch_add_data_response_success(monkeypatch, tmp_path):
    """Test successful execution of fetch_add_data_response"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"


    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)
    test_file = input_path / "test.csv"
    test_file.write_text("reference\nREF001\nREF002")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    mock_lookups_instance = MagicMock()
    mock_lookups_instance.lookups_path = str(pipeline_dir / "lookup.csv")
    mock_lookups_instance.get_max_entity.return_value = 1000000
    mock_lookups_instance.save_csv.return_value = [
        {"prefix": "test-prefix", "organisation": "test-org", "reference": "REF001", "entity": "1000001",
         "resource": "test"},
        {"prefix": "test-prefix", "organisation": "test-org", "reference": "REF002", "entity": "1000002",
         "resource": "test"}
    ]
    mock_lookups_instance.entity_num_gen = MagicMock()
    mock_lookups_instance.entity_num_gen.state = {}

    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)
    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups_instance)

    result = fetch_add_data_response(
        fetch_csv=[],
        request_id="req-001",
        collection="test-collection",
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        file_name="test.csv",
        input_path=str(input_path),
        file_path=str(input_path) + "/test.csv",
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url="http://example.com/endpoint",
        documentation_url="http://example.com/doc"
    )

    assert "entity-summary" in result
    assert "new-in-resource" in result["entity-summary"]
    assert "existing-in-resource" in result["entity-summary"]


def test_fetch_add_data_response_no_files(monkeypatch, tmp_path):
    """Test when input directory has no files"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"

    input_path.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)

    result = fetch_add_data_response(
        fetch_csv=[],
        request_id="req-001",
        collection="test-collection",
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        file_name="test.csv",
        input_path=str(input_path),
        file_path=str(input_path) + "/test.csv",
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url="http://example.com/endpoint",
        documentation_url="http://example.com/doc"
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 0


def test_fetch_add_data_response_file_not_found(monkeypatch, tmp_path):
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    mock_spec = MagicMock()
    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)

    with pytest.raises(FileNotFoundError):
        fetch_add_data_response(
            fetch_csv=[],
            request_id="req-001",
            collection="test-collection",
            dataset=dataset,
            organisation=organisation,
            pipeline_dir=str(pipeline_dir),
            file_name="test.csv",
            input_path=str(input_path),
            file_path=str(input_path) + "/test.csv",
            specification_dir=str(specification_dir),
            cache_dir=str(cache_dir),
            url="http://example.com/endpoint",
            documentation_url="http://example.com/doc"
        )


def test_add_data_read_entities_success(tmp_path):
    """Test reading entities from resource file"""
    resource_file = tmp_path / "test.csv"
    resource_file.write_text("reference,name\nREF001,Test1\nREF002,Test2")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    result = _add_data_read_entities(
        str(resource_file),
        "test-dataset",
        "test-org",
        mock_spec
    )

    assert len(result) == 2
    assert result[0]["reference"] == "REF001"
    assert result[0]["prefix"] == "test-prefix"
    assert result[0]["organisation"] == "test-org"
    assert result[1]["reference"] == "REF002"


def test_add_data_read_entities_skip_empty_reference(tmp_path):
    """Test skipping rows with empty reference"""
    resource_file = tmp_path / "test.csv"
    resource_file.write_text("reference,name\nREF001,Test1\n,Test2\nREF003,Test3")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    result = _add_data_read_entities(
        str(resource_file),
        "test-dataset",
        "test-org",
        mock_spec
    )

    assert len(result) == 2
    assert result[0]["reference"] == "REF001"
    assert result[1]["reference"] == "REF003"


def test_add_data_read_entities_file_error():
    """Test handling of file read errors"""
    mock_spec = MagicMock()

    with pytest.raises(Exception):
        _add_data_read_entities(
            "/nonexistent/file.csv",
            "test-dataset",
            "test-org",
            mock_spec
        )


def test_check_existing_entities_all_new(tmp_path):
    """Test when all lookups are new"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""},
        {"prefix": "p1", "organisation": "org1", "reference": "REF002", "entity": ""}
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text("prefix,organisation,reference,entity\n")

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups,
        str(lookup_file)
    )

    assert len(new_lookups) == 2
    assert len(existing_lookups) == 0


def test_check_existing_entities_some_existing(tmp_path):
    """Test when some lookups already exist"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""},
        {"prefix": "p1", "organisation": "org1", "reference": "REF002", "entity": ""}
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text(
        "prefix,organisation,reference,entity\n"
        "p1,org1,REF001,1000001\n"
    )

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups,
        str(lookup_file)
    )

    assert len(new_lookups) == 1
    assert new_lookups[0]["reference"] == "REF002"
    assert len(existing_lookups) == 1
    assert existing_lookups[0]["entity"] == "1000001"


def test_check_existing_entities_no_lookup_file():
    """Test when lookup file doesn't exist"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""}
    ]

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups,
        "/nonexistent/lookup.csv"
    )

    assert len(new_lookups) == 1
    assert len(existing_lookups) == 0



def test_assign_entity_numbers_creates_lookup_file(monkeypatch, tmp_path):
    """Test creating new lookup file and assigning entities"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    new_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "resource": "res1", "entity": ""}
    ]

    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(pipeline_dir / "lookup.csv")
    mock_lookups.get_max_entity.return_value = 1000000
    mock_lookups.save_csv.return_value = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": "1000001", "resource": "res1"}
    ]
    mock_lookups.entity_num_gen = MagicMock()
    mock_lookups.entity_num_gen.state = {}

    mock_spec = MagicMock()
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups)

    result = _assign_entity_numbers(
        new_lookups,
        str(pipeline_dir),
        "test-dataset",
        mock_spec
    )

    assert len(result) == 1
    assert result[0]["entity"] == "1000001"
    mock_lookups.load_csv.assert_called_once()
    mock_lookups.add_entry.assert_called_once()
    mock_lookups.save_csv.assert_called_once()


def test_assign_entity_numbers_updates_existing_file(monkeypatch, tmp_path):
    """Test updating existing lookup file"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    lookup_file = pipeline_dir / "lookup.csv"
    lookup_file.write_text("prefix,resource,organisation,reference,entity\n")

    new_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "resource": "res1"}
    ]

    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(lookup_file)
    mock_lookups.get_max_entity.return_value = 1000005
    mock_lookups.save_csv.return_value = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": "1000006", "resource": "res1"}
    ]
    mock_lookups.entity_num_gen = MagicMock()
    mock_lookups.entity_num_gen.state = {}

    mock_spec = MagicMock()
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups)

    result = _assign_entity_numbers(
        new_lookups,
        str(pipeline_dir),
        "test-dataset",
        mock_spec
    )

    assert len(result) == 1
    assert mock_lookups.entity_num_gen.state['current'] == 1000005


def test_get_entities_breakdown_success():
    """Test converting entities to breakdown format"""
    new_entities = [
        {
            "entity": "1000001",
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1"
        },
        {
            "entity": "1000002",
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF002",
            "resource": "res1"
        }
    ]

    result = _get_entities_breakdown(new_entities)

    assert len(result) == 2
    assert result[0]["entity"] == "1000001"
    assert result[0]["reference"] == "REF001"
    assert result[0]["organisation"] == "org1"
    assert result[0]["prefix"] == "p1"
    assert result[0]["end-date"] == ""
    assert result[1]["entity"] == "1000002"


def test_get_entities_breakdown_empty_list():
    """Test with empty entity list"""
    result = _get_entities_breakdown([])
    assert result == []


def test_get_entities_breakdown_missing_fields():
    """Test handling entities with missing fields"""
    new_entities = [
        {"entity": "1000001"}
    ]

    result = _get_entities_breakdown(new_entities)

    assert len(result) == 1
    assert result[0]["entity"] == "1000001"
    assert result[0]["reference"] == ""
    assert result[0]["organisation"] == ""


def test_get_existing_entities_breakdown_success():
    """Test converting existing entities to simplified format"""
    existing_entities = [
        {"entity": "1000001", "reference": "REF001"},
        {"entity": "1000002", "reference": "REF002"}
    ]

    result = _get_existing_entities_breakdown(existing_entities)

    assert len(result) == 2
    assert result[0]["entity"] == "1000001"
    assert result[0]["reference"] == "REF001"
    assert result[1]["entity"] == "1000002"
    assert result[1]["reference"] == "REF002"


def test_get_existing_entities_breakdown_removes_duplicates():
    """Test that duplicate entities are removed"""
    existing_entities = [
        {"entity": "1000001", "reference": "REF001"},
        {"entity": "1000001", "reference": "REF001"},
        {"entity": "1000002", "reference": "REF002"}
    ]

    result = _get_existing_entities_breakdown(existing_entities)

    assert len(result) == 2


def test_get_existing_entities_breakdown_empty_list():
    """Test with empty entity list"""
    result = _get_existing_entities_breakdown([])
    assert result == []


def test_get_existing_entities_breakdown_filters_empty_values():
    """Test filtering entities with empty entity or reference"""
    existing_entities = [
        {"entity": "1000001", "reference": "REF001"},
        {"entity": "", "reference": "REF002"},
        {"entity": "1000003", "reference": ""},
        {"entity": "1000004", "reference": "REF004"}
    ]

    result = _get_existing_entities_breakdown(existing_entities)

    assert len(result) == 2
    assert result[0]["entity"] == "1000001"
    assert result[1]["entity"] == "1000004"

def test_validate_endpoint_creates_file_and_appends(monkeypatch, tmp_path):

    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"


    def fake_append_endpoint(endpoint_csv_path, endpoint_url, entry_date, start_date, end_date):
        return "endpoint_hash", {
            "endpoint": "endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }
    monkeypatch.setattr("src.application.core.pipeline.append_endpoint", fake_append_endpoint)


    result = _validate_endpoint(url, str(pipeline_dir))
    assert result["endpoint_url_in_endpoint_csv"] is False or "new_endpoint_entry" in result
    assert endpoint_csv_path.exists()

def test_validate_endpoint_finds_existing(monkeypatch, tmp_path):

    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date', 'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "endpoint": "endpoint_hash",
            "endpoint-url": url,
            "parameters": "",
            "plugin": "",
            "entry-date": "2024-01-01T00:00:00",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    monkeypatch.setattr("src.application.core.pipeline.append_endpoint", lambda *a, **kw: (_ for _ in ()).throw(Exception("Should not be called")))

    result = _validate_endpoint(url, str(pipeline_dir))
    assert result["endpoint_url_in_endpoint_csv"] is True
    assert "existing_endpoint_entry" in result
    assert result["existing_endpoint_entry"]["endpoint-url"] == url
