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
    _validate_source,
    DatasetResourceLog,
    ColumnFieldLog,
    IssueLog,
    _add_data_csv,
    _add_data_non_csv,
    _add_data_pipeline,
)


def test_fetch_add_data_response_success(monkeypatch, tmp_path):
    """Test successful execution of fetch_add_data_response"""
    dataset = "test-dataset"
    organisation = "test-org"
    collection = "test-collection"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    url = "http://example.com/endpoint"
    documentation_url = "http://example.com/doc"

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
    monkeypatch.setattr("src.application.core.pipeline._validate_endpoint",
                        lambda url, dir: {"endpoint_url_in_endpoint_csv": True})
    monkeypatch.setattr("src.application.core.pipeline._validate_source",
                        lambda *a, **k: {"documentation_url_in_source_csv": True})

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url
    )

    assert "entity-summary" in result
    assert "new-in-resource" in result["entity-summary"]
    assert "existing-in-resource" in result["entity-summary"]


def test_fetch_add_data_response_no_files(monkeypatch, tmp_path):
    """Test when input directory has no files"""
    dataset = "test-dataset"
    organisation = "test-org"
    collection = "test-collection"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    url = "http://example.com/endpoint"
    documentation_url = "http://example.com/doc"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)
    monkeypatch.setattr("src.application.core.pipeline._validate_endpoint",
                        lambda url, dir: {"endpoint_url_in_endpoint_csv": True})
    monkeypatch.setattr("src.application.core.pipeline._validate_source",
                        lambda *a, **k: {"documentation_url_in_source_csv": True})

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 0


def test_fetch_add_data_response_file_not_found(monkeypatch, tmp_path):
    """Test when input path does not exist"""
    dataset = "test-dataset"
    organisation = "test-org"
    collection = "test-collection"
    request_id = "req-003"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "nonexistent"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    url = "http://example.com/endpoint"
    documentation_url = "http://example.com/doc"

    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)

    with pytest.raises(FileNotFoundError):
        fetch_add_data_response(
            collection=collection,
            dataset=dataset,
            organisation=organisation,
            pipeline_dir=str(pipeline_dir),
            input_path=str(input_path),
            specification_dir=str(specification_dir),
            cache_dir=str(cache_dir),
            url=url,
            documentation_url=documentation_url
        )


def test_fetch_add_data_response_with_existing_entities(monkeypatch, tmp_path):
    """Test processing with mix of new and existing entities"""
    dataset = "test-dataset"
    organisation = "test-org"
    collection = "test-collection"
    request_id = "req-005"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    url = "http://example.com/endpoint"
    documentation_url = "http://example.com/doc"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("reference\nREF001\nREF002\nREF003")

    converted_dir = tmp_path / "pipeline" / "converted"
    converted_dir.mkdir(parents=True, exist_ok=True)
    converted_csv = converted_dir / "test.csv"
    converted_csv.write_text("reference\nREF001\nREF002\nREF003", encoding="utf-8")

    lookup_file = pipeline_dir / "lookup.csv"
    lookup_file.write_text(
        "prefix,resource,organisation,reference,entity\n"
        "test-prefix,test,test-org,REF001,1000001\n"
    )

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    mock_lookups_instance = MagicMock()
    mock_lookups_instance.lookups_path = str(lookup_file)
    mock_lookups_instance.get_max_entity.return_value = 1000001
    mock_lookups_instance.save_csv.return_value = [
        {"prefix": "test-prefix", "organisation": "test-org", "reference": "REF002", "entity": "1000002",
         "resource": "test"},
        {"prefix": "test-prefix", "organisation": "test-org", "reference": "REF003", "entity": "1000003",
         "resource": "test"}
    ]
    mock_lookups_instance.entity_num_gen = MagicMock()
    mock_lookups_instance.entity_num_gen.state = {}

    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)
    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups_instance)
    monkeypatch.setattr("src.application.core.pipeline._validate_endpoint",
                        lambda url, dir: {"endpoint_url_in_endpoint_csv": True})
    monkeypatch.setattr("src.application.core.pipeline._validate_source",
                        lambda *a, **k: {"documentation_url_in_source_csv": True})
    monkeypatch.setattr("src.application.core.pipeline._add_data_pipeline",
                        lambda resource_file_path, resource_name, pipeline_dir, specification, dataset, pipeline,
                               organisation_path: str(converted_csv))

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 2
    assert result["entity-summary"]["existing-in-resource"] == 1


def test_fetch_add_data_response_handles_processing_error(monkeypatch, tmp_path):
    """Test handling of errors during file processing"""
    dataset = "test-dataset"
    organisation = "test-org"
    collection = "test-collection"
    request_id = "req-006"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    url = "http://example.com/endpoint"
    documentation_url = "http://example.com/doc"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("invalid csv content without proper headers")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda x: mock_spec)

    def raise_exception(*args, **kwargs):
        raise Exception("Processing error")

    monkeypatch.setattr("src.application.core.pipeline._add_data_read_entities", raise_exception)
    monkeypatch.setattr("src.application.core.pipeline._validate_endpoint",
                        lambda url, dir: {"endpoint_url_in_endpoint_csv": True})
    monkeypatch.setattr("src.application.core.pipeline._validate_source",
                        lambda *a, **k: {"documentation_url_in_source_csv": True})

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 0


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
    """Test creating new lookup file for assigning entities"""
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


def test_validate_endpoint_creates_file(monkeypatch, tmp_path):
    """Test that _validate_endpoint creates endpoint.csv if it doesn't exist"""
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

    assert not endpoint_csv_path.exists()

    _validate_endpoint(url, str(pipeline_dir))

    assert endpoint_csv_path.exists()

    with open(endpoint_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        assert headers == ['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date', 'start-date', 'end-date']


def test_validate_endpoint_appends(monkeypatch, tmp_path):
    """Test that _validate_endpoint appends new endpoint when URL not found"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/new-endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"

    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date',
                                               'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "endpoint": "existing_hash",
            "endpoint-url": "http://example.com/existing",
            "parameters": "",
            "plugin": "",
            "entry-date": "2024-01-01T00:00:00",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    def fake_append_endpoint(endpoint_csv_path, endpoint_url, entry_date, start_date, end_date):
        return "new_endpoint_hash", {
            "endpoint": "new_endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_endpoint", fake_append_endpoint)

    result = _validate_endpoint(url, str(pipeline_dir))

    assert result["endpoint_url_in_endpoint_csv"] is False
    assert "new_endpoint_entry" in result
    assert result["new_endpoint_entry"]["endpoint"] == "new_endpoint_hash"
    assert result["new_endpoint_entry"]["endpoint-url"] == url


def test_validate_endpoint_finds_existing(monkeypatch, tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date',
                                               'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "endpoint": "endpoint_hash",
            "endpoint-url": url,
            "parameters": "",
            "plugin": "",
            "entry-date": "2024-01-01",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    monkeypatch.setattr("src.application.core.pipeline.append_endpoint",
                        lambda *a, **kw: (_ for _ in ()).throw(Exception("Should not be called")))

    result = _validate_endpoint(url, str(pipeline_dir))
    assert result["endpoint_url_in_endpoint_csv"] is True
    assert "existing_endpoint_entry" in result
    assert result["existing_endpoint_entry"]["endpoint-url"] == url


def test_validate_endpoint_empty_url(monkeypatch, tmp_path):
    """Test _validate_endpoint with empty URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    result = _validate_endpoint("", str(pipeline_dir))

    assert result == {}


def test_validate_endpoint_csv_read_error(monkeypatch, tmp_path):
    """Test _validate_endpoint when reading CSV fails"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"

    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    endpoint_csv_path.write_bytes(b'\x00\x00\x00')

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

    assert "new_endpoint_entry" in result or "endpoint_url_in_endpoint_csv" in result


def test_validate_source_creates_new_source(monkeypatch, tmp_path):
    """Test _validate_source creates new source entry when it doesn't exist"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert result["documentation_url_in_source_csv"] is False
    assert "new_source_entry" in result
    assert result["new_source_entry"]["source"] == "source_hash_456"
    assert result["new_source_entry"]["collection"] == collection
    assert result["new_source_entry"]["organisation"] == organisation
    assert result["new_source_entry"]["pipelines"] == dataset


def test_validate_source_finds_existing_source(monkeypatch, tmp_path):
    """Test _validate_source finds existing source entry"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    with open(source_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['source', 'attribution', 'collection',
                                               'documentation-url', 'endpoint', 'licence',
                                               'organisation', 'pipelines', 'entry-date',
                                               'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "source": "existing_source_hash",
            "attribution": "",
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": "endpoint_hash_123",
            "licence": "",
            "organisation": organisation,
            "pipelines": dataset,
            "entry-date": "2024-01-01T00:00:00",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert result["documentation_url_in_source_csv"] is True
    assert "existing_source_entry" in result
    assert result["existing_source_entry"]["source"] == "existing_source_hash"
    assert result["existing_source_entry"]["collection"] == collection


def test_validate_source_no_endpoint_key(tmp_path):
    """Test _validate_source returns empty dict when no endpoint key available"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {}

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert result == {}


def test_validate_source_empty_documentation_url(monkeypatch, tmp_path):
    """Test _validate_source handles empty documentation URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = ""
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert "documentation_url_in_source_csv" in result
    assert result["new_source_entry"]["documentation-url"] == ""


def test_validate_source_uses_new_endpoint_entry(monkeypatch, tmp_path):
    """Test _validate_source uses endpoint from new_endpoint_entry when available"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": False,
        "new_endpoint_entry": {
            "endpoint": "new_endpoint_hash_789"
        }
    }

    captured_endpoint_key = None

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        nonlocal captured_endpoint_key
        captured_endpoint_key = endpoint_key
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert captured_endpoint_key == "new_endpoint_hash_789"
    assert result["new_source_entry"]["endpoint"] == "new_endpoint_hash_789"


def test_validate_source_handles_csv_read_error(monkeypatch, tmp_path):
    """Test _validate_source handles CSV read errors gracefully"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    source_csv_path.write_bytes(b'\x00\x00\x00')

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert "documentation_url_in_source_csv" in result


def test_validate_endpoint_creates_file(monkeypatch, tmp_path):
    """Test that _validate_endpoint creates endpoint.csv if it doesn't exist"""
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

    assert not endpoint_csv_path.exists()

    _validate_endpoint(url, str(pipeline_dir))

    assert endpoint_csv_path.exists()

    with open(endpoint_csv_path, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        headers = next(reader)
        assert headers == ['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date', 'start-date', 'end-date']


def test_validate_endpoint_appends(monkeypatch, tmp_path):
    """Test that _validate_endpoint appends new endpoint when URL not found"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/new-endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"

    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date',
                                               'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "endpoint": "existing_hash",
            "endpoint-url": "http://example.com/existing",
            "parameters": "",
            "plugin": "",
            "entry-date": "2024-01-01T00:00:00",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    def fake_append_endpoint(endpoint_csv_path, endpoint_url, entry_date, start_date, end_date):
        return "new_endpoint_hash", {
            "endpoint": "new_endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_endpoint", fake_append_endpoint)

    result = _validate_endpoint(url, str(pipeline_dir))

    assert result["endpoint_url_in_endpoint_csv"] is False
    assert "new_endpoint_entry" in result
    assert result["new_endpoint_entry"]["endpoint"] == "new_endpoint_hash"
    assert result["new_endpoint_entry"]["endpoint-url"] == url


def test_validate_endpoint_finds_existing(monkeypatch, tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=['endpoint', 'endpoint-url', 'parameters', 'plugin', 'entry-date',
                                               'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "endpoint": "endpoint_hash",
            "endpoint-url": url,
            "parameters": "",
            "plugin": "",
            "entry-date": "2024-01-01",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    monkeypatch.setattr("src.application.core.pipeline.append_endpoint",
                        lambda *a, **kw: (_ for _ in ()).throw(Exception("Should not be called")))

    result = _validate_endpoint(url, str(pipeline_dir))
    assert result["endpoint_url_in_endpoint_csv"] is True
    assert "existing_endpoint_entry" in result
    assert result["existing_endpoint_entry"]["endpoint-url"] == url


def test_validate_endpoint_empty_url(monkeypatch, tmp_path):
    """Test _validate_endpoint with empty URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    result = _validate_endpoint("", str(pipeline_dir))

    assert result == {}


def test_validate_endpoint_csv_read_error(monkeypatch, tmp_path):
    """Test _validate_endpoint when reading CSV fails"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"

    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    endpoint_csv_path.write_bytes(b'\x00\x00\x00')

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

    assert "new_endpoint_entry" in result or "endpoint_url_in_endpoint_csv" in result


def test_validate_source_creates_new_source(monkeypatch, tmp_path):
    """Test _validate_source creates new source entry when it doesn't exist"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert result["documentation_url_in_source_csv"] is False
    assert "new_source_entry" in result
    assert result["new_source_entry"]["source"] == "source_hash_456"
    assert result["new_source_entry"]["collection"] == collection
    assert result["new_source_entry"]["organisation"] == organisation
    assert result["new_source_entry"]["pipelines"] == dataset


def test_validate_source_finds_existing_source(monkeypatch, tmp_path):
    """Test _validate_source finds existing source entry"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    with open(source_csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['source', 'attribution', 'collection',
                                               'documentation-url', 'endpoint', 'licence',
                                               'organisation', 'pipelines', 'entry-date',
                                               'start-date', 'end-date'])
        writer.writeheader()
        writer.writerow({
            "source": "existing_source_hash",
            "attribution": "",
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": "endpoint_hash_123",
            "licence": "",
            "organisation": organisation,
            "pipelines": dataset,
            "entry-date": "2024-01-01T00:00:00",
            "start-date": "2024-01-01",
            "end-date": ""
        })

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert result["documentation_url_in_source_csv"] is True
    assert "existing_source_entry" in result
    assert result["existing_source_entry"]["source"] == "existing_source_hash"
    assert result["existing_source_entry"]["collection"] == collection


def test_validate_source_no_endpoint_key(tmp_path):
    """Test _validate_source returns empty dict when no endpoint key available"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {}

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert result == {}


def test_validate_source_empty_documentation_url(monkeypatch, tmp_path):
    """Test _validate_source handles empty documentation URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = ""
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert "documentation_url_in_source_csv" in result
    assert result["new_source_entry"]["documentation-url"] == ""


def test_validate_source_uses_new_endpoint_entry(monkeypatch, tmp_path):
    """Test _validate_source uses endpoint from new_endpoint_entry when available"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": False,
        "new_endpoint_entry": {
            "endpoint": "new_endpoint_hash_789"
        }
    }

    captured_endpoint_key = None

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        nonlocal captured_endpoint_key
        captured_endpoint_key = endpoint_key
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date
        }

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert captured_endpoint_key == "new_endpoint_hash_789"
    assert result["new_source_entry"]["endpoint"] == "new_endpoint_hash_789"


def test_validate_source_handles_csv_read_error(monkeypatch, tmp_path):
    """Test _validate_source handles CSV read errors gracefully"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {
            "endpoint": "endpoint_hash_123"
        }
    }

    source_csv_path.write_bytes(b'\x00\x00\x00')

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary
    )

    assert "documentation_url_in_source_csv" in result


class DummyAPI:
    def __init__(self, specification=None):
        pass

    def get_valid_category_values(self, dataset, pipeline):
        return {}


def _patch_api(monkeypatch):
    monkeypatch.setattr("src.application.core.pipeline.API", DummyAPI)


def test__add_data_csv_phase_order(tmp_path):
    resource_path = tmp_path / "input.csv"
    resource_path.write_text("reference,name\nREF001,Test\n", encoding="utf-8")
    converted_path = tmp_path / "converted.csv"

    phases = _add_data_csv(
        resource_file_path=str(resource_path),
        converted_csv_path=str(converted_path),
        dataset_resource_log=DatasetResourceLog(dataset="ds", resource="input"),
        skip_patterns=[],
        intermediate_fieldnames=["reference", "name"],
        columns=[("reference", "reference"), ("name", "name")],
        column_field_log=ColumnFieldLog(dataset="ds", resource="input"),
    )
    phase_names = [type(p).__name__ for p in phases]
    assert phase_names == ["ConvertPhase", "NormalisePhase", "ParsePhase", "MapPhase", "SavePhase"]


def test__add_data_non_csv_phase_order(tmp_path, monkeypatch):
    _patch_api(monkeypatch)
    resource_path = tmp_path / "input.geojson"
    resource_path.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
    converted_path = tmp_path / "converted.csv"
    harmonised_path = tmp_path / "harm.csv"
    transformed_path = tmp_path / "transformed.csv"
    organisation_csv = tmp_path / "organisation.csv"
    organisation_csv.write_text("organisation,name\ntest-org,Test Organisation\n", encoding="utf-8")

    mock_spec = MagicMock()
    mock_spec.get_field_datatype_map.return_value = {}
    mock_spec.schema_field = {
        "tree": ["reference", "name", "geometry", "point"]
    }
    mock_spec.current_fieldnames.return_value = ["reference"]
    mock_spec.intermediate_fieldnames.return_value = ["reference", "name"]
    mock_spec.get_field_typology_map.return_value = {}
    mock_spec.get_field_prefix_map.return_value = {}
    mock_spec.get_odp_collections.return_value = {}
    mock_spec.factor_fieldnames.return_value = ["reference"]
    mock_spec.dataset_prefix.return_value = "pfx"
    mock_spec.pipeline = {"tree": {"schema": "tree"}}

    print(f"schema_field type: {type(mock_spec.schema_field)}")
    print(f"schema_field['tree'] type: {type(mock_spec.schema_field['tree'])}")
    print(f"get_odp_collections type: {type(mock_spec.get_odp_collections())}")
    assert isinstance(mock_spec.schema_field, dict), "schema_field must be dict"
    assert isinstance(mock_spec.schema_field["tree"],
                      list), "schema_field['tree'] must be list"  # ← CHANGE: list, not dict
    assert isinstance(mock_spec.get_odp_collections(), dict), "get_odp_collections must return dict"

    mock_pipeline = MagicMock()
    mock_pipeline.name = "tree"
    mock_pipeline.filters.return_value = {}
    mock_pipeline.migrations.return_value = []
    mock_pipeline.path = str(tmp_path)

    phases = _add_data_non_csv(
        resource_file_path=str(resource_path),
        converted_csv_path=str(converted_path),
        harmonised_csv_path=str(harmonised_path),
        transformed_csv_path=str(transformed_path),
        dataset_resource_log=DatasetResourceLog(dataset="tree", resource="input"),
        issue_log=IssueLog(dataset="tree", resource="input"),
        column_field_log=ColumnFieldLog(dataset="tree", resource="input"),
        specification=mock_spec,
        pipeline=mock_pipeline,
        resource_name="input",
        skip_patterns=[],
        concats={},
        columns=[("reference", "reference")],
        patches=[],
        default_fields=["organisation"],
        default_values={"organisation": "org1"},
        combine_fields=[],
        lookups=MagicMock(),
        organisation_path=str(organisation_csv),
    )
    phase_names = [type(p).__name__ for p in phases]
    assert phase_names[0] == "ConvertPhase"
    assert "HarmonisePhase" in phase_names
    assert phase_names[-1] == "SavePhase"
    assert phase_names.count("SavePhase") == 2


def test__add_data_pipeline_csv_returns_converted(tmp_path, monkeypatch):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("reference,name\nREF001,Test\n", encoding="utf-8")
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    mock_spec = MagicMock()
    mock_spec.intermediate_fieldnames.return_value = ["reference", "name"]

    mock_pipeline = MagicMock()
    mock_pipeline.skip_patterns.return_value = []
    mock_pipeline.concatenations.return_value = []
    mock_pipeline.columns.return_value = [("reference", "reference"), ("name", "name")]
    mock_pipeline.patches.return_value = []
    mock_pipeline.default_fields.return_value = []
    mock_pipeline.default_values.return_value = {}
    mock_pipeline.combine_fields.return_value = []
    mock_pipeline.lookups.return_value = MagicMock()
    mock_pipeline.path = str(tmp_path)

    def fake_run_pipeline(*phases):
        convert_phase = phases[0]
        log = getattr(convert_phase, "dataset_resource_log", None)
        out = getattr(convert_phase, "output_path", None)
        if log:
            log.mime_type = "text/csv"
        if out:
            os.makedirs(os.path.dirname(out), exist_ok=True)
            with open(out, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["reference", "name"])
                writer.writerow(["REF001", "Test"])

    monkeypatch.setattr("src.application.core.pipeline.run_pipeline", fake_run_pipeline)

    result = _add_data_pipeline(
        resource_file_path=str(input_csv),
        resource_name="input",
        pipeline_dir=str(pipeline_dir),
        specification=mock_spec,
        dataset="ds",
        pipeline=mock_pipeline,
        organisation_path=str(tmp_path / "organisation.csv"),
    )
    assert result.endswith("/converted/input.csv")
    assert os.path.exists(result)


def test__add_data_pipeline_non_csv_returns_harmonised(tmp_path, monkeypatch):
    _patch_api(monkeypatch)
    original_cwd = os.getcwd()
    os.chdir(tmp_path)

    try:
        input_geo = tmp_path / "input.geojson"
        input_geo.write_text('{"type":"FeatureCollection","features":[]}', encoding="utf-8")
        pipeline_dir = tmp_path / "pipeline"
        pipeline_dir.mkdir()

        organisation_csv = tmp_path / "organisation.csv"
        organisation_csv.write_text("organisation,name\ntest-org,Test Organisation\n", encoding="utf-8")

        mock_spec = MagicMock()
        mock_spec.intermediate_fieldnames.return_value = ["reference", "name"]
        mock_spec.factor_fieldnames.return_value = ["reference"]
        mock_spec.get_field_datatype_map.return_value = {}
        mock_spec.get_field_typology_map.return_value = {}
        mock_spec.get_field_prefix_map.return_value = {}
        mock_spec.get_odp_collections.return_value = {}
        mock_spec.current_fieldnames.return_value = ["reference"]
        mock_spec.dataset_prefix.return_value = "pfx"
        mock_spec.schema_field = {"tree": ["reference", "name", "geometry", "point"]}
        mock_spec.pipeline = {"tree": {"schema": "tree"}}

        mock_pipeline = MagicMock()
        mock_pipeline.skip_patterns.return_value = []
        mock_pipeline.concatenations.return_value = {}
        mock_pipeline.columns.return_value = [("reference", "reference")]
        mock_pipeline.patches.return_value = []
        mock_pipeline.default_fields.return_value = []
        mock_pipeline.default_values.return_value = {}
        mock_pipeline.combine_fields.return_value = []
        mock_pipeline.lookups.return_value = MagicMock()
        mock_pipeline.filters.return_value = {}
        mock_pipeline.migrations.return_value = []
        mock_pipeline.name = "tree"
        mock_pipeline.path = str(tmp_path)

        def fake_run_pipeline(*phases):
            convert_phase = phases[0]
            if hasattr(convert_phase, "dataset_resource_log"):
                convert_phase.dataset_resource_log.mime_type = "application/json"

            save_phase_count = 0
            for p in phases:
                if type(p).__name__ == "SavePhase":
                    out = getattr(p, "output_path", None) or getattr(p, "path", None) or getattr(p, "_output_path", "")
                    enabled = getattr(p, "enabled", False)

                    if out and enabled and save_phase_count == 0:
                        os.makedirs(os.path.dirname(out), exist_ok=True)
                        with open(out, "w", encoding="utf-8", newline="") as f:
                            writer = csv.writer(f)
                            writer.writerow(["reference"])
                            writer.writerow(["REF001"])
                        save_phase_count += 1

        monkeypatch.setattr("src.application.core.pipeline.run_pipeline", fake_run_pipeline)

        result = _add_data_pipeline(
            resource_file_path=str(input_geo),
            resource_name="input",
            pipeline_dir=str(pipeline_dir),
            specification=mock_spec,
            dataset="tree",
            pipeline=mock_pipeline,
            organisation_path=str(organisation_csv),
        )

        expected = os.path.join("harmonised", "input.csv")
        assert os.path.normpath(result) == os.path.normpath(expected), f"Expected {expected}, got: {result}"
        assert os.path.exists(result), f"File does not exist: {result}"

    finally:
        os.chdir(original_cwd)


def test__add_data_pipeline_non_csv_fallback_to_converted_if_no_harmonised(tmp_path, monkeypatch):
    _patch_api(monkeypatch)
    input_geo = tmp_path / "input.geojson"
    input_geo.write_text("{}", encoding="utf-8")
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    organisation_csv = tmp_path / "organisation.csv"
    organisation_csv.write_text("organisation,name\ntest-org,Test Organisation\n", encoding="utf-8")

    mock_spec = MagicMock()
    mock_spec.intermediate_fieldnames.return_value = ["reference"]
    mock_spec.factor_fieldnames.return_value = ["reference"]

    mock_pipeline = MagicMock()
    mock_pipeline.skip_patterns.return_value = []
    mock_pipeline.concatenations.return_value = {}
    mock_pipeline.columns.return_value = [("reference", "reference")]
    mock_pipeline.patches.return_value = []
    mock_pipeline.default_fields.return_value = []
    mock_pipeline.default_values.return_value = {}
    mock_pipeline.combine_fields.return_value = []
    mock_pipeline.lookups.return_value = MagicMock()
    mock_pipeline.filters.return_value = {}
    mock_pipeline.migrations.return_value = []
    mock_pipeline.name = "tree"
    mock_pipeline.path = str(tmp_path)

    def fake_run_pipeline(*phases):
        phases[0].dataset_resource_log.mime_type = "application/json"
        out = phases[0].output_path
        os.makedirs(os.path.dirname(out), exist_ok=True)
        with open(out, "w", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["reference"])
            writer.writerow(["REF001"])

    monkeypatch.setattr("src.application.core.pipeline.run_pipeline", fake_run_pipeline)

    result = _add_data_pipeline(
        resource_file_path=str(input_geo),
        resource_name="input",
        pipeline_dir=str(pipeline_dir),
        specification=mock_spec,
        dataset="tree",
        pipeline=mock_pipeline,
        organisation_path=str(organisation_csv),
    )
    assert result.endswith("/converted/input.csv") or result.endswith("/input.geojson")
    assert os.path.exists(result) or os.path.exists(str(input_geo))


def test__add_data_pipeline_csv_missing_converted_raises(tmp_path, monkeypatch):
    input_csv = tmp_path / "input.csv"
    input_csv.write_text("reference\nR1\n", encoding="utf-8")
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    mock_spec = MagicMock()
    mock_spec.intermediate_fieldnames.return_value = ["reference"]

    mock_pipeline = MagicMock()
    mock_pipeline.skip_patterns.return_value = []
    mock_pipeline.concatenations.return_value = []
    mock_pipeline.columns.return_value = [("reference", "reference")]
    mock_pipeline.patches.return_value = []
    mock_pipeline.default_fields.return_value = []
    mock_pipeline.default_values.return_value = {}
    mock_pipeline.combine_fields.return_value = []
    mock_pipeline.lookups.return_value = MagicMock()
    mock_pipeline.path = str(tmp_path)

    def fake_run_pipeline(*phases):
        phases[0].dataset_resource_log.mime_type = "text/csv"

    monkeypatch.setattr("src.application.core.pipeline.run_pipeline", fake_run_pipeline)

    result = _add_data_pipeline(
        resource_file_path=str(input_csv),
        resource_name="input",
        pipeline_dir=str(pipeline_dir),
        specification=mock_spec,
        dataset="ds",
        pipeline=mock_pipeline,
        organisation_path=str(tmp_path / "organisation.csv"),
    )
    assert result.endswith("/converted/input.csv")


def test_validate_endpoint_appends_new_when_missing(tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    endpoint_csv = pipeline_dir / "endpoint.csv"
    if endpoint_csv.exists():
        endpoint_csv.unlink()

    summary = _validate_endpoint("http://example.com/wfs", str(pipeline_dir))
    assert endpoint_csv.exists()
    with open(endpoint_csv, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        assert len(rows) == 1
        assert rows[0]["endpoint-url"] == "http://example.com/wfs"
    assert "endpoint_url_in_endpoint_csv" in summary


def test_validate_endpoint_detects_existing(tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    endpoint_csv = pipeline_dir / "endpoint.csv"
    with open(endpoint_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["endpoint", "endpoint-url", "parameters", "plugin", "entry-date", "start-date", "end-date"])
        writer.writerow(["hash123", "http://example.com/wfs", "", "", "2025-01-01T00:00:00", "2025-01-01", ""])

    summary = _validate_endpoint("http://example.com/wfs", str(pipeline_dir))
    assert summary["endpoint_url_in_endpoint_csv"] is True


def test_validate_source_appends_with_endpoint_key(tmp_path, monkeypatch):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv = pipeline_dir / "source.csv"
    endpoint_summary = {"new_endpoint_entry": {"endpoint": "hash123"}}

    def fake_append_source(source_csv_path, collection, organisation, endpoint_key,
                           attribution, documentation_url, licence, pipelines,
                           entry_date, start_date, end_date):
        new_row = {
            "source": "src_hash",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date or "",
            "start-date": start_date or "",
            "end-date": end_date or "",
        }
        os.makedirs(os.path.dirname(source_csv_path), exist_ok=True)
        write_header = not os.path.exists(source_csv_path) or os.path.getsize(source_csv_path) == 0
        with open(source_csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(new_row.keys()))
            if write_header:
                writer.writeheader()
            writer.writerow(new_row)
        return "src_hash", new_row

    monkeypatch.setattr("src.application.core.pipeline.append_source", fake_append_source)

    summary = _validate_source(
        documentation_url="http://example.com/doc",
        pipeline_dir=str(pipeline_dir),
        collection="tree",
        organisation="org1",
        dataset="tree",
        endpoint_summary=endpoint_summary,
    )
    assert "documentation_url_in_source_csv" in summary
    assert source_csv.exists()
    with open(source_csv, "r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
        assert len(rows) == 1
        assert rows[0]["documentation-url"] == "http://example.com/doc"


def test_validate_source_reads_existing(tmp_path, monkeypatch):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv = pipeline_dir / "source.csv"
    source_key = "src1"
    with open(source_csv, "w", encoding="utf-8", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            ["source", "collection", "organisation", "endpoint", "attribution", "documentation-url", "licence",
             "pipelines", "entry-date", "start-date", "end-date"])
        writer.writerow(
            [source_key, "tree", "org1", "hash123", "", "http://example.com/doc", "", "tree", "2025-01-01T00:00:00",
             "2025-01-01", ""])

    monkeypatch.setattr(
        "src.application.core.pipeline.append_source",
        lambda *args, **kwargs: (source_key, None)
    )

    endpoint_summary = {"existing_endpoint_entry": {"endpoint": "hash123"}}
    summary = _validate_source(
        documentation_url="http://example.com/doc",
        pipeline_dir=str(pipeline_dir),
        collection="tree",
        organisation="org1",
        dataset="tree",
        endpoint_summary=endpoint_summary,
    )
    assert summary["documentation_url_in_source_csv"] is True


def test_fetch_add_data_response_handles_multiple_files(tmp_path, monkeypatch):
    pipeline_dir = tmp_path / "pipeline"
    input_dir = tmp_path / "collection" / "resource" / "req-42"
    spec_dir = tmp_path / "spec"
    cache_dir = tmp_path / "cache"
    for d in (pipeline_dir, input_dir, spec_dir, cache_dir):
        d.mkdir(parents=True, exist_ok=True)
    (input_dir / "a.csv").write_text("reference\nA1\n", encoding="utf-8")
    (input_dir / "b.geojson").write_text("{}", encoding="utf-8")

    class DummySpec:
        def dataset_prefix(self, ds): return "pfx"

        def get_dataset_entity_min(self, ds): return 1000000

        def get_dataset_entity_max(self, ds): return 9999999

        def intermediate_fieldnames(self, pipe): return ["reference"]

        def factor_fieldnames(self): return ["reference"]

        pipeline = {"x": {"schema": "schema"}}

    class DummyPipeline:
        def __init__(self, d, ds): self.name = ds; self.path = d

        def skip_patterns(self, r): return []

        def concatenations(self, r, endpoints=None): return []

        def columns(self, r, endpoints=None): return [("reference", "reference")]

        def patches(self, resource=None): return []

        def default_fields(self, resource=None): return []

        def default_values(self, endpoints=None): return {}

        def combine_fields(self, endpoints=None): return []

        def lookups(self, resource=None): return MagicMock()

        def filters(self, r): return []

        def migrations(self): return []

    monkeypatch.setattr("src.application.core.pipeline.Specification", lambda p: DummySpec())
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", lambda d, ds: DummyPipeline(d, ds))

    def fake_run_pipeline(*phases):
        path = getattr(convert, "path", "")
        log = getattr(convert, "dataset_resource_log", None)
        out = getattr(convert, "output_path", None)
        if path.endswith(".csv"):
            if log: log.mime_type = "text/csv"
            if out:
                os.makedirs(os.path.dirname(out), exist_ok=True)
                with open(out, "w", encoding="utf-8", newline="") as f:
                    w = csv.writer(f)
                    w.writerow(["reference"])
                    w.writerow(["A1"])
        else:
            if log: log.mime_type = "application/json"
            for p in phases:
                if type(p).__name__ == "SavePhase":
                    o = getattr(p, "output_path", "")
                    if "harmonised/" in o:
                        os.makedirs(os.path.dirname(o), exist_ok=True)
                        with open(o, "w", encoding="utf-8", newline="") as f:
                            w = csv.writer(f)
                            w.writerow(["reference"])
                            w.writerow(["B1"])
                    if "transformed/" in o:
                        os.makedirs(os.path.dirname(o), exist_ok=True)
                        with open(o, "w", encoding="utf-8", newline="") as f:
                            w = csv.writer(f)
                            w.writerow(["reference"])
                            w.writerow(["B1"])

    monkeypatch.setattr("src.application.core.pipeline.run_pipeline", fake_run_pipeline)

    res = fetch_add_data_response(
        collection="tree",
        dataset="tree",
        organisation="local-authority:ABC",
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_dir),
        specification_dir=str(spec_dir),
        cache_dir=str(cache_dir),
        url="http://endpoint",
        documentation_url="http://docs",
    )
    assert "entity-summary" in res
    assert "endpoint-summary" in res
    assert "source-summary" in res