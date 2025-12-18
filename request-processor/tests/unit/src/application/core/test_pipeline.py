import pytest
import os
import csv
from unittest.mock import MagicMock, patch, mock_open
from src.application.core.pipeline import (
    fetch_add_data_response,
    _add_data_read_entities,
    _check_existing_entities,
    _assign_entity_numbers,
    _get_entities_breakdown,
    _get_existing_entities_breakdown,
    _validate_endpoint,
    _validate_source,
    _add_data_pipeline,
    _add_data_phases,
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
        {
            "prefix": "test-prefix",
            "organisation": "test-org",
            "reference": "REF001",
            "entity": "1000001",
            "resource": "test",
        },
        {
            "prefix": "test-prefix",
            "organisation": "test-org",
            "reference": "REF002",
            "entity": "1000002",
            "resource": "test",
        },
    ]
    mock_lookups_instance.entity_num_gen = MagicMock()
    mock_lookups_instance.entity_num_gen.state = {}

    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.Lookups", lambda x: mock_lookups_instance
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
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
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
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
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )

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
            documentation_url=documentation_url,
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
    cache_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("reference\nREF001\nREF002\nREF003")

    # Create organisation.csv
    org_csv = cache_dir / "organisation.csv"
    org_csv.write_text("organisation,name\ntest-org,Test Organisation")

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
        {
            "prefix": "test-prefix",
            "organisation": "test-org",
            "reference": "REF002",
            "entity": "1000002",
            "resource": "test",
        },
        {
            "prefix": "test-prefix",
            "organisation": "test-org",
            "reference": "REF003",
            "entity": "1000003",
            "resource": "test",
        },
    ]
    mock_lookups_instance.entity_num_gen = MagicMock()
    mock_lookups_instance.entity_num_gen.state = {}

    harmonised_dir = pipeline_dir / "harmonised"
    harmonised_dir.mkdir(parents=True)
    harmonised_file = harmonised_dir / "test.csv"
    harmonised_file.write_text(
        "reference,entity\n"
        "REF001,1000001\n"
        "REF002,\n"
        "REF003,\n"
    )

    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.Lookups", lambda x: mock_lookups_instance
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._add_data_pipeline",
        lambda *args, **kwargs: str(harmonised_file),
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
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

    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )

    def raise_exception(*args, **kwargs):
        raise Exception("Processing error")

    monkeypatch.setattr(
        "src.application.core.pipeline._add_data_read_entities", raise_exception
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
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
        str(resource_file), "test-dataset", "test-org", mock_spec
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
        str(resource_file), "test-dataset", "test-org", mock_spec
    )

    assert len(result) == 2
    assert result[0]["reference"] == "REF001"
    assert result[1]["reference"] == "REF003"


def test_add_data_read_entities_file_error():
    """Test handling of file read errors"""
    mock_spec = MagicMock()

    with pytest.raises(Exception):
        _add_data_read_entities(
            "/nonexistent/file.csv", "test-dataset", "test-org", mock_spec
        )


def test_check_existing_entities_all_new(tmp_path):
    """Test when all lookups are new"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""},
        {"prefix": "p1", "organisation": "org1", "reference": "REF002", "entity": ""},
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text("prefix,organisation,reference,entity\n")

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups, str(lookup_file)
    )

    assert len(new_lookups) == 2
    assert len(existing_lookups) == 0


def test_check_existing_entities_some_existing(tmp_path):
    """Test when some lookups already exist"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""},
        {"prefix": "p1", "organisation": "org1", "reference": "REF002", "entity": ""},
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text(
        "prefix,organisation,reference,entity\n" "p1,org1,REF001,1000001\n"
    )

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups, str(lookup_file)
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
        unidentified_lookups, "/nonexistent/lookup.csv"
    )

    assert len(new_lookups) == 1
    assert len(existing_lookups) == 0


def test_assign_entity_numbers_creates_lookup_file(monkeypatch, tmp_path):
    """Test creating new lookup file for assigning entities"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    new_lookups = [
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1",
            "entity": "",
        }
    ]

    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(pipeline_dir / "lookup.csv")
    mock_lookups.get_max_entity.return_value = 1000000
    mock_lookups.save_csv.return_value = [
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "entity": "1000001",
            "resource": "res1",
        }
    ]
    mock_lookups.entity_num_gen = MagicMock()
    mock_lookups.entity_num_gen.state = {}

    mock_spec = MagicMock()
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups)

    result = _assign_entity_numbers(
        new_lookups, str(pipeline_dir), "test-dataset", mock_spec
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
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1",
        }
    ]

    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(lookup_file)
    mock_lookups.get_max_entity.return_value = 1000005
    mock_lookups.save_csv.return_value = [
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "entity": "1000006",
            "resource": "res1",
        }
    ]
    mock_lookups.entity_num_gen = MagicMock()
    mock_lookups.entity_num_gen.state = {}

    mock_spec = MagicMock()
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups)

    result = _assign_entity_numbers(
        new_lookups, str(pipeline_dir), "test-dataset", mock_spec
    )

    assert len(result) == 1
    assert mock_lookups.entity_num_gen.state["current"] == 1000005


def test_get_entities_breakdown_success():
    """Test converting entities to breakdown format"""
    new_entities = [
        {
            "entity": "1000001",
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1",
        },
        {
            "entity": "1000002",
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF002",
            "resource": "res1",
        },
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
    new_entities = [{"entity": "1000001"}]

    result = _get_entities_breakdown(new_entities)

    assert len(result) == 1
    assert result[0]["entity"] == "1000001"
    assert result[0]["reference"] == ""
    assert result[0]["organisation"] == ""


def test_get_existing_entities_breakdown_success():
    """Test converting existing entities to simplified format"""
    existing_entities = [
        {"entity": "1000001", "reference": "REF001"},
        {"entity": "1000002", "reference": "REF002"},
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
        {"entity": "1000002", "reference": "REF002"},
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
        {"entity": "1000004", "reference": "REF004"},
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

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date
    ):
        return "endpoint_hash", {
            "endpoint": "endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_endpoint", fake_append_endpoint
    )

    assert not endpoint_csv_path.exists()

    _validate_endpoint(url, str(pipeline_dir))

    assert endpoint_csv_path.exists()

    with open(endpoint_csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        assert headers == [
            "endpoint",
            "endpoint-url",
            "parameters",
            "plugin",
            "entry-date",
            "start-date",
            "end-date",
        ]


def test_validate_endpoint_appends(monkeypatch, tmp_path):
    """Test that _validate_endpoint appends new endpoint when URL not found"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/new-endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"

    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "endpoint",
                "endpoint-url",
                "parameters",
                "plugin",
                "entry-date",
                "start-date",
                "end-date",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": "existing_hash",
                "endpoint-url": "http://example.com/existing",
                "parameters": "",
                "plugin": "",
                "entry-date": "2024-01-01T00:00:00",
                "start-date": "2024-01-01",
                "end-date": "",
            }
        )

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date
    ):
        return "new_endpoint_hash", {
            "endpoint": "new_endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_endpoint", fake_append_endpoint
    )

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
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "endpoint",
                "endpoint-url",
                "parameters",
                "plugin",
                "entry-date",
                "start-date",
                "end-date",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": "endpoint_hash",
                "endpoint-url": url,
                "parameters": "",
                "plugin": "",
                "entry-date": "2024-01-01",
                "start-date": "2024-01-01",
                "end-date": "",
            }
        )

    monkeypatch.setattr(
        "src.application.core.pipeline.append_endpoint",
        lambda *a, **kw: (_ for _ in ()).throw(Exception("Should not be called")),
    )

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
    endpoint_csv_path.write_bytes(b"\x00\x00\x00")

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date
    ):
        return "endpoint_hash", {
            "endpoint": "endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_endpoint", fake_append_endpoint
    )

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
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    def fake_append_source(
        source_csv_path,
        collection,
        organisation,
        endpoint_key,
        attribution,
        documentation_url,
        licence,
        pipelines,
        entry_date,
        start_date,
        end_date,
    ):
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
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_source", fake_append_source
    )

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
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
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    with open(source_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source",
                "attribution",
                "collection",
                "documentation-url",
                "endpoint",
                "licence",
                "organisation",
                "pipelines",
                "entry-date",
                "start-date",
                "end-date",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
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
                "end-date": "",
            }
        )

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr(
        "src.application.core.pipeline.append_source", fake_append_source
    )

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
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
        endpoint_summary,
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
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    def fake_append_source(
        source_csv_path,
        collection,
        organisation,
        endpoint_key,
        attribution,
        documentation_url,
        licence,
        pipelines,
        entry_date,
        start_date,
        end_date,
    ):
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
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_source", fake_append_source
    )

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
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
        "new_endpoint_entry": {"endpoint": "new_endpoint_hash_789"},
    }

    captured_endpoint_key = None

    def fake_append_source(
        source_csv_path,
        collection,
        organisation,
        endpoint_key,
        attribution,
        documentation_url,
        licence,
        pipelines,
        entry_date,
        start_date,
        end_date,
    ):
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
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_source", fake_append_source
    )

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
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
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    source_csv_path.write_bytes(b"\x00\x00\x00")

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr(
        "src.application.core.pipeline.append_source", fake_append_source
    )

    result = _validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
    )

    assert "documentation_url_in_source_csv" in result

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
    cache_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    mock_pipeline = MagicMock()
    
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline", lambda x, y: mock_pipeline
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 0


def test_fetch_add_data_response_file_not_found(monkeypatch, tmp_path):
    """Test when input path does not exist"""
    dataset = "test-dataset"
    organisation = "test-org"
    collection = "test-collection"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "nonexistent"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    url = "http://example.com/endpoint"
    documentation_url = "http://example.com/doc"

    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )

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
            documentation_url=documentation_url,
        )

def test_add_data_pipeline_returns_harmonised_path(monkeypatch, tmp_path):
    """Test _add_data_pipeline returns harmonised CSV path when it exists"""
    resource_file_path = tmp_path / "test.csv"
    resource_file_path.write_text("reference\nREF001")
    resource_name = "test"
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    
    mock_spec = MagicMock()
    mock_spec.pipeline = {"test-dataset": {"schema": "test-schema"}}
    mock_spec.intermediate_fieldnames.return_value = ["field1"]
    mock_spec.schema_field = {"test-schema": ["field1"]}
    mock_spec.current_fieldnames.return_value = ["field1"]
    mock_spec.factor_fieldnames.return_value = ["field1"]
    mock_spec.get_field_datatype_map.return_value = {}
    mock_spec.get_field_typology_map.return_value = {}
    mock_spec.get_field_prefix_map.return_value = {}
    mock_spec.get_odp_collections.return_value = []
    mock_spec.dataset_prefix.return_value = "test-prefix"
    
    mock_pipeline = MagicMock()
    mock_pipeline.name = "test-dataset"
    mock_pipeline.path = str(pipeline_dir)
    mock_pipeline.skip_patterns.return_value = []
    mock_pipeline.concatenations.return_value = {}
    mock_pipeline.columns.return_value = {}
    mock_pipeline.patches.return_value = []
    mock_pipeline.lookups.return_value = {}
    mock_pipeline.default_fields.return_value = {}
    mock_pipeline.default_values.return_value = {}
    mock_pipeline.combine_fields.return_value = {}
    mock_pipeline.filters.return_value = {}
    mock_pipeline.migrations.return_value = {}
    
    organisation_path = tmp_path / "organisation.csv"
    organisation_path.write_text("organisation,name\ntest-org,Test")

    # Create harmonised file that the function will find
    harmonised_dir = pipeline_dir / "harmonised"
    harmonised_dir.mkdir(exist_ok=True)
    harmonised_path = harmonised_dir / "test.csv"
    harmonised_path.write_text("entity,reference\n1000001,REF001")

    def mock_run_pipeline(*args):
        pass

    monkeypatch.setattr(
        "src.application.core.pipeline.run_pipeline", mock_run_pipeline
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.API", MagicMock()
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.Organisation", MagicMock()
    )

    result = _add_data_pipeline(
        resource_file_path=str(resource_file_path),
        resource_name=resource_name,
        pipeline_dir=str(pipeline_dir),
        specification=mock_spec,
        dataset="test-dataset",
        pipeline=mock_pipeline,
        organisation_path=str(organisation_path),
    )
    
    assert result is not None
    assert "harmonised" in result
    assert result.endswith("test.csv")

def test_add_data_phases_returns_correct_phase_list(tmp_path):
    """Test _add_data_phases returns proper list of pipeline phases"""
    resource_file_path = str(tmp_path / "test.csv")
    converted_csv_path = str(tmp_path / "converted.csv")
    harmonised_csv_path = str(tmp_path / "harmonised.csv")
    transformed_csv_path = str(tmp_path / "transformed.csv")
    
    mock_spec = MagicMock()
    mock_spec.pipeline = {"test-dataset": {"schema": "test-schema"}}
    mock_spec.intermediate_fieldnames.return_value = ["field1"]
    mock_spec.schema_field = {"test-schema": ["field1"]}
    mock_spec.current_fieldnames.return_value = ["field1"]
    mock_spec.factor_fieldnames.return_value = ["field1"]
    mock_spec.get_field_datatype_map.return_value = {}
    mock_spec.get_field_typology_map.return_value = {}
    mock_spec.get_field_prefix_map.return_value = {}
    mock_spec.get_odp_collections.return_value = []
    mock_spec.dataset_prefix.return_value = "test-prefix"
    
    mock_pipeline = MagicMock()
    mock_pipeline.name = "test-dataset"
    mock_pipeline.path = str(tmp_path)
    mock_pipeline.filters.return_value = {}
    mock_pipeline.migrations.return_value = {}
    
    dataset_resource_log = MagicMock()
    issue_log = MagicMock()
    column_field_log = MagicMock()
    organisation_path = str(tmp_path / "organisation.csv")
    
    with patch("src.application.core.pipeline.API") as mock_api:
        mock_api_instance = MagicMock()
        mock_api_instance.get_valid_category_values.return_value = {}
        mock_api.return_value = mock_api_instance
        
        with patch("src.application.core.pipeline.Organisation"):
            phases = _add_data_phases(
                resource_file_path=resource_file_path,
                converted_csv_path=converted_csv_path,
                harmonised_csv_path=harmonised_csv_path,
                transformed_csv_path=transformed_csv_path,
                dataset_resource_log=dataset_resource_log,
                issue_log=issue_log,
                column_field_log=column_field_log,
                specification=mock_spec,
                pipeline=mock_pipeline,
                resource_name="test",
                skip_patterns=[],
                concats={},
                columns={},
                patches=[],
                default_fields={},
                default_values={},
                combine_fields={},
                lookups={},
                organisation_path=organisation_path,
            )
    
    assert len(phases) > 0
    # Verify key phases are present
    phase_names = [phase.__class__.__name__ for phase in phases]
    assert "ConvertPhase" in phase_names
    assert "HarmonisePhase" in phase_names
    assert "OrganisationPhase" in phase_names
    assert "EntityLookupPhase" in phase_names
    assert "SavePhase" in phase_names



def test_add_data_read_entities_success(tmp_path):
    """Test reading entities from resource file"""
    resource_file = tmp_path / "test.csv"
    resource_file.write_text("reference,name\nREF001,Test1\nREF002,Test2")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    result = _add_data_read_entities(
        str(resource_file), "test-dataset", "test-org", mock_spec
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
        str(resource_file), "test-dataset", "test-org", mock_spec
    )

    assert len(result) == 2
    assert result[0]["reference"] == "REF001"
    assert result[1]["reference"] == "REF003"


def test_add_data_read_entities_file_error():
    """Test handling of file read errors"""
    mock_spec = MagicMock()

    with pytest.raises(Exception):
        _add_data_read_entities(
            "/nonexistent/file.csv", "test-dataset", "test-org", mock_spec
        )


def test_add_data_read_entities_with_whitespace(tmp_path):
    """Test handling references with whitespace"""
    resource_file = tmp_path / "test.csv"
    resource_file.write_text("reference,name\n  REF001  ,Test1\nREF002,Test2")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    result = _add_data_read_entities(
        str(resource_file), "test-dataset", "test-org", mock_spec
    )

    assert len(result) == 2
    assert result[0]["reference"] == "REF001" 



def test_check_existing_entities_all_new(tmp_path):
    """Test when all lookups are new"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""},
        {"prefix": "p1", "organisation": "org1", "reference": "REF002", "entity": ""},
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text("prefix,organisation,reference,entity\n")

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups, str(lookup_file)
    )

    assert len(new_lookups) == 2
    assert len(existing_lookups) == 0


def test_check_existing_entities_some_existing(tmp_path):
    """Test when some lookups already exist"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""},
        {"prefix": "p1", "organisation": "org1", "reference": "REF002", "entity": ""},
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text(
        "prefix,organisation,reference,entity\n" "p1,org1,REF001,1000001\n"
    )

    new_lookups, existing_lookups = _check_existing_entities(
        unidentified_lookups, str(lookup_file)
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
        unidentified_lookups, "/nonexistent/lookup.csv"
    )

    assert len(new_lookups) == 1
    assert len(existing_lookups) == 0



def test_assign_entity_numbers_creates_lookup_file(monkeypatch, tmp_path):
    """Test creating new lookup file for assigning entities"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    new_lookups = [
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1",
            "entity": "",
        }
    ]

    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(pipeline_dir / "lookup.csv")
    mock_lookups.get_max_entity.return_value = 1000000
    mock_lookups.save_csv.return_value = [
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "entity": "1000001",
            "resource": "res1",
        }
    ]
    mock_lookups.entity_num_gen = MagicMock()
    mock_lookups.entity_num_gen.state = {}

    mock_spec = MagicMock()
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups)

    result = _assign_entity_numbers(
        new_lookups, str(pipeline_dir), "test-dataset", mock_spec
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
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1",
        }
    ]

    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(lookup_file)
    mock_lookups.get_max_entity.return_value = 1000005
    mock_lookups.save_csv.return_value = [
        {
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "entity": "1000006",
            "resource": "res1",
        }
    ]
    mock_lookups.entity_num_gen = MagicMock()
    mock_lookups.entity_num_gen.state = {}

    mock_spec = MagicMock()
    mock_spec.get_dataset_entity_min.return_value = 1000000
    mock_spec.get_dataset_entity_max.return_value = 9999999

    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda x: mock_lookups)

    result = _assign_entity_numbers(
        new_lookups, str(pipeline_dir), "test-dataset", mock_spec
    )

    assert len(result) == 1
    assert mock_lookups.entity_num_gen.state["current"] == 1000005



def test_get_entities_breakdown_success():
    """Test converting entities to breakdown format"""
    new_entities = [
        {
            "entity": "1000001",
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF001",
            "resource": "res1",
        },
        {
            "entity": "1000002",
            "prefix": "p1",
            "organisation": "org1",
            "reference": "REF002",
            "resource": "res1",
        },
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
    new_entities = [{"entity": "1000001"}]

    result = _get_entities_breakdown(new_entities)

    assert len(result) == 1
    assert result[0]["entity"] == "1000001"
    assert result[0]["reference"] == ""
    assert result[0]["organisation"] == ""



def test_get_existing_entities_breakdown_success():
    """Test converting existing entities to simplified format"""
    existing_entities = [
        {"entity": "1000001", "reference": "REF001"},
        {"entity": "1000002", "reference": "REF002"},
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
        {"entity": "1000002", "reference": "REF002"},
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
        {"entity": "1000004", "reference": "REF004"},
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

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date
    ):
        return "endpoint_hash", {
            "endpoint": "endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.pipeline.append_endpoint", fake_append_endpoint
    )

    assert not endpoint_csv_path.exists()

    _validate_endpoint(url, str(pipeline_dir))

    assert endpoint_csv_path.exists()

    with open(endpoint_csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        assert headers == [
            "endpoint",
            "endpoint-url",
            "parameters",
            "plugin",
            "entry-date",
            "start-date",
            "end-date",
        ]


def test_validate_endpoint_empty_url(monkeypatch, tmp_path):
    """Test _validate_endpoint with empty URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    result = _validate_endpoint("", str(pipeline_dir))

    assert result == {}



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
        endpoint_summary,
    )

    assert result == {}



def test_fetch_add_data_response_all_lookups_exist(monkeypatch, tmp_path):
    """Test when all lookups already exist (line 364: continue path)"""
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
    cache_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("reference\nREF001")


    org_csv = cache_dir / "organisation.csv"
    org_csv.write_text("organisation,name\ntest-org,Test Organisation")

    lookup_file = pipeline_dir / "lookup.csv"
    lookup_file.write_text(
        "prefix,resource,organisation,reference,entity\n"
        "test-prefix,test,test-org,REF001,1000001\n"
    )

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )
    
    # Mock _add_data_pipeline to return a harmonised path
    harmonised_file = pipeline_dir / "harmonised" / "test.csv"
    harmonised_file.parent.mkdir(parents=True, exist_ok=True)
    harmonised_file.write_text("reference,entity\nREF001,1000001\n")
    monkeypatch.setattr(
        "src.application.core.pipeline._add_data_pipeline",
        lambda *args, **kwargs: str(harmonised_file),
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
    )

    assert "entity-summary" in result
    # All references already exist, so new should be 0
    assert result["entity-summary"]["new-in-resource"] == 0
    assert result["entity-summary"]["existing-in-resource"] == 1


def test_fetch_add_data_response_no_references_in_file(monkeypatch, tmp_path):
    """Test when file has no references (line 355: continue path)"""
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
    cache_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("reference\n\n\n")

    org_csv = cache_dir / "organisation.csv"
    org_csv.write_text("organisation,name\ntest-org,Test Organisation")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_endpoint",
        lambda url, dir: {"endpoint_url_in_endpoint_csv": True},
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._validate_source",
        lambda *a, **k: {"documentation_url_in_source_csv": True},
    )
    
    harmonised_file = pipeline_dir / "harmonised" / "test.csv"
    harmonised_file.parent.mkdir(parents=True, exist_ok=True)
    harmonised_file.write_text("reference,entity\n")
    monkeypatch.setattr(
        "src.application.core.pipeline._add_data_pipeline",
        lambda *args, **kwargs: str(harmonised_file),
    )

    result = fetch_add_data_response(
        collection=collection,
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        input_path=str(input_path),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 0
    assert result["entity-summary"]["existing-in-resource"] == 0


def test_fetch_add_data_response_unexpected_error(monkeypatch, tmp_path):
    """Test handling of unexpected errors (line 416-418: exception handling)"""
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
    test_file.write_text("reference\nREF001")

    mock_spec = MagicMock()
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )

    def raise_unexpected_error(path):
        raise RuntimeError("Unexpected filesystem error")

    monkeypatch.setattr("os.listdir", raise_unexpected_error)

    with pytest.raises(RuntimeError, match="Unexpected filesystem error"):
        fetch_add_data_response(
            collection=collection,
            dataset=dataset,
            organisation=organisation,
            pipeline_dir=str(pipeline_dir),
            input_path=str(input_path),
            specification_dir=str(specification_dir),
            cache_dir=str(cache_dir),
            url=url,
            documentation_url=documentation_url,
        )


def test_check_existing_entities_lookup_file_read_error(monkeypatch, tmp_path):
    """Test error handling when reading lookup file fails (line 476-477)"""
    unidentified_lookups = [
        {"prefix": "p1", "organisation": "org1", "reference": "REF001", "entity": ""}
    ]

    lookup_file = tmp_path / "lookup.csv"
    lookup_file.write_text("invalid,csv,content")


    with patch("csv.DictReader", side_effect=Exception("CSV read error")):
        new_lookups, existing_lookups = _check_existing_entities(
            unidentified_lookups, str(lookup_file)
        )


        assert len(new_lookups) == 1
        assert len(existing_lookups) == 0


def test_add_data_pipeline_returns_converted_when_harmonised_missing(monkeypatch, tmp_path):
    """Test _add_data_pipeline returns converted path when harmonised doesn't exist (line 770-774)"""
    resource_file_path = tmp_path / "test.csv"
    resource_file_path.write_text("reference\nREF001")
    
    resource_name = "test"
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    dataset = "test-dataset"
    
    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"
    mock_spec.get_field_datatype_map.return_value = {}
    mock_spec.get_field_typology_map.return_value = {}
    mock_spec.get_field_prefix_map.return_value = {}
    mock_spec.get_odp_collections.return_value = []
    mock_spec.factor_fieldnames.return_value = []
    mock_spec.intermediate_fieldnames.return_value = []
    mock_spec.current_fieldnames.return_value = []
    mock_spec.schema_field = {"test-schema": []}
    mock_spec.pipeline = {"test-dataset": {"schema": "test-schema"}}
    
    mock_pipeline = MagicMock()
    mock_pipeline.name = dataset
    mock_pipeline.path = str(pipeline_dir)
    mock_pipeline.skip_patterns.return_value = []
    mock_pipeline.concatenations.return_value = {}
    mock_pipeline.columns.return_value = {}
    mock_pipeline.patches.return_value = {}
    mock_pipeline.default_fields.return_value = {}
    mock_pipeline.default_values.return_value = {}
    mock_pipeline.combine_fields.return_value = {}
    mock_pipeline.lookups.return_value = {}
    mock_pipeline.filters.return_value = {}
    mock_pipeline.migrations.return_value = []
    
    org_path = tmp_path / "organisation.csv"
    org_path.write_text("organisation,name\ntest-org,Test Organisation")

    converted_dir = pipeline_dir / "converted"
    converted_dir.mkdir(parents=True)
    converted_file = converted_dir / "test.csv"
    converted_file.write_text("reference,entity\nREF001,")
    
    harmonised_dir = tmp_path / "harmonised"
    harmonised_dir.mkdir(exist_ok=True)
    
    def mock_run_pipeline_no_harmonised(*args):
        pass
    
    original_exists = os.path.exists
    def mock_exists(path):
        if "harmonised" in str(path) and str(path).endswith(".csv"):
            return False
        return original_exists(path)
    
    monkeypatch.setattr("src.application.core.pipeline.run_pipeline", mock_run_pipeline_no_harmonised)
    monkeypatch.setattr("os.path.exists", mock_exists)

    result = _add_data_pipeline(
        resource_file_path=str(resource_file_path),
        resource_name=resource_name,
        pipeline_dir=str(pipeline_dir),
        specification=mock_spec,
        dataset=dataset,
        pipeline=mock_pipeline,
        organisation_path=str(org_path),
    )

    assert "converted" in result
    assert result == str(converted_file)