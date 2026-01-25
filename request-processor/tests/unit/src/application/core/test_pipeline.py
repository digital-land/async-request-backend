import pytest
import os
import csv
from unittest.mock import MagicMock
from src.application.core.pipeline import (
    fetch_add_data_response,
    _get_entities_breakdown,
    _get_existing_entities_breakdown,
    _validate_endpoint,
    _validate_source,
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
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())
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
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
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
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())
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
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
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
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    with pytest.raises(FileNotFoundError):
        fetch_add_data_response(
            collection=collection,
            dataset=dataset,
            organisation_provider=organisation,
            pipeline_dir=str(pipeline_dir),
            input_dir=str(input_path),
            output_path=str(input_path / "output.csv"),
            specification_dir=str(specification_dir),
            cache_dir=str(cache_dir),
            url=url,
            documentation_url=documentation_url,
        )


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
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    def raise_exception(*args, **kwargs):
        raise Exception("Processing error")

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
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        url=url,
        documentation_url=documentation_url,
    )

    assert "entity-summary" in result
    assert result["entity-summary"]["new-in-resource"] == 0


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
