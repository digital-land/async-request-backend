import pytest
import os
import csv
from unittest.mock import MagicMock
from src.application.core.pipeline import (
    fetch_add_data_response,
    _get_entities_breakdown,
    _get_existing_entities_breakdown,
)


def test_fetch_add_data_response_success(monkeypatch, tmp_path):
    """Test successful execution of fetch_add_data_response"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

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

    result = fetch_add_data_response(
        dataset=dataset,
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        endpoint=endpoint,
    )

    assert "new-in-resource" in result
    assert "existing-in-resource" in result


def test_fetch_add_data_response_no_files(monkeypatch, tmp_path):
    """Test when input directory has no files"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    result = fetch_add_data_response(
        dataset=dataset,
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        endpoint=endpoint,
    )

    assert "new-in-resource" in result
    assert result["new-in-resource"] == 0


def test_fetch_add_data_response_file_not_found(monkeypatch, tmp_path):
    """Test when input path does not exist"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "nonexistent"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr(
        "src.application.core.pipeline.Specification", lambda x: mock_spec
    )
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    with pytest.raises(FileNotFoundError):
        fetch_add_data_response(
            dataset=dataset,
            organisation_provider=organisation,
            pipeline_dir=str(pipeline_dir),
            input_dir=str(input_path),
            output_path=str(input_path / "output.csv"),
            specification_dir=str(specification_dir),
            cache_dir=str(cache_dir),
            endpoint=endpoint,
        )


def test_fetch_add_data_response_handles_processing_error(monkeypatch, tmp_path):
    """Test handling of errors during file processing"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

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

    result = fetch_add_data_response(
        dataset=dataset,
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
        specification_dir=str(specification_dir),
        cache_dir=str(cache_dir),
        endpoint=endpoint,
    )

    assert "new-in-resource" in result
    assert result["new-in-resource"] == 0


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
