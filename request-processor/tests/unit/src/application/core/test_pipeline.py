import json
import pytest
from datetime import date
from unittest.mock import MagicMock, call
from src.application.core.pipeline import (
    fetch_response_data,
    fetch_add_data_response,
    _assign_entries,
    _create_auto_old_entity_redirects,
    _create_old_entity_redirects,
    _filter_selected_entities,
    _get_entities_breakdown,
    _get_existing_entities_breakdown,
    _create_entity_organisation,
    _format_task_summary,
    _find_duplicate_candidates,
    run_task_pipeline,
)


def test_fetch_response_data_calls_assign_entries_with_expected_params(
    monkeypatch, tmp_path
):
    dataset = "test-dataset"
    organisation = "test-org"
    request_id = "request-123"
    collection_dir = tmp_path / "collection"
    converted_dir = tmp_path / "converted"
    issue_dir = tmp_path / "issue"
    column_field_dir = tmp_path / "column-field"
    transformed_dir = tmp_path / "transformed"
    dataset_resource_dir = tmp_path / "dataset-resource"
    pipeline_dir = tmp_path / "pipeline"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"

    input_dir = collection_dir / "resource" / request_id
    input_dir.mkdir(parents=True)
    resource_file = input_dir / "resource-hash.csv"
    resource_file.write_text("reference\nREF001\n")

    converted_dir.mkdir()
    pipeline_dir.mkdir()
    specification_dir.mkdir()
    cache_dir.mkdir()

    mock_specification = MagicMock()
    mock_issue_log = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.path = str(pipeline_dir)
    mock_pipeline.transform.return_value = mock_issue_log
    mock_api = MagicMock()
    mock_api.get_valid_category_values.return_value = {"category": ["value"]}
    mock_organisation = MagicMock()
    assign_entries = MagicMock()

    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline",
        MagicMock(return_value=mock_pipeline),
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.API",
        MagicMock(return_value=mock_api),
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.Organisation",
        MagicMock(return_value=mock_organisation),
    )
    monkeypatch.setattr("src.application.core.pipeline._assign_entries", assign_entries)

    fetch_response_data(
        dataset=dataset,
        organisation=organisation,
        request_id=request_id,
        collection_dir=str(collection_dir),
        converted_dir=str(converted_dir),
        issue_dir=str(issue_dir),
        column_field_dir=str(column_field_dir),
        transformed_dir=str(transformed_dir),
        dataset_resource_dir=str(dataset_resource_dir),
        pipeline_dir=str(pipeline_dir),
        specification=mock_specification,
        cache_dir=str(cache_dir),
        additional_col_mappings=None,
        additional_concats=None,
    )

    assign_entries.assert_called_once_with(
        resource_path=str(resource_file),
        dataset=dataset,
        organisation=organisation,
        pipeline_dir=str(pipeline_dir),
        specification=mock_specification,
        cache_dir=str(cache_dir),
        endpoints=[],
    )


def test_fetch_response_data_reraises_assign_entries_exception(monkeypatch, tmp_path):
    dataset = "test-dataset"
    organisation = "test-org"
    request_id = "request-123"
    collection_dir = tmp_path / "collection"
    converted_dir = tmp_path / "converted"
    issue_dir = tmp_path / "issue"
    column_field_dir = tmp_path / "column-field"
    transformed_dir = tmp_path / "transformed"
    dataset_resource_dir = tmp_path / "dataset-resource"
    pipeline_dir = tmp_path / "pipeline"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"

    input_dir = collection_dir / "resource" / request_id
    input_dir.mkdir(parents=True)
    resource_file = input_dir / "resource-hash.csv"
    resource_file.write_text("reference\nREF001\n")

    converted_dir.mkdir()
    pipeline_dir.mkdir()
    specification_dir.mkdir()
    cache_dir.mkdir()

    mock_specification = MagicMock()
    mock_pipeline = MagicMock()
    mock_api = MagicMock()
    assign_entries = MagicMock(side_effect=RuntimeError("assign failed"))

    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline",
        MagicMock(return_value=mock_pipeline),
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.API",
        MagicMock(return_value=mock_api),
    )
    monkeypatch.setattr("src.application.core.pipeline._assign_entries", assign_entries)

    with pytest.raises(RuntimeError, match="assign failed"):
        fetch_response_data(
            dataset=dataset,
            organisation=organisation,
            request_id=request_id,
            collection_dir=str(collection_dir),
            converted_dir=str(converted_dir),
            issue_dir=str(issue_dir),
            column_field_dir=str(column_field_dir),
            transformed_dir=str(transformed_dir),
            dataset_resource_dir=str(dataset_resource_dir),
            pipeline_dir=str(pipeline_dir),
            specification=mock_specification,
            cache_dir=str(cache_dir),
            additional_col_mappings=None,
            additional_concats=None,
        )


def test_fetch_response_data_reraises_pipeline_transform_exception(
    monkeypatch, tmp_path
):
    dataset = "test-dataset"
    organisation = "test-org"
    request_id = "request-123"
    collection_dir = tmp_path / "collection"
    converted_dir = tmp_path / "converted"
    issue_dir = tmp_path / "issue"
    column_field_dir = tmp_path / "column-field"
    transformed_dir = tmp_path / "transformed"
    dataset_resource_dir = tmp_path / "dataset-resource"
    pipeline_dir = tmp_path / "pipeline"
    specification_dir = tmp_path / "specification"
    cache_dir = tmp_path / "cache"

    input_dir = collection_dir / "resource" / request_id
    input_dir.mkdir(parents=True)
    resource_file = input_dir / "resource-hash.csv"
    resource_file.write_text("reference\nREF001\n")

    converted_dir.mkdir()
    pipeline_dir.mkdir()
    specification_dir.mkdir()
    cache_dir.mkdir()

    mock_specification = MagicMock()
    mock_pipeline = MagicMock()
    mock_pipeline.path = str(pipeline_dir)
    mock_pipeline.transform.side_effect = RuntimeError("transform failed")
    mock_api = MagicMock()
    mock_api.get_valid_category_values.return_value = {"category": ["value"]}
    mock_organisation = MagicMock()
    assign_entries = MagicMock()

    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline",
        MagicMock(return_value=mock_pipeline),
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.API",
        MagicMock(return_value=mock_api),
    )
    monkeypatch.setattr(
        "src.application.core.pipeline.Organisation",
        MagicMock(return_value=mock_organisation),
    )
    monkeypatch.setattr("src.application.core.pipeline._assign_entries", assign_entries)

    with pytest.raises(RuntimeError, match="transform failed"):
        fetch_response_data(
            dataset=dataset,
            organisation=organisation,
            request_id=request_id,
            collection_dir=str(collection_dir),
            converted_dir=str(converted_dir),
            issue_dir=str(issue_dir),
            column_field_dir=str(column_field_dir),
            transformed_dir=str(transformed_dir),
            dataset_resource_dir=str(dataset_resource_dir),
            pipeline_dir=str(pipeline_dir),
            specification=mock_specification,
            cache_dir=str(cache_dir),
            additional_col_mappings=None,
            additional_concats=None,
        )


def test_fetch_add_data_response_success(monkeypatch, tmp_path):
    """Test successful execution of fetch_add_data_response"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
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
        specification=mock_spec,
        cache_dir=str(cache_dir),
        endpoint=endpoint,
    )

    assert "new-in-resource" in result
    assert "existing-in-resource" in result


def test_assign_entries_prioritises_rows_not_in_excluded_references(
    monkeypatch, tmp_path
):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    resource_path = tmp_path / "resource.csv"
    resource_path.write_text("reference\nREF001\nREF002\n")
    discovered_lookups = [
        [
            {
                "prefix": "test-prefix",
                "resource": "resource",
                "organisation": "test-org",
                "reference": "REF001",
                "entity": "",
            }
        ],
        [
            {
                "prefix": "test-prefix",
                "resource": "resource",
                "organisation": "test-org",
                "reference": "REF002",
                "entity": "",
            }
        ],
    ]
    assigned_lookups = [
        {**discovered_lookups[1][0], "entity": "1000001"},
        {**discovered_lookups[0][0], "entity": "1000002"},
    ]

    mock_pipeline = MagicMock()
    mock_pipeline.name = "test-dataset"
    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(pipeline_dir / "lookup.csv")
    mock_lookups.get_max_entity.return_value = 1000000
    mock_lookups.save_csv.return_value = assigned_lookups
    mock_lookups.entity_num_gen.state = {}
    mock_specification = MagicMock()
    mock_specification.get_dataset_entity_min.return_value = 1000000
    mock_specification.get_dataset_entity_max.return_value = 1999999

    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline", MagicMock(return_value=mock_pipeline)
    )
    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda _: mock_lookups)
    monkeypatch.setattr(
        "src.application.core.pipeline.get_resource_unidentified_lookups",
        MagicMock(return_value=discovered_lookups),
    )

    result = _assign_entries(
        resource_path=str(resource_path),
        dataset="test-dataset",
        organisation="test-org",
        pipeline_dir=str(pipeline_dir),
        specification=mock_specification,
        cache_dir=str(tmp_path),
        excluded_references=["REF001"],
    )

    assert mock_lookups.add_entry.call_args_list == [
        call(discovered_lookups[1][0]),
        call(discovered_lookups[0][0]),
    ]
    assert result == assigned_lookups


@pytest.mark.parametrize("excluded_references", [None, []])
def test_assign_entries_keeps_original_order_when_excluded_references_empty_or_null(
    monkeypatch, tmp_path, excluded_references
):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    resource_path = tmp_path / "resource.csv"
    resource_path.write_text("reference\nREF001\nREF002\n")
    discovered_lookups = [
        [
            {
                "prefix": "test-prefix",
                "resource": "resource",
                "organisation": "test-org",
                "reference": "REF001",
                "entity": "",
            }
        ],
        [
            {
                "prefix": "test-prefix",
                "resource": "resource",
                "organisation": "test-org",
                "reference": "REF002",
                "entity": "",
            }
        ],
    ]
    assigned_lookups = [
        {**discovered_lookups[0][0], "entity": "1000001"},
        {**discovered_lookups[1][0], "entity": "1000002"},
    ]

    mock_pipeline = MagicMock()
    mock_pipeline.name = "test-dataset"
    mock_lookups = MagicMock()
    mock_lookups.lookups_path = str(pipeline_dir / "lookup.csv")
    mock_lookups.get_max_entity.return_value = 1000000
    mock_lookups.save_csv.return_value = assigned_lookups
    mock_lookups.entity_num_gen.state = {}
    mock_specification = MagicMock()
    mock_specification.get_dataset_entity_min.return_value = 1000000
    mock_specification.get_dataset_entity_max.return_value = 1999999

    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline", MagicMock(return_value=mock_pipeline)
    )
    monkeypatch.setattr("src.application.core.pipeline.Lookups", lambda _: mock_lookups)
    monkeypatch.setattr(
        "src.application.core.pipeline.get_resource_unidentified_lookups",
        MagicMock(return_value=discovered_lookups),
    )

    result = _assign_entries(
        resource_path=str(resource_path),
        dataset="test-dataset",
        organisation="test-org",
        pipeline_dir=str(pipeline_dir),
        specification=mock_specification,
        cache_dir=str(tmp_path),
        excluded_references=excluded_references,
    )

    assert mock_lookups.add_entry.call_count == 2
    assert result == assigned_lookups


def test_fetch_add_data_response_no_files(monkeypatch, tmp_path):
    """Test when input directory has no files"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    result = fetch_add_data_response(
        dataset=dataset,
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
        specification=mock_spec,
        cache_dir=str(cache_dir),
        endpoint=endpoint,
    )

    assert "new-in-resource" in result
    assert result["new-in-resource"] == 0


def test_fetch_add_data_response_includes_selected_old_entity_redirects(
    monkeypatch, tmp_path
):
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)
    (input_path / "test.csv").write_text("reference\nREF001\nREF002")

    mock_pipeline = MagicMock()
    mock_pipeline.path = str(pipeline_dir)
    mock_pipeline.redirect_lookups.return_value = {}
    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline", MagicMock(return_value=mock_pipeline)
    )
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())
    mock_api = MagicMock()
    mock_api.get_valid_category_values.return_value = {}
    monkeypatch.setattr(
        "src.application.core.pipeline.API", MagicMock(return_value=mock_api)
    )
    monkeypatch.setattr(
        "src.application.core.pipeline._find_duplicate_candidates",
        MagicMock(
            return_value=[
                {
                    "old_entity": "900002",
                    "entity": "1000002",
                    "match_type": "complete_match",
                    "new_reference": "REF002",
                    "name_similarity": "",
                    "evidence": "geometry complete_match, name similarity 100%",
                    "old_entity_redirects": [],
                },
                {
                    "old_entity": "900001",
                    "entity": "1000002",
                    "match_type": "single_match",
                    "new_reference": "REF002",
                    "name_similarity": "85%",
                    "evidence": "geometry single_match, name similarity 85%",
                    "old_entity_redirects": [],
                },
            ]
        ),
    )
    issue_log = MagicMock()
    issue_log.rows = []
    monkeypatch.setattr(
        "src.application.core.pipeline._process_add_data_resource",
        MagicMock(
            return_value=(
                mock_pipeline,
                issue_log,
                [],
                [
                    {
                        "entity": "1000001",
                        "reference": "REF001",
                        "organisation": "test-org",
                    },
                    {
                        "entity": "1000002",
                        "reference": "REF002",
                        "organisation": "test-org",
                    },
                ],
                [],
            )
        ),
    )

    result = fetch_add_data_response(
        dataset=dataset,
        organisation_provider=organisation,
        pipeline_dir=str(pipeline_dir),
        input_dir=str(input_path),
        output_path=str(input_path / "output.csv"),
        specification=MagicMock(),
        cache_dir=str(cache_dir),
        endpoint=endpoint,
        excluded_references=["REF001"],
        selected_redirects=[
            {"reference": "REF002", "old_entity_number": "900001"},
        ],
    )

    assert result["old-entity"] == [
        {
            "old-entity": "900002",
            "status": "301",
            "entity": "1000002",
            "entry-date": date.today().isoformat(),
            "notes": "test-org geometry complete_match, name similarity 100%",
        },
        {
            "old-entity": "900001",
            "status": "301",
            "entity": "1000002",
            "entry-date": date.today().isoformat(),
            "notes": "test-org geometry single_match, name similarity 85%",
        },
    ]
    assert result["new-entities"] == [
        {
            "entity": "1000002",
            "prefix": "",
            "end-date": "",
            "endpoint": "",
            "resource": "",
            "reference": "REF002",
            "entry-date": "",
            "start-date": "",
            "entry-number": "",
            "organisation": "test-org",
        }
    ]
    assert "all-entities" not in result


def test_fetch_add_data_response_file_not_found(monkeypatch, tmp_path):
    """Test when input path does not exist"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "nonexistent"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    pipeline_dir.mkdir(parents=True)

    mock_spec = MagicMock()
    monkeypatch.setattr("src.application.core.pipeline.Pipeline", MagicMock())
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    with pytest.raises(FileNotFoundError):
        fetch_add_data_response(
            dataset=dataset,
            organisation_provider=organisation,
            pipeline_dir=str(pipeline_dir),
            input_dir=str(input_path),
            output_path=str(input_path / "output.csv"),
            specification=mock_spec,
            cache_dir=str(cache_dir),
            endpoint=endpoint,
        )


def test_fetch_add_data_response_handles_processing_error(monkeypatch, tmp_path):
    """Test handling of errors during file processing"""
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("invalid csv content without proper headers")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"

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
        specification=mock_spec,
        cache_dir=str(cache_dir),
        endpoint=endpoint,
    )

    assert "new-in-resource" in result
    assert result["new-in-resource"] == 0


def test_fetch_add_data_response_reraises_processing_error(monkeypatch, tmp_path):
    dataset = "test-dataset"
    organisation = "test-org"
    pipeline_dir = tmp_path / "pipeline"
    input_path = tmp_path / "resource"
    cache_dir = tmp_path / "cache"
    endpoint = "abc123hash"

    input_path.mkdir(parents=True)
    pipeline_dir.mkdir(parents=True)

    test_file = input_path / "test.csv"
    test_file.write_text("invalid csv content without proper headers")

    mock_spec = MagicMock()
    mock_spec.dataset_prefix.return_value = "test-prefix"
    mock_pipeline = MagicMock()
    mock_pipeline.transform.side_effect = Exception("Processing error")

    monkeypatch.setattr(
        "src.application.core.pipeline.Pipeline", MagicMock(return_value=mock_pipeline)
    )
    monkeypatch.setattr("src.application.core.pipeline.Organisation", MagicMock())

    with pytest.raises(Exception, match="Processing error"):
        fetch_add_data_response(
            dataset=dataset,
            organisation_provider=organisation,
            pipeline_dir=str(pipeline_dir),
            input_dir=str(input_path),
            output_path=str(input_path / "output.csv"),
            specification=mock_spec,
            cache_dir=str(cache_dir),
            endpoint=endpoint,
        )


def test_find_duplicate_candidates_passes_redirect_lookups(monkeypatch, tmp_path):
    calls = {}

    def fake_find_duplicate_redirect_candidates(**kwargs):
        calls.update(kwargs)
        return [{"old_entity": "100"}]

    monkeypatch.setattr(
        "src.application.core.pipeline.find_duplicate_redirect_candidates",
        fake_find_duplicate_redirect_candidates,
    )

    result = _find_duplicate_candidates(
        dataset="conservation-area",
        specification=MagicMock(),
        output_path=str(tmp_path / "transformed.csv"),
        redirect_lookups={"100": {"entity": "300", "status": "301"}},
        organisation_provider="local-authority:STH",
        organisation_index=MagicMock(),
    )

    assert result == [{"old_entity": "100"}]
    assert calls["redirect_lookups"] == {"100": {"entity": "300", "status": "301"}}


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


# --- _create_entity_organisation ---


def test_filter_selected_entities_excludes_requested_references():
    new_entities = [
        {"entity": "1000001", "reference": "REF001", "organisation": "test-org"},
        {"entity": "1000002", "reference": "REF002", "organisation": "test-org"},
    ]

    result = _filter_selected_entities(
        new_entities,
        ["REF001"],
    )

    assert result == [new_entities[1]]


@pytest.mark.parametrize("excluded_references", [None, []])
def test_filter_selected_entities_returns_all_when_excluded_references_empty_or_null(
    excluded_references,
):
    new_entities = [
        {"entity": "1000001", "reference": "REF001", "organisation": "test-org"},
        {"entity": "1000002", "reference": "REF002", "organisation": "test-org"},
    ]

    assert _filter_selected_entities(new_entities, excluded_references) == new_entities


def test_filter_selected_entities_returns_all_for_non_matching_excluded_reference():
    new_entities = [
        {"entity": "1000001", "reference": "REF001", "organisation": "test-org"},
    ]

    assert _filter_selected_entities(new_entities, ["REF999"]) == new_entities


def test_create_entity_organisation_uses_selected_entity_subset(tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    (pipeline_dir / "entity-organisation.csv").write_text(
        "dataset,entity-minimum,entity-maximum,organisation\n"
    )
    new_entities = [
        {"entity": "1000001", "reference": "REF001", "organisation": "test-org"},
        {"entity": "1000002", "reference": "REF002", "organisation": "test-org"},
    ]

    result = _create_entity_organisation(
        _filter_selected_entities(
            new_entities,
            ["REF001"],
        ),
        "test-dataset",
        "test-org",
        str(pipeline_dir),
    )

    assert result[0]["entity-minimum"] == 1000002
    assert result[0]["entity-maximum"] == 1000002


def test_create_old_entity_redirects_from_selected_redirects():
    result = _create_old_entity_redirects(
        [{"reference": "REF002", "old_entity_number": "900001"}],
        duplicate_candidates=[
            {
                "old_entity": "900001",
                "entity": "1000002",
                "new_reference": "REF002",
                "evidence": "geometry single_match, name similarity 85%",
            }
        ],
        organisation="local-authority:MAI",
    )

    assert result == [
        {
            "old-entity": "900001",
            "status": "301",
            "entity": "1000002",
            "entry-date": date.today().isoformat(),
            "notes": "local-authority:MAI geometry single_match, name similarity 85%",
        }
    ]


def test_create_old_entity_redirects_applies_selected_status():
    result = _create_old_entity_redirects(
        [
            {
                "old_entity_number": "900001",
                "status": "410",
            }
        ],
        duplicate_candidates=[
            {
                "old_entity": "900001",
                "entity": "1000002",
                "new_reference": "REF002",
                "match_type": "all_fields_match",
                "evidence": "all comparable fields match",
            }
        ],
        organisation="local-authority:MAI",
    )

    assert result[0]["status"] == "410"
    assert result[0]["entity"] == ""
    assert result[0]["notes"] == (
        "local-authority:MAI retirement selected in Assign Entities"
    )


def test_create_old_entity_redirects_allows_existing_candidate_target():
    result = _create_old_entity_redirects(
        [
            {
                "old_entity_number": "7001056210",
                "status": "410",
            }
        ],
        duplicate_candidates=[
            {
                "old_entity": "7001056210",
                "entity": "7001067890",
                "new_reference": "TPO 20/90b",
                "evidence": "all comparable fields match",
            }
        ],
        organisation="local-authority:CAS",
    )

    assert result == [
        {
            "old-entity": "7001056210",
            "status": "410",
            "entity": "",
            "entry-date": date.today().isoformat(),
            "notes": "local-authority:CAS retirement selected in Assign Entities",
        }
    ]


def test_create_old_entity_redirects_resolves_current_target_from_selection():
    result = _create_old_entity_redirects(
        [
            {
                "reference": "TPO 20/90b",
                "old_entity_number": "7001056210",
                "status": "301",
            }
        ],
        duplicate_candidates=[
            {
                "old_entity": "7001056210",
                "entity": "7001067890",
                "new_reference": "TPO 20/90b",
            }
        ],
    )

    assert result == [
        {
            "old-entity": "7001056210",
            "status": "301",
            "entity": "7001067890",
            "entry-date": date.today().isoformat(),
            "notes": "",
        }
    ]


def test_create_old_entity_redirects_rejects_changed_target_when_candidates_ambiguous():
    result = _create_old_entity_redirects(
        [
            {
                "reference": "TPO 20/90b",
                "old_entity_number": "7001056210",
                "status": "301",
            }
        ],
        duplicate_candidates=[
            {
                "old_entity": "7001056210",
                "entity": "7001067890",
                "new_reference": "TPO 20/90b",
            },
            {
                "old_entity": "7001056210",
                "entity": "7001067891",
                "new_reference": "TPO 20/90b",
            },
        ],
    )

    assert result == []


def test_create_old_entity_retirement_rejects_unmatched_old_entity():
    result = _create_old_entity_redirects(
        [{"old_entity_number": "9999999999", "status": "410"}],
        duplicate_candidates=[
            {
                "old_entity": "7001056210",
                "entity": "7001067890",
                "new_reference": "TPO 20/90b",
            }
        ],
    )

    assert result == []


def test_create_old_entity_redirects_defaults_invalid_status_to_301():
    result = _create_old_entity_redirects(
        [
            {
                "reference": "REF002",
                "old_entity_number": "900001",
                "status": "302",
            }
        ],
        duplicate_candidates=[
            {
                "old_entity": "900001",
                "entity": "1000002",
                "new_reference": "REF002",
            }
        ],
    )

    assert result[0]["status"] == "301"


def test_create_old_entity_redirects_ignores_redirects_for_excluded_references():
    result = _create_old_entity_redirects(
        [{"reference": "REF001", "old_entity_number": "900001"}],
        excluded_references=["REF001"],
        duplicate_candidates=[
            {
                "old_entity": "900001",
                "entity": "1000001",
                "new_reference": "REF001",
            }
        ],
    )

    assert result == []


def test_create_auto_old_entity_redirects_for_complete_matches():
    result = _create_auto_old_entity_redirects(
        [
            {
                "old_entity": "900001",
                "entity": "1000001",
                "match_type": "complete_match",
                "name_similarity": "",
                "evidence": "geometry complete_match",
                "old_entity_redirects": [],
            }
        ],
        organisation="local-authority:MAI",
    )

    assert result == [
        {
            "old-entity": "900001",
            "status": "301",
            "entity": "1000001",
            "entry-date": date.today().isoformat(),
            "notes": "local-authority:MAI geometry complete_match",
        }
    ]


def test_create_auto_old_entity_redirects_requires_review_for_all_fields_match():
    result = _create_auto_old_entity_redirects(
        [
            {
                "old_entity": "900001",
                "entity": "1000001",
                "match_type": "all_fields_match",
                "old_entity_redirects": [],
            }
        ]
    )

    assert result == []


def test_create_auto_old_entity_redirects_for_single_matches_above_85_percent():
    result = _create_auto_old_entity_redirects(
        [
            {
                "old_entity": "900001",
                "entity": "1000001",
                "match_type": "single_match",
                "name_similarity": "86%",
                "evidence": "geometry single_match, name similarity 86%",
                "old_entity_redirects": [],
            },
            {
                "old_entity": "900002",
                "entity": "1000002",
                "match_type": "single_match",
                "name_similarity": "85%",
                "evidence": "geometry single_match, name similarity 85%",
                "old_entity_redirects": [],
            },
        ],
    )

    assert result == [
        {
            "old-entity": "900001",
            "status": "301",
            "entity": "1000001",
            "entry-date": date.today().isoformat(),
            "notes": "geometry single_match, name similarity 86%",
        }
    ]


def test_create_auto_old_entity_redirects_ignores_existing_redirects_and_unselected_ids():
    result = _create_auto_old_entity_redirects(
        [
            {
                "old_entity": "900001",
                "entity": "1000001",
                "match_type": "complete_match",
                "name_similarity": "",
                "old_entity_redirects": [],
            },
            {
                "old_entity": "900002",
                "entity": "1000002",
                "match_type": "complete_match",
                "name_similarity": "",
                "old_entity_redirects": [{"old-entity": "900002"}],
            },
        ],
        selected_entity_ids={"1000002"},
    )

    assert result == []


@pytest.mark.parametrize("selected_redirects", [None, []])
def test_create_old_entity_redirects_returns_empty_when_selection_empty_or_null(
    selected_redirects,
):
    assert _create_old_entity_redirects(selected_redirects, []) == []


def test_create_old_entity_redirects_ignores_unassigned_or_invalid_redirects():
    result = _create_old_entity_redirects(
        [
            {"reference": "REF001"},
            {"old_entity_number": "900001"},
        ],
        duplicate_candidates=[],
    )

    assert result == []


def test_create_entity_organisation_sets_overlap_true_when_range_already_present(
    tmp_path,
):
    """New entities that already fall within an existing range should flag overlap"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    (pipeline_dir / "entity-organisation.csv").write_text(
        "dataset,entity-minimum,entity-maximum,organisation\n"
        "nature-improvement-area,10100000,10100011,government-organisation:PB202\n"
    )

    new_entities = [{"entity": "10100002"}, {"entity": "10100005"}]

    result = _create_entity_organisation(
        new_entities,
        "nature-improvement-area",
        "government-organisation:PB202",
        str(pipeline_dir),
    )

    assert len(result) == 1
    assert result[0]["overlap"] is True
    assert result[0]["error"] is False
    assert "entity-minimum" not in result[0]
    assert "entity-maximum" not in result[0]


def test_create_entity_organisation_no_overlap_when_range_not_present(tmp_path):
    """New entities outside all existing ranges should not flag overlap"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    (pipeline_dir / "entity-organisation.csv").write_text(
        "dataset,entity-minimum,entity-maximum,organisation\n"
        "nature-improvement-area,10100000,10100011,government-organisation:PB202\n"
    )

    new_entities = [{"entity": "10200000"}, {"entity": "10200001"}]

    result = _create_entity_organisation(
        new_entities,
        "nature-improvement-area",
        "government-organisation:PB202",
        str(pipeline_dir),
    )

    assert len(result) == 1
    assert result[0]["overlap"] is False
    assert result[0]["error"] is False
    assert result[0]["entity-minimum"] == 10200000
    assert result[0]["entity-maximum"] == 10200001


def test_create_entity_organisation_missing_csv_sets_error_true(tmp_path):
    """Missing entity-organisation.csv should set error but still return a mapping"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    new_entities = [{"entity": "10100002"}]

    result = _create_entity_organisation(
        new_entities,
        "nature-improvement-area",
        "government-organisation:PB202",
        str(pipeline_dir),
    )

    assert len(result) == 1
    assert result[0]["error"] is True
    assert result[0]["overlap"] is False
    assert "entity-minimum" not in result[0]
    assert "entity-maximum" not in result[0]


def test_create_entity_organisation_empty_entities_returns_empty_list(tmp_path):
    result = _create_entity_organisation(
        [], "nature-improvement-area", "government-organisation:PB202", str(tmp_path)
    )
    assert result == []


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


# --- _format_task_summary ---


def test_format_task_summary_issue_singular():
    mappings = {
        ("geometry", "invalid WKT"): {
            "summary-singular": "{count} geometry value is invalid",
            "summary-plural": "{count} geometry values are invalid",
        }
    }
    details = json.dumps({"issue_type": "invalid WKT", "field": "geometry", "count": 1})
    result = _format_task_summary(details, "issue", mappings)
    assert result == "1 geometry value is invalid"


def test_format_task_summary_issue_plural():
    mappings = {
        ("geometry", "invalid WKT"): {
            "summary-singular": "{count} geometry value is invalid",
            "summary-plural": "{count} geometry values are invalid",
        }
    }
    details = json.dumps({"issue_type": "invalid WKT", "field": "geometry", "count": 3})
    result = _format_task_summary(details, "issue", mappings)
    assert result == "3 geometry values are invalid"


def test_format_task_summary_column_field_source():
    result = _format_task_summary(
        json.dumps({"field": "geometry", "issue_type": "missing-field"}),
        "column-field",
        {},
    )
    assert result == "Geometry column missing"


def test_format_task_summary_unknown_mapping_returns_empty():
    details = json.dumps(
        {"issue_type": "unknown-type", "field": "some-field", "count": 1}
    )
    result = _format_task_summary(details, "issue", {})
    assert result == ""


def test_format_task_summary_malformed_json_returns_empty():
    result = _format_task_summary("not valid json", "issue", {})
    assert result == ""


def test_format_task_summary_none_returns_empty():
    result = _format_task_summary(None, "issue", {})
    assert result == ""


# --- run_task_pipeline ---


def test_run_task_pipeline_returns_task_log_with_summary(tmp_path, monkeypatch):
    issue_csv = tmp_path / "issues.csv"
    issue_csv.write_text(
        "issue-type,field,resource,dataset,severity,responsibility\n"
        "invalid WKT,geometry,res1,conservation-area,error,external\n"
    )
    task_log_path = str(tmp_path / "tasks.csv")

    mappings = {
        ("geometry", "invalid WKT"): {
            "summary-singular": "1 geometry value is invalid",
            "summary-plural": "{count} geometry values are invalid",
        }
    }
    monkeypatch.setattr("src.application.core.pipeline.load_mappings", lambda: mappings)

    result = run_task_pipeline(
        task_log_path=task_log_path,
        dataset="conservation-area",
        organisation="local-authority:CTY",
        issue_path=str(issue_csv),
    )

    assert isinstance(result, list)
    assert len(result) > 0
    assert "summary" in result[0]


def test_run_task_pipeline_raises_on_failed_status(tmp_path, monkeypatch):
    from digital_land.pipeline.task import TaskPipelineStatus

    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = TaskPipelineStatus.FAILED
    monkeypatch.setattr(
        "src.application.core.pipeline.TaskPipeline", lambda: mock_pipeline
    )

    with pytest.raises(RuntimeError, match="TaskPipeline failed"):
        run_task_pipeline(
            task_log_path=str(tmp_path / "tasks.csv"),
            dataset="conservation-area",
            organisation="local-authority:CTY",
            issue_path=str(tmp_path / "nonexistent.csv"),
        )


def test_run_task_pipeline_passes_column_field_path_and_mandatory_fields(
    tmp_path, monkeypatch
):
    from digital_land.pipeline.task import TaskPipelineStatus

    mock_pipeline = MagicMock()
    mock_pipeline.run.return_value = TaskPipelineStatus.COMPLETE
    monkeypatch.setattr(
        "src.application.core.pipeline.TaskPipeline", lambda: mock_pipeline
    )
    monkeypatch.setattr("src.application.core.pipeline.load_mappings", lambda: {})

    task_log_path = str(tmp_path / "tasks.csv")
    issue_path = str(tmp_path / "issues.csv")
    column_field_path = str(tmp_path / "column-field.csv")
    mandatory_fields = ["reference", "name"]

    result = run_task_pipeline(
        task_log_path=task_log_path,
        dataset="conservation-area",
        organisation="local-authority:CTY",
        issue_path=issue_path,
        column_field_path=column_field_path,
        mandatory_fields=mandatory_fields,
    )

    mock_pipeline.run.assert_called_once_with(
        output_path=task_log_path,
        dataset="conservation-area",
        organisation="local-authority:CTY",
        issue_path=issue_path,
        column_field_path=column_field_path,
        mandatory_fields=mandatory_fields,
    )
    assert result == []


def test_run_task_pipeline_empty_issue_path_returns_empty(tmp_path, monkeypatch):
    task_log_path = str(tmp_path / "tasks.csv")
    monkeypatch.setattr("src.application.core.pipeline.load_mappings", lambda: {})

    result = run_task_pipeline(
        task_log_path=task_log_path,
        dataset="conservation-area",
        organisation="local-authority:CTY",
        issue_path=str(tmp_path / "nonexistent.csv"),
    )

    assert result == []


# --- _get_column_mapping (workflow) ---


def test_get_column_mapping_returns_list(tmp_path):
    from src.application.core.workflow import _get_column_mapping

    csv_path = tmp_path / "column-field.csv"
    csv_path.write_text(
        "dataset,resource,column,field\nconservation-area,res1,ref,reference\n"
    )

    mock_spec = MagicMock(dataset_field={"conservation-area": ["reference"]})
    result = _get_column_mapping(str(csv_path), "conservation-area", [], mock_spec)

    assert len(result) == 1
    assert result[0]["field"] == "reference"
    assert result[0]["column"] == "ref"
    assert result[0]["mandatory"] is False


def test_get_column_mapping_missing_file_returns_empty(tmp_path):
    from src.application.core.workflow import _get_column_mapping

    mock_spec = MagicMock(dataset_field={})
    result = _get_column_mapping(
        str(tmp_path / "nonexistent.csv"), "conservation-area", [], mock_spec
    )

    assert result == []


def test_get_column_mapping_filters_empty_rows(tmp_path):
    from src.application.core.workflow import _get_column_mapping

    csv_path = tmp_path / "column-field.csv"
    csv_path.write_text(
        "dataset,resource,column,field\nconservation-area,res1,,\nconservation-area,res1,ref,reference\n"
    )

    mock_spec = MagicMock(dataset_field={"conservation-area": ["reference"]})
    result = _get_column_mapping(str(csv_path), "conservation-area", [], mock_spec)

    assert len(result) == 1
    assert result[0]["field"] == "reference"
    assert result[0]["column"] == "ref"
