import csv
import json
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import duckdb
import pytest

from application.core import platform_comparison as comparison


class OrganisationIndex:
    def lookup(self, provider):
        return provider if provider == "local-authority:ABC" else None

    def get(self, provider):
        return {"entity": "318", "organisation": provider}


def write_resource(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(
            stream, fieldnames=["entry-number", "entity", "field", "value"]
        )
        writer.writeheader()
        for entry, entity, fields in rows:
            for field, value in fields.items():
                writer.writerow(
                    {
                        "entry-number": entry,
                        "entity": entity,
                        "field": field,
                        "value": value,
                    }
                )
    return path


def write_platform(path, rows):
    json_path = path.with_suffix(".json")
    json_path.write_text(json.dumps(rows))
    with duckdb.connect() as connection:
        connection.read_json(str(json_path)).write_parquet(str(path))
    return path


def snapshot_row(entity, **fields):
    return {
        "entity": entity,
        "organisation-entity": "318",
        "quality": "authoritative",
        **fields,
    }


def prepare(resource, parquet):
    @contextmanager
    def download(dataset):
        assert dataset == "green-belt"
        yield str(parquet) if parquet else None

    return comparison.prepare_platform_comparison(
        "green-belt",
        str(resource),
        "local-authority:ABC",
        OrganisationIndex(),
        download,
    )


def test_comparison_preserves_categories_repeated_rows_and_authoritative_scope(
    tmp_path,
):
    resource = write_resource(
        tmp_path / "resource.csv",
        [
            (
                1,
                100,
                {"name": "Area", "geometry": "POINT (0 0)", "entry-date": "2026-01-01"},
            ),
            (2, "100.0", {"name": "Area", "geometry": "POINT (1 1)"}),
            (3, 200, {"name": "New area", "geometry": "POINT (2 2)"}),
            (
                4,
                101,
                {
                    "name": "Same",
                    "geometry": "MULTIPOINT ((3.0000 3.0000))",
                    "height": "100.0",
                },
            ),
        ],
    )
    parquet = write_platform(
        tmp_path / "platform.parquet",
        [
            snapshot_row(
                100, name="Area", geometry="POINT (0 0)", **{"entry-date": "2020-01-01"}
            ),
            snapshot_row(101, name="Same", geometry="POINT (3 3)", height=100),
            snapshot_row(999, name="Platform only", geometry="POINT (5 5)"),
            snapshot_row(700, name="Non-authoritative", quality="secondary"),
            snapshot_row(
                701, name="Other organisation", **{"organisation-entity": "999"}
            ),
        ],
    )
    summary, rows = prepare(resource, parquet)
    assert summary["status"] == "complete"
    assert summary["existing_count"] == 3
    assert summary["row_count"] == 5
    assert summary["counts"] == {"new": 1, "changed": 1, "in_both": 1, "existing": 1}
    assert [row["category"] for row in rows] == [
        "in_both",
        "changed",
        "new",
        "in_both",
        "existing",
    ]
    assert rows[1]["changed_fields"] == {"geometry": "(different geometry on platform)"}
    assert rows[-1]["entity"] == "999"
    assert (
        "fields" not in rows[0]
    )  # Resource geometry is already stored in the transformed facts.
    assert "geometry" in summary["columns"]
    json.dumps({"summary": summary, "rows": rows})


def test_platform_size_limit_skips_comparison(tmp_path, monkeypatch):
    resource = write_resource(tmp_path / "resource.csv", [(1, 100, {"name": "A"})])
    parquet = write_platform(
        tmp_path / "platform.parquet", [snapshot_row(100), snapshot_row(101)]
    )
    monkeypatch.setattr(comparison, "_PLATFORM_ENTITY_LIMIT", 1)
    diff = MagicMock(side_effect=AssertionError("Should not compare over the limit"))
    monkeypatch.setattr(comparison, "_diff_entity_fields", diff)
    summary, rows = prepare(resource, parquet)
    assert summary["status"] == "too-large"
    assert summary["existing_count"] == 2
    assert rows == []
    diff.assert_not_called()


@pytest.mark.parametrize("missing", ["snapshot", "quality", "organisation-entity"])
def test_incomplete_platform_input_never_looks_like_an_empty_platform(
    tmp_path, missing
):
    resource = write_resource(tmp_path / "resource.csv", [(1, 100, {"name": "A"})])
    if missing == "snapshot":
        parquet = None
    else:
        row = snapshot_row(100)
        row.pop(missing)
        parquet = write_platform(tmp_path / "platform.parquet", [row])
    summary, rows = prepare(resource, parquet)
    assert summary["status"] == "unavailable"
    assert rows == []


def test_missing_organisation_does_not_download_platform(tmp_path):
    resource = write_resource(tmp_path / "resource.csv", [(1, 100, {"name": "A"})])
    download = MagicMock(
        side_effect=AssertionError("Missing organisation must fail closed")
    )
    summary, rows = comparison.prepare_platform_comparison(
        "green-belt",
        str(resource),
        "unknown",
        OrganisationIndex(),
        download,
    )
    assert summary["status"] == "unavailable"
    assert rows == []
    download.assert_not_called()


def test_comparison_error_does_not_return_partial_rows(tmp_path, monkeypatch):
    resource = write_resource(
        tmp_path / "resource.csv", [(1, 100, {"name": "A"}), (2, 101, {"name": "B"})]
    )
    parquet = write_platform(
        tmp_path / "platform.parquet", [snapshot_row(100), snapshot_row(101)]
    )
    monkeypatch.setattr(
        comparison,
        "_diff_entity_fields",
        MagicMock(side_effect=[{}, RuntimeError("broken")]),
    )
    summary, rows = prepare(resource, parquet)
    assert summary["status"] == "unavailable"
    assert rows == []


def test_pipeline_downloads_snapshot_once_for_duplicates_and_comparison(
    tmp_path, monkeypatch
):
    from application.core import pipeline

    source = tmp_path / "input"
    source.mkdir()
    (source / "input.csv").write_text("name\nArea\n")
    output = write_resource(tmp_path / "transformed.csv", [(1, 100, {"name": "Area"})])
    parquet = write_platform(
        tmp_path / "platform.parquet", [snapshot_row(100, name="Area")]
    )
    downloads = []

    @contextmanager
    def download(dataset):
        downloads.append(dataset)
        yield str(parquet)

    def duplicates(*args):
        with args[-1]("green-belt") as path:
            assert path == str(parquet)
        return []

    pipe = MagicMock()
    monkeypatch.setattr(pipeline, "Pipeline", lambda *args, **kwargs: pipe)
    monkeypatch.setattr(pipeline, "Organisation", lambda *args: OrganisationIndex())
    monkeypatch.setattr(pipeline, "API", MagicMock())
    monkeypatch.setattr(
        pipeline,
        "_process_add_data_resource",
        lambda *args, **kwargs: (pipe, None, [], [], []),
    )
    monkeypatch.setattr(pipeline, "_find_duplicate_candidates", duplicates)
    monkeypatch.setattr(pipeline, "_download_platform_dataset_parquet", download)
    result = pipeline.fetch_add_data_response(
        "green-belt",
        "local-authority:ABC",
        str(tmp_path),
        str(source),
        str(output),
        SimpleNamespace(),
        str(tmp_path),
        "endpoint",
    )
    assert downloads == ["green-belt"]
    assert result["platform-comparison"]["status"] == "complete"
    assert result["comparison-rows"][0]["category"] == "in_both"


def test_comparison_rows_are_stored_separately_from_normal_facts():
    import tasks

    session = MagicMock()
    comparison_rows = [
        {"source": "resource", "entity": "100", "entry_number": "1", "category": "new"}
    ]
    tasks._save_response_details(
        session, 42, [{"name": "Area"}], [], [], comparison_rows=comparison_rows
    )
    details = [call.args[0].detail for call in session.add.call_args_list]
    assert details[0]["converted_row"] == {"name": "Area"}
    assert "platform-comparison" not in details[0]
    assert details[1] == {"platform-comparison": comparison_rows[0]}
    session.commit.assert_called_once()


def test_repeated_entity_summary_uses_last_entry_number_not_csv_order(tmp_path):
    resource = write_resource(
        tmp_path / "resource.csv",
        [
            (2, 100, {"name": "Changed"}),
            (1, 100, {"name": "Original"}),
        ],
    )
    parquet = write_platform(
        tmp_path / "platform.parquet", [snapshot_row(100, name="Original")]
    )
    summary, rows = prepare(resource, parquet)
    assert [row["entry_number"] for row in rows] == ["1", "2"]
    assert [row["category"] for row in rows] == ["in_both", "changed"]
    assert summary["counts"] == {"new": 0, "changed": 1, "in_both": 0, "existing": 0}


def test_empty_authoritative_scope_is_a_valid_empty_platform(tmp_path):
    resource = write_resource(tmp_path / "resource.csv", [(1, 100, {"name": "Area"})])
    parquet = write_platform(
        tmp_path / "platform.parquet", [snapshot_row(999, quality="secondary")]
    )
    summary, rows = prepare(resource, parquet)
    assert summary["status"] == "complete"
    assert summary["existing_count"] == 0
    assert [row["category"] for row in rows] == ["new"]


def test_duplicate_platform_ids_disable_comparison(tmp_path):
    resource = write_resource(tmp_path / "resource.csv", [(1, 100, {"name": "Area"})])
    parquet = write_platform(
        tmp_path / "platform.parquet", [snapshot_row(100), snapshot_row(100)]
    )
    summary, rows = prepare(resource, parquet)
    assert summary["status"] == "unavailable"
    assert rows == []
