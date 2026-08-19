import csv
import importlib
import os
import sqlite3
import sys
import types
from contextlib import contextmanager

import duckdb
import pytest
import requests

from application.core import duplicates


class FakeSpecification:
    def __init__(self, typology="geography"):
        self.typology = typology

    def get_dataset_typology(self, dataset):
        return self.typology


class FakeOrganisationIndex:
    def __init__(self):
        self.organisation = {
            "local-authority:STH": {
                "entity": "318",
                "reference": "STH",
                "organisation": "local-authority:STH",
            }
        }

    def lookup(self, organisation):
        return organisation

    def get(self, organisation):
        return self.organisation[organisation]


def _write_transformed_csv(path):
    rows = [
        {
            "entry-number": "1",
            "entity": "200",
            "field": "reference",
            "value": "new-ref",
        },
        {"entry-number": "1", "entity": "200", "field": "name", "value": "New name"},
        {
            "entry-number": "1",
            "entity": "200",
            "field": "entry-date",
            "value": "2026-01-01",
        },
        {
            "entry-number": "1",
            "entity": "200",
            "field": "end-date",
            "value": "",
        },
        {
            "entry-number": "1",
            "entity": "200",
            "field": "geometry",
            "value": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
        },
    ]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["entry-number", "entity", "field", "value"]
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_non_spatial_transformed_csv(path):
    rows = []
    for entity, reference, name, category, entry_date in (
        ("200", "new-ref", "  Main Hall ", "Community", "2026-01-01"),
        ("201", "different-ref", "Other Hall", "Education", "2026-01-01"),
    ):
        for field, value in (
            ("reference", reference),
            ("conservation-area", "CA-MAI-02"),
            ("name", name),
            ("category", category),
            ("entry-date", entry_date),
            ("notes", "resource-only notes"),
            ("description", "resource-only description"),
        ):
            rows.append(
                {
                    "entry-number": entity,
                    "entity": entity,
                    "field": field,
                    "value": value,
                }
            )

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["entry-number", "entity", "field", "value"]
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_non_spatial_transformed_csv_with_organisation(path):
    rows = []
    for field, value in (
        ("reference", "new-ref"),
        ("name", "Main Hall"),
        ("category", "Community"),
        ("entry-date", "2026-01-01"),
        ("organisation", "local-authority:STH"),
        ("organisation-entity", "resource-only-value"),
    ):
        rows.append(
            {
                "entry-number": "200",
                "entity": "200",
                "field": field,
                "value": value,
            }
        )

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f, fieldnames=["entry-number", "entity", "field", "value"]
        )
        writer.writeheader()
        writer.writerows(rows)


@contextmanager
def _existing_parquet(path):
    yield str(path)


def _write_platform_parquet(path, rows):
    csv_path = path.with_suffix(".source.csv")
    fieldnames = list(dict.fromkeys(field for row in rows for field in row))
    with open(csv_path, "w", newline="", encoding="utf-8") as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    escaped_parquet_path = str(path).replace("'", "''")
    connection = duckdb.connect()
    try:
        connection.execute(
            f"""
            COPY (
                SELECT * FROM read_csv(?, all_varchar = true, header = true)
            ) TO '{escaped_parquet_path}' (FORMAT PARQUET)
            """,
            [str(csv_path)],
        )
    finally:
        connection.close()


def test_non_spatial_comparison_fields_use_whitelist():
    fields = duplicates._non_spatial_comparison_fields(
        [
            {
                "entity": "200",
                "name": "Main Hall",
                "start-date": "2026-01-01",
                "doc-url": "https://example.com/doc",
                "document-url": "https://example.com/document",
                "documentation-url": "https://example.com/documentation",
                "category": "Community",
                "conservation-area": "CA-MAI-02",
                "reference": "new-ref",
            }
        ]
    )

    assert fields == ["doc-url", "document-url", "name", "start-date"]


def test_duplicate_candidates_are_provision_entities_against_existing_platform(
    tmp_path, monkeypatch
):
    transformed_path = tmp_path / "transformed.csv"
    _write_transformed_csv(transformed_path)

    def fake_run_duplicate_check(rows, spatial_field):
        provision_row = next(row for row in rows if row["entity"] == "200")
        assert provision_row["organisation_entity"] == "318"

        if spatial_field == "point":
            return {"complete_matches": [], "single_matches": []}
        return {
            "complete_matches": [
                {
                    "entity_a": "100",
                    "organisation_entity_a": "318",
                    "entity_b": "200",
                    "organisation_entity_b": "",
                },
                {
                    "entity_a": "101",
                    "organisation_entity_a": "11",
                    "entity_b": "102",
                    "organisation_entity_b": "12",
                },
            ],
            "single_matches": [],
        }

    def fake_fetch_platform_entities(dataset, organisation_entity):
        assert dataset == "conservation-area"
        assert organisation_entity == "318"
        return [
            {
                "entity": "100",
                "reference": "old-ref",
                "name": "Old name",
                "entry_date": "2020-01-01",
                "end_date": "",
                "geometry": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
                "organisation_entity": "318",
            },
            {"entity": "101", "reference": "existing-a"},
            {"entity": "102", "reference": "existing-b"},
        ]

    monkeypatch.setattr(duplicates, "_run_duplicate_check", fake_run_duplicate_check)

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="conservation-area",
        specification=FakeSpecification(),
        transformed_csv_path=str(transformed_path),
        redirect_lookups={
            "100": {"entity": "300", "status": "301"},
            "999": {"entity": "998", "status": "301"},
        },
        organisation_provider="local-authority:STH",
        organisation_index=FakeOrganisationIndex(),
        fetch_platform_entities=fake_fetch_platform_entities,
    )

    assert len(candidates) == 1
    assert candidates[0]["old_entity"] == "100"
    assert candidates[0]["entity"] == "200"
    assert candidates[0]["old_reference"] == "old-ref"
    assert candidates[0]["new_reference"] == "new-ref"
    assert candidates[0]["old_entry_date"] == "2020-01-01"
    assert candidates[0]["new_entry_date"] == "2026-01-01"
    assert candidates[0]["old_end_date"] == ""
    assert candidates[0]["new_end_date"] == ""
    assert candidates[0]["old_organisation"] == "local-authority:STH"
    assert candidates[0]["new_organisation"] == "local-authority:STH"
    assert candidates[0]["old_organisation_entity"] == "318"
    assert candidates[0]["new_organisation_entity"] == "318"
    assert candidates[0]["match_type"] == "complete_match"
    assert candidates[0]["old_entity_redirects"] == [
        {
            "old-entity": "100",
            "status": "301",
            "entity": "300",
        }
    ]


def test_duplicate_candidates_map_organisation_entities_when_new_entity_is_a(
    tmp_path, monkeypatch
):
    transformed_path = tmp_path / "transformed.csv"
    _write_transformed_csv(transformed_path)

    def fake_run_duplicate_check(rows, spatial_field):
        provision_row = next(row for row in rows if row["entity"] == "200")
        assert provision_row["organisation_entity"] == "318"

        if spatial_field == "point":
            return {"complete_matches": [], "single_matches": []}
        return {
            "complete_matches": [
                {
                    "entity_a": "200",
                    "organisation_entity_a": "",
                    "entity_b": "100",
                    "organisation_entity_b": "318",
                },
            ],
            "single_matches": [],
        }

    def fake_fetch_platform_entities(dataset, organisation_entity):
        return [
            {
                "entity": "100",
                "reference": "old-ref",
                "name": "Old name",
                "geometry": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
            },
        ]

    monkeypatch.setattr(duplicates, "_run_duplicate_check", fake_run_duplicate_check)

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="conservation-area",
        specification=FakeSpecification(),
        transformed_csv_path=str(transformed_path),
        organisation_provider="local-authority:STH",
        organisation_index=FakeOrganisationIndex(),
        fetch_platform_entities=fake_fetch_platform_entities,
    )

    assert candidates[0]["old_entity"] == "100"
    assert candidates[0]["entity"] == "200"
    assert candidates[0]["old_organisation_entity"] == "318"
    assert candidates[0]["new_organisation_entity"] == "318"


def test_fetch_platform_entities_reads_datasette_without_pagination(monkeypatch):
    calls = []

    class FakeResponse:
        def raise_for_status(self):
            pass

        def json(self):
            return [
                {
                    "entity": "100",
                    "reference": "old-ref",
                    "organisation_entity": "318",
                }
            ]

    def fake_get(url, params, timeout):
        calls.append({"url": url, "params": params, "timeout": timeout})
        return FakeResponse()

    monkeypatch.setattr(duplicates.requests, "get", fake_get)

    rows = duplicates._fetch_platform_entities("conservation-area", "318")

    assert rows[0]["entity"] == "100"
    assert calls == [
        {
            "url": (
                "https://datasette.planning.data.gov.uk/"
                "conservation-area/entity.json"
            ),
            "params": {
                "_shape": "array",
                "_size": "max",
                "organisation_entity__exact": "318",
            },
            "timeout": 120,
        }
    ]


def test_datasette_base_url_can_be_configured(monkeypatch):
    monkeypatch.setenv(
        "DATASETTE_BASE_URL", "https://datasette.staging.planning.data.gov.uk"
    )
    reloaded = importlib.reload(duplicates)

    try:
        assert (
            reloaded.DATASETTE_BASE_URL
            == "https://datasette.staging.planning.data.gov.uk"
        )
    finally:
        monkeypatch.delenv("DATASETTE_BASE_URL")
        importlib.reload(duplicates)


def test_name_similarity_uses_partial_ratio_for_added_words():
    assert (
        duplicates._name_similarity("South Jesmond", "South Jesmond Conservation Area")
        == "100%"
    )


def test_run_duplicate_check_commits_before_spatialite_metadata(monkeypatch):
    rows = [
        {
            "entity": "100",
            "reference": "old-ref",
            "geometry": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
        },
        {
            "entity": "200",
            "reference": "new-ref",
            "geometry": "POLYGON ((0 0, 0 1, 1 1, 1 0, 0 0))",
        },
    ]

    class FakeConnection:
        def __init__(self, path):
            self.conn = sqlite3.connect(path)
            self.committed = False

        def execute(self, *args, **kwargs):
            return self.conn.execute(*args, **kwargs)

        def executemany(self, *args, **kwargs):
            return self.conn.executemany(*args, **kwargs)

        def commit(self):
            self.committed = True
            return self.conn.commit()

        def close(self):
            return self.conn.close()

    def fake_duplicate_geometry_check(conn, spatial_field):
        assert conn.committed
        assert spatial_field == "geometry"
        return (
            None,
            None,
            {
                "complete_matches": [{"entity_a": 100, "entity_b": 200}],
                "single_matches": [],
            },
        )

    monkeypatch.setitem(
        sys.modules,
        "spatialite",
        types.SimpleNamespace(connect=lambda path: FakeConnection(path)),
    )
    monkeypatch.setattr(
        "digital_land.expectations.operations.dataset.duplicate_geometry_check",
        fake_duplicate_geometry_check,
    )

    matches = duplicates._run_duplicate_check(rows, "geometry")

    assert matches["complete_matches"][0]["entity_a"] == 100
    assert matches["complete_matches"][0]["entity_b"] == 200


def test_duplicate_candidates_skip_non_conservation_area(tmp_path, monkeypatch):
    transformed_path = tmp_path / "transformed.csv"
    _write_transformed_csv(transformed_path)
    monkeypatch.setattr(
        duplicates,
        "_run_duplicate_check",
        lambda rows, spatial_field: (_ for _ in ()).throw(AssertionError()),
    )

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="tree",
        specification=FakeSpecification(),
        transformed_csv_path=str(transformed_path),
        organisation_provider="local-authority:STH",
        organisation_index=FakeOrganisationIndex(),
        fetch_platform_entities=lambda dataset, organisation_entity: (
            _ for _ in ()
        ).throw(AssertionError()),
    )

    assert candidates == []


def test_non_spatial_candidates_match_all_comparable_fields(tmp_path, monkeypatch):
    transformed_path = tmp_path / "transformed.csv"
    _write_non_spatial_transformed_csv(transformed_path)
    platform_path = tmp_path / "platform.parquet"
    _write_platform_parquet(
        platform_path,
        [
            {
                " Entity ": "100",
                "Reference": "old-ref",
                "conservation-area": "CA04",
                " Name ": "main hall",
                "CATEGORY": " community ",
                "entry_date": "2020-01-01",
                "notes": "platform-only metadata",
            },
            {
                " Entity ": "101",
                "Reference": "old-other",
                " Name ": "Different Hall",
                "CATEGORY": "Different",
                "entry-date": "2020-01-01",
                "notes": "",
            },
        ],
    )
    monkeypatch.setattr(
        duplicates,
        "_run_duplicate_check",
        lambda rows, spatial_field: (_ for _ in ()).throw(AssertionError()),
    )

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="tree-preservation-order",
        specification=FakeSpecification(typology="legal-instrument"),
        transformed_csv_path=str(transformed_path),
        redirect_lookups={"100": {"entity": "300", "status": "301"}},
        download_platform_dataset_parquet=lambda dataset: _existing_parquet(
            platform_path
        ),
    )

    assert len(candidates) == 1
    assert candidates[0]["old_entity"] == "100"
    assert candidates[0]["entity"] == "200"
    assert candidates[0]["old_reference"] == "old-ref"
    assert candidates[0]["new_reference"] == "new-ref"
    assert candidates[0]["match_type"] == "all_fields_match"
    assert candidates[0]["evidence"] == "all comparable fields match"
    assert candidates[0]["old_entry_date"] == "2020-01-01"
    assert candidates[0]["new_entry_date"] == "2026-01-01"
    assert candidates[0]["old_fields"]["category"] == " community "
    assert candidates[0]["new_fields"]["category"] == "Community"
    assert candidates[0]["old_fields"]["conservation-area"] == "CA04"
    assert candidates[0]["new_fields"]["conservation-area"] == "CA-MAI-02"
    assert candidates[0]["old_entity_redirects"] == [
        {"old-entity": "100", "entity": "300", "status": "301"}
    ]


def test_non_spatial_candidates_emit_each_old_new_pair_and_skip_same_entity(tmp_path):
    transformed_path = tmp_path / "transformed.csv"
    _write_non_spatial_transformed_csv(transformed_path)
    platform_path = tmp_path / "platform.parquet"
    _write_platform_parquet(
        platform_path,
        [
            {
                "entity": "100",
                "reference": "old-a",
                "name": "Main Hall",
                "category": "Community",
            },
            {
                "entity": "101",
                "reference": "old-b",
                "name": "Main Hall",
                "category": "Community",
            },
            {
                "entity": "200",
                "reference": "same-entity",
                "name": "Main Hall",
                "category": "Community",
            },
        ],
    )

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="tree-preservation-order",
        specification=FakeSpecification(typology="legal-instrument"),
        transformed_csv_path=str(transformed_path),
        download_platform_dataset_parquet=lambda dataset: _existing_parquet(
            platform_path
        ),
    )

    assert sorted((row["old_entity"], row["entity"]) for row in candidates) == [
        ("100", "200"),
        ("101", "200"),
    ]


def test_non_spatial_candidates_resolve_platform_organisation_entity(tmp_path):
    transformed_path = tmp_path / "transformed.csv"
    _write_non_spatial_transformed_csv_with_organisation(transformed_path)
    platform_path = tmp_path / "platform.parquet"
    _write_platform_parquet(
        platform_path,
        [
            {
                "entity": "100",
                "reference": "old-ref",
                "name": "Main Hall",
                "category": "Community",
                "entry-date": "2020-01-01",
                "organisation": "",
                "organisation-entity": "318",
            },
            {
                "entity": "101",
                "reference": "other-org",
                "name": "Main Hall",
                "category": "Community",
                "entry-date": "2020-01-01",
                "organisation": "",
                "organisation-entity": "999",
            },
        ],
    )

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="tree-preservation-order",
        specification=FakeSpecification(typology="legal-instrument"),
        transformed_csv_path=str(transformed_path),
        organisation_provider="local-authority:STH",
        organisation_index=FakeOrganisationIndex(),
        download_platform_dataset_parquet=lambda dataset: _existing_parquet(
            platform_path
        ),
    )

    assert [(row["old_entity"], row["entity"]) for row in candidates] == [
        ("100", "200")
    ]
    assert candidates[0]["old_organisation"] == "local-authority:STH"
    assert candidates[0]["old_organisation_entity"] == "318"
    assert candidates[0]["old_fields"]["organisation"] == "local-authority:STH"


def test_non_spatial_candidate_download_failure_is_propagated(tmp_path):
    transformed_path = tmp_path / "transformed.csv"
    _write_non_spatial_transformed_csv(transformed_path)

    @contextmanager
    def fail_fetch(dataset):
        raise requests.RequestException("unavailable")
        yield

    with pytest.raises(requests.RequestException, match="unavailable"):
        duplicates.find_duplicate_redirect_candidates(
            dataset="tree-preservation-order",
            specification=FakeSpecification(typology="legal-instrument"),
            transformed_csv_path=str(transformed_path),
            download_platform_dataset_parquet=fail_fetch,
        )


@pytest.mark.parametrize("missing_status", [403, 404])
def test_non_spatial_candidate_missing_published_parquet_returns_empty(
    monkeypatch, tmp_path, missing_status
):
    transformed_path = tmp_path / "transformed.csv"
    _write_non_spatial_transformed_csv(transformed_path)
    calls = []

    class FakeResponse:
        status_code = missing_status

        def raise_for_status(self):
            raise AssertionError(
                f"{missing_status} should be handled before raise_for_status"
            )

        def close(self):
            calls.append("closed")

    def fake_get(url, timeout, stream):
        calls.append((url, timeout, stream))
        return FakeResponse()

    monkeypatch.setattr(duplicates.requests, "get", fake_get)

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="new-dataset",
        specification=FakeSpecification(typology="legal-instrument"),
        transformed_csv_path=str(transformed_path),
    )

    assert candidates == []
    assert calls == [
        (
            f"{duplicates.DATASTORE_URL.rstrip('/')}/dataset/new-dataset.parquet",
            120,
            True,
        ),
        "closed",
    ]


def test_download_platform_dataset_parquet_streams_configured_file(
    monkeypatch, tmp_path
):
    calls = []
    source_path = tmp_path / "source.parquet"
    _write_platform_parquet(source_path, [{"entity": "100", "name": "Main Hall"}])
    parquet_bytes = source_path.read_bytes()

    class FakeResponse:
        status_code = 200
        headers = {"Content-Length": str(len(parquet_bytes))}

        def raise_for_status(self):
            pass

        def iter_content(self, chunk_size):
            assert chunk_size == 1024 * 1024
            yield parquet_bytes

        def close(self):
            calls.append("closed")

    def fake_get(url, timeout, stream):
        calls.append((url, timeout, stream))
        return FakeResponse()

    monkeypatch.setattr(duplicates.requests, "get", fake_get)

    with duplicates._download_platform_dataset_parquet("test dataset") as path:
        downloaded_path = path
        connection = duckdb.connect()
        try:
            rows = connection.execute(
                "SELECT entity, name FROM read_parquet(?)", [path]
            ).fetchall()
        finally:
            connection.close()

    assert rows == [("100", "Main Hall")]
    assert not os.path.exists(downloaded_path)
    assert calls == [
        (
            f"{duplicates.DATASTORE_URL.rstrip('/')}/dataset/test%20dataset.parquet",
            120,
            True,
        ),
        "closed",
    ]
