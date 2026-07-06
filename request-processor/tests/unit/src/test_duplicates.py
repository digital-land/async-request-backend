import csv

from application.core import duplicates


class FakeSpecification:
    def __init__(self, typology="geography"):
        self.typology = typology

    def get_dataset_typology(self, dataset):
        return self.typology


class FakeOrganisationIndex:
    organisation = {
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


def test_duplicate_candidates_are_provision_entities_against_existing_platform(
    tmp_path, monkeypatch
):
    transformed_path = tmp_path / "transformed.csv"
    _write_transformed_csv(transformed_path)

    def fake_run_duplicate_check(rows, spatial_field):
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
    assert candidates[0]["new_organisation_entity"] == ""
    assert candidates[0]["match_type"] == "complete_match"


def test_duplicate_candidates_map_organisation_entities_when_new_entity_is_a(
    tmp_path, monkeypatch
):
    transformed_path = tmp_path / "transformed.csv"
    _write_transformed_csv(transformed_path)

    def fake_run_duplicate_check(rows, spatial_field):
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
    assert candidates[0]["new_organisation_entity"] == ""


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


def test_name_similarity_uses_partial_ratio_for_added_words():
    assert (
        duplicates._name_similarity("South Jesmond", "South Jesmond Conservation Area")
        == "100%"
    )


def test_run_duplicate_check_commits_before_spatialite_metadata():
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


def test_duplicate_candidates_skip_non_geography(tmp_path, monkeypatch):
    transformed_path = tmp_path / "transformed.csv"
    _write_transformed_csv(transformed_path)
    monkeypatch.setattr(
        duplicates,
        "_run_duplicate_check",
        lambda rows, spatial_field: (_ for _ in ()).throw(AssertionError()),
    )

    candidates = duplicates.find_duplicate_redirect_candidates(
        dataset="article-4-direction",
        specification=FakeSpecification(typology="legal-instrument"),
        transformed_csv_path=str(transformed_path),
        organisation_provider="local-authority:STH",
        organisation_index=FakeOrganisationIndex(),
        fetch_platform_entities=lambda dataset, organisation_entity: [
            {"entity": "100"}
        ],
    )

    assert candidates == []
