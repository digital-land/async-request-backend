import csv
import os
import sqlite3
import tempfile

import requests
from rapidfuzz import fuzz

from application.logging.logger import get_logger

logger = get_logger(__name__)

DATASETTE_BASE_URL = "https://datasette.planning.data.gov.uk"
REDIRECT_NOTE = "Redirect duplicate entity selected in Assign Entities"


def _normalise_entity_id(raw) -> str:
    if raw is None or raw == "":
        return ""
    try:
        return str(int(float(str(raw))))
    except (ValueError, TypeError):
        return str(raw)


def _name_similarity(existing_name: str, new_name: str) -> str:
    if not existing_name or not new_name:
        return ""
    similarity = fuzz.partial_ratio(existing_name.lower(), new_name.lower())
    return f"{round(similarity)}%"


def _read_provision_entities(transformed_csv_path: str) -> list:
    if not os.path.exists(transformed_csv_path):
        return []

    entity_fields = {
        "reference",
        "name",
        "geometry",
        "point",
        "organisation",
        "organisation_entity",
        "organisation-entity",
        "entry-date",
        "end-date",
    }
    entities = {}
    with open(transformed_csv_path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            entity = _normalise_entity_id(row.get("entity"))
            field = row.get("field", "")
            if not entity or field not in entity_fields:
                continue

            entity_row = entities.setdefault(entity, {"entity": entity})
            entity_row[field] = row.get("value", "")

    return list(entities.values())


def _organisation_row_for_provider(
    organisation_index, organisation_provider: str
) -> dict:
    if not organisation_index or not organisation_provider:
        return {}

    try:
        organisation = organisation_index.lookup(organisation_provider)
        if not organisation:
            return {}
        return organisation_index.get(organisation) or {}
    except (AttributeError, KeyError):
        return {}


def _resolve_organisation_entity(organisation_index, organisation_provider: str) -> str:
    return str(
        _organisation_row_for_provider(organisation_index, organisation_provider).get(
            "entity", ""
        )
    )


def _organisation_identifier_for_entity(
    organisation_index, organisation_entity: str
) -> str:
    if not organisation_index or not organisation_entity:
        return ""

    try:
        for row in organisation_index.organisation.values():
            if str(row.get("entity", "")) == str(organisation_entity):
                return row.get("organisation") or row.get("reference", "")
    except AttributeError:
        return ""

    return ""


def _fetch_platform_entities(dataset: str, organisation_entity: str) -> list:
    if not organisation_entity:
        return []

    url = f"{DATASETTE_BASE_URL.rstrip('/')}/{dataset}/entity.json"
    response = requests.get(
        url,
        params={
            "_shape": "array",
            "_size": "max",
            "organisation_entity__exact": organisation_entity,
        },
        timeout=120,
    )
    response.raise_for_status()
    data = response.json()
    if isinstance(data, list):
        return data
    return data.get("rows") or []


def _create_entity_table(conn: sqlite3.Connection, rows: list):
    conn.execute(
        """
        CREATE TABLE entity (
            entity INTEGER,
            reference TEXT,
            organisation_entity INTEGER,
            geometry TEXT,
            point TEXT,
            name TEXT
        );
        """
    )
    conn.executemany(
        """
        INSERT INTO entity (
            entity, reference, organisation_entity, geometry, point, name
        ) VALUES (?, ?, ?, ?, ?, ?);
        """,
        [
            (
                _normalise_entity_id(row.get("entity")),
                row.get("reference", ""),
                row.get("organisation_entity")
                or row.get("organisation-entity")
                or None,
                row.get("geometry", ""),
                row.get("point", ""),
                row.get("name", ""),
            )
            for row in rows
            if _normalise_entity_id(row.get("entity"))
        ],
    )


def _run_duplicate_check(rows: list, spatial_field: str) -> dict:
    if not any(row.get(spatial_field) for row in rows):
        return {"complete_matches": [], "single_matches": []}

    fd, path = tempfile.mkstemp(suffix=".sqlite3")
    os.close(fd)
    conn = None
    try:
        import spatialite
        from digital_land.expectations.operations.dataset import (
            duplicate_geometry_check,
        )

        conn = spatialite.connect(path)
        _create_entity_table(conn, rows)
        conn.commit()
        _, _, details = duplicate_geometry_check(conn, spatial_field)
        return {
            "complete_matches": details.get("complete_matches", []),
            "single_matches": details.get("single_matches", []),
        }
    finally:
        if conn is not None:
            conn.close()
        try:
            os.unlink(path)
        except OSError:
            pass


def _build_candidate(
    *,
    match: dict,
    match_type: str,
    spatial_field: str,
    old_entity: dict,
    new_entity: dict,
    dataset: str,
    old_organisation_entity: str = "",
    new_organisation_entity: str = "",
    organisation_index=None,
    organisation_provider: str = "",
) -> dict:
    existing_name = str(old_entity.get("name", "") or "")
    new_name = str(new_entity.get("name", "") or "")
    new_organisation = _organisation_row_for_provider(
        organisation_index, organisation_provider
    ).get("organisation", "") or str(new_entity.get("organisation", "") or "")
    old_organisation_entity = str(
        old_entity.get("organisation_entity", "")
        or old_organisation_entity
        or match.get("organisation_entity_a", "")
    )
    old_organisation = _organisation_identifier_for_entity(
        organisation_index, old_organisation_entity
    )
    evidence = [
        "point exact match" if spatial_field == "point" else f"geometry {match_type}"
    ]
    similarity = _name_similarity(existing_name, new_name)
    if similarity:
        evidence.append(f"name similarity {similarity}")

    redirect = {
        "old_entity": _normalise_entity_id(old_entity.get("entity")),
        "entity": _normalise_entity_id(new_entity.get("entity")),
        "dataset": dataset,
        "old_reference": str(old_entity.get("reference", "") or ""),
        "new_reference": str(new_entity.get("reference", "") or ""),
        "match_type": match_type,
        "notes": REDIRECT_NOTE,
    }

    return {
        **redirect,
        "old_name": existing_name,
        "new_name": new_name,
        "old_entry_date": str(
            old_entity.get("entry_date", "") or old_entity.get("entry-date", "") or ""
        ),
        "new_entry_date": str(
            new_entity.get("entry_date", "") or new_entity.get("entry-date", "") or ""
        ),
        "old_end_date": str(
            old_entity.get("end_date", "") or old_entity.get("end-date", "") or ""
        ),
        "new_end_date": str(
            new_entity.get("end_date", "") or new_entity.get("end-date", "") or ""
        ),
        "old_organisation": old_organisation,
        "new_organisation": new_organisation,
        "old_organisation_entity": old_organisation_entity,
        "new_organisation_entity": str(
            new_entity.get("organisation_entity", "")
            or new_organisation_entity
            or match.get("organisation_entity_b", "")
        ),
        "evidence": ", ".join(evidence),
        "name_similarity": similarity,
    }


def _entities_for_match(match: dict, provision_by_id: dict, platform_by_id: dict):
    entity_a = _normalise_entity_id(match.get("entity_a"))
    entity_b = _normalise_entity_id(match.get("entity_b"))
    if entity_a in provision_by_id and entity_b in platform_by_id:
        return {
            "new_entity": provision_by_id[entity_a],
            "old_entity": platform_by_id[entity_b],
            "old_organisation_entity": match.get("organisation_entity_b", ""),
            "new_organisation_entity": match.get("organisation_entity_a", ""),
        }

    if entity_b in provision_by_id and entity_a in platform_by_id:
        return {
            "new_entity": provision_by_id[entity_b],
            "old_entity": platform_by_id[entity_a],
            "old_organisation_entity": match.get("organisation_entity_a", ""),
            "new_organisation_entity": match.get("organisation_entity_b", ""),
        }

    return None


def find_duplicate_redirect_candidates(
    *,
    dataset: str,
    specification,
    transformed_csv_path: str,
    organisation_provider: str = "",
    organisation_index=None,
    fetch_platform_entities=_fetch_platform_entities,
) -> list[dict]:
    if dataset != "conservation-area":
        return []

    if specification.get_dataset_typology(dataset) != "geography":
        return []

    provision_rows = _read_provision_entities(transformed_csv_path)
    if not provision_rows:
        return []

    organisation_entity = _resolve_organisation_entity(
        organisation_index, organisation_provider
    )
    for row in provision_rows:
        row.setdefault("organisation_entity", organisation_entity)

    platform_rows = fetch_platform_entities(dataset, organisation_entity)
    if not platform_rows:
        return []

    provision_by_id = {
        _normalise_entity_id(row.get("entity")): row for row in provision_rows
    }
    platform_by_id = {
        _normalise_entity_id(row.get("entity")): row for row in platform_rows
    }
    combined_rows = platform_rows + provision_rows

    matches = []
    for spatial_field in ("geometry", "point"):
        field_matches = _run_duplicate_check(combined_rows, spatial_field)
        matches.extend(
            (match, "complete_match", spatial_field)
            for match in field_matches.get("complete_matches", [])
        )
        matches.extend(
            (match, "single_match", spatial_field)
            for match in field_matches.get("single_matches", [])
        )

    candidates = []
    seen = set()
    for match, match_type, spatial_field in matches:
        matched_entities = _entities_for_match(match, provision_by_id, platform_by_id)
        if not matched_entities:
            continue
        new_entity = matched_entities["new_entity"]
        old_entity = matched_entities["old_entity"]

        key = (
            _normalise_entity_id(old_entity.get("entity")),
            _normalise_entity_id(new_entity.get("entity")),
        )
        if key in seen or key[0] == key[1]:
            continue
        seen.add(key)
        candidates.append(
            _build_candidate(
                match=match,
                match_type=match_type,
                spatial_field=spatial_field,
                old_entity=old_entity,
                new_entity=new_entity,
                dataset=dataset,
                old_organisation_entity=matched_entities["old_organisation_entity"],
                new_organisation_entity=matched_entities["new_organisation_entity"],
                organisation_index=organisation_index,
                organisation_provider=organisation_provider,
            )
        )

    return candidates
