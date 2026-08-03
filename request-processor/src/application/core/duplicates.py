import os
import sqlite3
import tempfile
from contextlib import contextmanager
from urllib.parse import quote

import duckdb
import requests
from rapidfuzz import fuzz

from application.configurations.config import DATASTORE_URL
from application.logging.logger import get_logger

logger = get_logger(__name__)

DATASETTE_BASE_URL = os.getenv(
    "DATASETTE_BASE_URL", "https://datasette.planning.data.gov.uk"
)
REDIRECT_NOTE = "Redirect duplicate entity selected in Assign Entities"
NON_SPATIAL_EXCLUDED_FIELDS = {
    "reference",
    "entry-date",
    "notes",
    "description",
    "organisation-entity",
    "organisation_entity",
}


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

    connection = duckdb.connect()
    try:
        rows = connection.execute(
            """
            SELECT entity, field, value
            FROM read_csv(?, all_varchar = true, header = true)
            """,
            [transformed_csv_path],
        ).fetchall()
    finally:
        connection.close()

    entities = {}
    for raw_entity, raw_field, value in rows:
        entity = _normalise_entity_id(raw_entity)
        field = str(raw_field or "").strip().lower()
        if not entity or not field:
            continue

        entity_row = entities.setdefault(entity, {"entity": entity})
        entity_row[field] = value or ""

    return list(entities.values())


def _normalise_fingerprint_value(value) -> str:
    return str(value or "").strip().lower()


def _normalise_entity_fields(row: dict) -> dict:
    return {
        str(field).strip().lower(): value
        for field, value in row.items()
        if str(field).strip()
    }


def _non_spatial_comparison_fields(provision_rows: list[dict]) -> list[str]:
    fields = {
        str(field).strip().lower()
        for row in provision_rows
        for field in row
        if field != "entity"
        and str(field).strip().lower() not in NON_SPATIAL_EXCLUDED_FIELDS
    }
    return sorted(field for field in fields if field)


def _quote_identifier(identifier: str) -> str:
    return f'"{identifier.replace(chr(34), chr(34) * 2)}"'


def _quote_literal(value: str) -> str:
    return f"'{str(value).replace(chr(39), chr(39) * 2)}'"


@contextmanager
def _download_platform_dataset_parquet(dataset: str):
    url = f"{DATASTORE_URL.rstrip('/')}/dataset/{quote(dataset)}.parquet"
    file_descriptor, parquet_path = tempfile.mkstemp(suffix=".parquet")
    os.close(file_descriptor)
    response = None
    try:
        response = requests.get(url, timeout=120, stream=True)
        response.raise_for_status()
        with open(parquet_path, "wb") as parquet_file:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    parquet_file.write(chunk)
        yield parquet_path
    finally:
        if response is not None:
            response.close()
        if os.path.exists(parquet_path):
            os.unlink(parquet_path)


def _match_platform_dataset_entities(
    parquet_path: str,
    provision_rows: list[dict],
    comparison_fields: list[str],
    organisation_provider: str = "",
    organisation_entity: str = "",
):
    connection = duckdb.connect()
    try:
        platform_columns = [
            row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [parquet_path]
            ).fetchall()
        ]
        platform_columns_by_normalised_name = {
            str(column).strip().lower(): column for column in platform_columns
        }
        if "entity" not in platform_columns_by_normalised_name:
            return

        platform_organisation_entity_column = platform_columns_by_normalised_name.get(
            "organisation-entity"
        ) or platform_columns_by_normalised_name.get("organisation_entity")
        if organisation_entity and not platform_organisation_entity_column:
            return
        platform_organisation_column = platform_columns_by_normalised_name.get(
            "organisation"
        )
        raw_platform_organisation_expression = (
            f"cast(p.{_quote_identifier(platform_organisation_column)} AS VARCHAR)"
            if platform_organisation_column
            else "''"
        )
        platform_organisation_expression = (
            _quote_literal(organisation_provider)
            if organisation_provider
            else f"coalesce(nullif({raw_platform_organisation_expression}, ''), '')"
        )
        platform_organisation_entity_expression = (
            "trim(coalesce(cast("
            f"p.{_quote_identifier(platform_organisation_entity_column)} AS VARCHAR), "
            "''))"
            if platform_organisation_entity_column
            else "''"
        )
        comparison_column_names = [
            f"comparison_{index}" for index in range(len(comparison_fields))
        ]
        definitions = ["new_entity VARCHAR"] + [
            f"{_quote_identifier(column)} VARCHAR" for column in comparison_column_names
        ]
        connection.execute(f"CREATE TEMP TABLE provision ({', '.join(definitions)})")
        connection.executemany(
            f"INSERT INTO provision VALUES ({', '.join('?' for _ in definitions)})",
            [
                [str(row.get("entity", ""))]
                + [
                    _normalise_fingerprint_value(row.get(field, ""))
                    for field in comparison_fields
                ]
                for row in provision_rows
            ],
        )

        comparisons = []
        for field, comparison_column in zip(
            comparison_fields, comparison_column_names, strict=True
        ):
            platform_column = platform_columns_by_normalised_name.get(field)
            if field == "organisation":
                platform_expression = platform_organisation_expression
            else:
                platform_expression = (
                    f"p.{_quote_identifier(platform_column)}"
                    if platform_column
                    else "''"
                )
            comparisons.append(
                "lower(trim(coalesce(cast("
                f"{platform_expression} AS VARCHAR), ''))) = "
                f"r.{_quote_identifier(comparison_column)}"
            )

        entity_column = _quote_identifier(platform_columns_by_normalised_name["entity"])
        platform_organisation_filter = ""
        query_parameters = [parquet_path]
        if organisation_entity and platform_organisation_entity_column:
            platform_organisation_filter = (
                f"AND {platform_organisation_entity_expression} = ?"
            )
            query_parameters.append(organisation_entity)
        query = f"""
            SELECT
                p.*,
                {platform_organisation_expression} AS __normalised_organisation,
                r.new_entity AS __new_entity
            FROM read_parquet(?) AS p
            JOIN provision AS r ON {' AND '.join(comparisons)}
            WHERE trim(coalesce(cast(p.{entity_column} AS VARCHAR), '')) <> ''
              AND cast(p.{entity_column} AS VARCHAR) <> r.new_entity
              {platform_organisation_filter}
        """
        cursor = connection.execute(query, query_parameters)
        result_columns = [description[0] for description in cursor.description]
        provision_by_id = {str(row.get("entity", "")): row for row in provision_rows}
        for values in cursor.fetchall():
            result = dict(zip(result_columns, values, strict=True))
            new_entity_id = str(result.pop("__new_entity", "") or "")
            normalised_organisation = str(
                result.pop("__normalised_organisation", "") or ""
            )
            if normalised_organisation:
                result["organisation"] = normalised_organisation
            new_entity = provision_by_id.get(new_entity_id)
            if new_entity:
                yield _normalise_entity_fields(result), new_entity
    finally:
        connection.close()


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
    old_organisation = str(old_entity.get("organisation", "") or "") or (
        _organisation_identifier_for_entity(organisation_index, old_organisation_entity)
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
        "old_fields": {
            str(field): str(value or "")
            for field, value in old_entity.items()
            if field != "entity"
        },
        "new_fields": {
            str(field): str(value or "")
            for field, value in new_entity.items()
            if field != "entity"
        },
    }


def _build_non_spatial_candidate(
    *,
    old_entity: dict,
    new_entity: dict,
    dataset: str,
    redirect_lookups: dict,
) -> dict:
    old_entity_id = _normalise_entity_id(old_entity.get("entity"))
    new_entity_id = _normalise_entity_id(new_entity.get("entity"))
    candidate = {
        "old_entity": old_entity_id,
        "entity": new_entity_id,
        "dataset": dataset,
        "old_reference": str(old_entity.get("reference", "") or ""),
        "new_reference": str(new_entity.get("reference", "") or ""),
        "match_type": "all_fields_match",
        "notes": REDIRECT_NOTE,
        "old_name": str(old_entity.get("name", "") or ""),
        "new_name": str(new_entity.get("name", "") or ""),
        "old_entry_date": str(
            old_entity.get("entry-date", "") or old_entity.get("entry_date", "") or ""
        ),
        "new_entry_date": str(
            new_entity.get("entry-date", "") or new_entity.get("entry_date", "") or ""
        ),
        "old_end_date": str(
            old_entity.get("end-date", "") or old_entity.get("end_date", "") or ""
        ),
        "new_end_date": str(
            new_entity.get("end-date", "") or new_entity.get("end_date", "") or ""
        ),
        "old_organisation": str(old_entity.get("organisation", "") or ""),
        "new_organisation": str(new_entity.get("organisation", "") or ""),
        "old_organisation_entity": str(
            old_entity.get("organisation-entity", "")
            or old_entity.get("organisation_entity", "")
            or ""
        ),
        "new_organisation_entity": str(
            new_entity.get("organisation-entity", "")
            or new_entity.get("organisation_entity", "")
            or ""
        ),
        "evidence": "all comparable fields match",
        "name_similarity": "",
        "old_fields": {
            str(field): str(value or "")
            for field, value in old_entity.items()
            if field != "entity"
        },
        "new_fields": {
            str(field): str(value or "")
            for field, value in new_entity.items()
            if field != "entity"
        },
    }
    existing_redirect = redirect_lookups.get(old_entity_id)
    candidate["old_entity_redirects"] = (
        [{"old-entity": old_entity_id, **existing_redirect}]
        if existing_redirect
        else []
    )
    return candidate


def _find_non_spatial_candidates(
    dataset: str,
    provision_rows: list[dict],
    redirect_lookups: dict,
    download_platform_dataset_parquet,
    organisation_provider="",
    organisation_index=None,
) -> list[dict]:
    comparison_fields = _non_spatial_comparison_fields(provision_rows)
    if not comparison_fields:
        return []

    candidates = []
    seen = set()
    organisation_entity = _resolve_organisation_entity(
        organisation_index, organisation_provider
    )
    if organisation_provider and not organisation_entity:
        return []
    try:
        with download_platform_dataset_parquet(dataset) as parquet_path:
            matches = _match_platform_dataset_entities(
                parquet_path,
                provision_rows,
                comparison_fields,
                organisation_provider,
                organisation_entity,
            )
            for platform_row, provision_row in matches:
                old_entity_id = _normalise_entity_id(platform_row.get("entity"))
                new_entity_id = _normalise_entity_id(provision_row.get("entity"))
                key = (old_entity_id, new_entity_id)
                if (
                    not old_entity_id
                    or not new_entity_id
                    or key[0] == key[1]
                    or key in seen
                ):
                    continue
                seen.add(key)
                candidates.append(
                    _build_non_spatial_candidate(
                        old_entity=platform_row,
                        new_entity=provision_row,
                        dataset=dataset,
                        redirect_lookups=redirect_lookups,
                    )
                )
    except Exception as err:
        logger.exception(
            "Failed to compare platform dataset %s for duplicates: %s", dataset, err
        )
        raise

    return candidates


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
    redirect_lookups: dict | None = None,
    organisation_provider: str = "",
    organisation_index=None,
    fetch_platform_entities=_fetch_platform_entities,
    download_platform_dataset_parquet=_download_platform_dataset_parquet,
) -> list[dict]:
    provision_rows = _read_provision_entities(transformed_csv_path)
    if not provision_rows:
        return []

    dataset_typology = specification.get_dataset_typology(dataset)
    redirect_lookups = redirect_lookups or {}
    if dataset_typology != "geography":
        return _find_non_spatial_candidates(
            dataset,
            provision_rows,
            redirect_lookups,
            download_platform_dataset_parquet,
            organisation_provider,
            organisation_index,
        )

    if dataset != "conservation-area":
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
        candidate = _build_candidate(
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
        existing_redirect = redirect_lookups.get(key[0])
        candidate["old_entity_redirects"] = (
            [{"old-entity": key[0], **existing_redirect}] if existing_redirect else []
        )
        candidates.append(candidate)

    return candidates
