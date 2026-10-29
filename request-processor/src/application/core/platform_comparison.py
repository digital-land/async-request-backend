"""Compare transformed rows with one published platform snapshot in the async job."""

import csv
import os
import re
from concurrent.futures import ThreadPoolExecutor

import duckdb
from shapely import wkt

from application.core.duplicates import (
    _download_platform_dataset_parquet,
    _normalise_entity_id,
    _quote_identifier,
    _resolve_organisation_entity,
)
from application.logging.logger import get_logger

logger = get_logger(__name__)

_ENTITY_COL_EXCLUDE = {
    "prefix",
    "typology",
    "organisation-entity",
    "organisation",
    "end-date",
    "entry-date",
    "dataset",
}
_GEO_FIELDS = {"geometry", "point"}
_DATE_PREFIX_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})[T ]")
_CHANGED_VALUE_MAX_LEN = 200
_GEO_TOLERANCE = 1e-4
_PLATFORM_ENTITY_LIMIT = 15000


def _normalise_field_value(value) -> str:
    if value is None:
        return ""
    s = str(value).strip()
    if not s:
        return ""
    # Platform values are often typed (100 vs "100.0" from the pipeline).
    try:
        f = float(s)
        if f == int(f):
            return str(int(f))
    except (ValueError, OverflowError):
        pass
    # Platform datetimes vs pipeline dates: compare on the date part only.
    m = _DATE_PREFIX_RE.match(s)
    if m:
        return m.group(1)
    return s


def _geometries_differ(res_wkt: str, plat_wkt: str) -> bool:
    """
    True when two WKT geometries represent meaningfully different shapes.

    Uses Hausdorff distance so that reprocessing artefacts (coordinate
    precision, vertex ordering, POINT vs MULTIPOINT wrapping) are ignored,
    while a genuine move of a boundary or point is detected.
    """
    try:
        g1 = wkt.loads(res_wkt)
        g2 = wkt.loads(plat_wkt)
    except Exception:
        # Unparseable — fall back to an exact text comparison.
        return str(res_wkt).strip() != str(plat_wkt).strip()
    if g1.is_empty or g2.is_empty:
        return g1.is_empty != g2.is_empty
    try:
        return g1.hausdorff_distance(g2) > _GEO_TOLERANCE
    except Exception:
        return not g1.equals(g2)


def _diff_entity_fields(resource_fields: dict, platform_entity: dict) -> dict:
    """
    Return {column: platform_value} for fields whose resource value differs
    from the platform entity's value. Only fields the resource provided are
    compared. Geometry/point are compared by shape (see _geometries_differ)
    rather than raw WKT, since platform geometry is reprocessed and never
    matches the submitted text.
    """
    changed = {}
    for col, res_val in resource_fields.items():
        if col == "entity" or col in _ENTITY_COL_EXCLUDE:
            continue
        plat_val = platform_entity.get(col)
        if col in _GEO_FIELDS:
            res_has = bool(str(res_val or "").strip())
            plat_has = bool(str(plat_val or "").strip())
            if res_has != plat_has:
                changed[col] = "on platform" if plat_has else "(no value on platform)"
            elif res_has and plat_has and _geometries_differ(res_val, plat_val):
                changed[col] = "(different geometry on platform)"
            continue
        if _normalise_field_value(res_val) != _normalise_field_value(plat_val):
            changed[col] = str(
                plat_val if plat_val not in (None, "") else "(no value on platform)"
            )[:_CHANGED_VALUE_MAX_LEN]
    return changed


def _resource_entries(path):
    """Preserve entry numbers and repeated entity IDs, unlike the dedup pivot."""
    entries = {}
    with open(path, newline="", encoding="utf-8-sig") as stream:
        for fact in csv.DictReader(stream):
            entry_number = str(fact.get("entry-number", ""))
            entity = _normalise_entity_id(fact.get("entity"))
            if not entry_number or not entity:
                continue
            entry = entries.setdefault(entry_number, {"entity": entity, "fields": {}})
            if entry["entity"] != entity:
                raise ValueError("A transformed entry contains different entity IDs")
            field = fact.get("field")
            if field:
                entry["fields"][field] = fact.get("value") or ""
    return dict(sorted(entries.items(), key=lambda item: int(item[0])))


def _platform_rows(parquet_path, organisation_entity):
    """Filter the snapshot using the same authoritative organisation scope as Manage."""
    with duckdb.connect() as connection:
        names = {
            str(row[0]).lower().replace("_", "-"): row[0]
            for row in connection.execute(
                "DESCRIBE SELECT * FROM read_parquet(?)", [parquet_path]
            ).fetchall()
        }
        if not {"entity", "organisation-entity", "quality"} <= names.keys():
            raise ValueError(
                "Platform snapshot lacks entity, organisation or quality columns"
            )
        organisation = _quote_identifier(names["organisation-entity"])
        quality = _quote_identifier(names["quality"])
        scope = (
            f"try_cast({organisation} AS BIGINT) = ? AND {quality} = 'authoritative'"
        )
        params = [parquet_path, int(organisation_entity)]
        count = connection.execute(
            f"SELECT count(*) FROM read_parquet(?) WHERE {scope}", params
        ).fetchone()[0]
        if count > _PLATFORM_ENTITY_LIMIT:
            return count, []
        cursor = connection.execute(
            f"SELECT * FROM read_parquet(?) WHERE {scope}", params
        )
        columns = [column[0].lower().replace("_", "-") for column in cursor.description]
        rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    return count, [
        {
            key: (value.isoformat() if hasattr(value, "isoformat") else value)
            for key, value in row.items()
        }
        for row in rows
    ]


def prepare_platform_comparison(
    dataset,
    transformed_csv_path,
    organisation_provider,
    organisation_index,
    download_platform_dataset_parquet=None,
):
    """Return a small summary and separately persisted/paginated comparison rows.

    Failure disables the comparison; it must never turn an incomplete platform
    snapshot into an empty platform or prevent the resource's transform rendering.
    """
    summary = {
        "version": 1,
        "status": "unavailable",
        "existing_count": 0,
        "row_count": 0,
    }
    try:
        if not os.path.exists(transformed_csv_path):
            return summary, []
        organisation_entity = _resolve_organisation_entity(
            organisation_index, organisation_provider
        )
        if not str(organisation_entity).isdigit():
            return summary, []
        entries = _resource_entries(transformed_csv_path)
        download = (
            download_platform_dataset_parquet or _download_platform_dataset_parquet
        )
        with download(dataset) as parquet_path:
            if parquet_path is None:
                return summary, []
            count, platform_rows = _platform_rows(parquet_path, organisation_entity)
        summary["existing_count"] = count
        if count > _PLATFORM_ENTITY_LIMIT:
            return {**summary, "status": "too-large"}, []
        platform_by_id = {
            _normalise_entity_id(row.get("entity")): row for row in platform_rows
        }
        if len(platform_by_id) != count or "" in platform_by_id:
            raise ValueError("Platform snapshot has missing or repeated entity IDs")
        entry_numbers = list(entries)

        def compare(entry_number):
            entry = entries[entry_number]
            entity = entry["entity"]
            on_platform = entity in platform_by_id
            changed = (
                _diff_entity_fields(entry["fields"], platform_by_id[entity])
                if on_platform
                else {}
            )
            return {
                "source": "resource",
                "entry_number": entry_number,
                "entity": entity,
                "category": (
                    ("changed" if changed else "in_both") if on_platform else "new"
                ),
                "changed_fields": changed,
            }

        if entry_numbers:
            with ThreadPoolExecutor(max_workers=min(4, len(entry_numbers))) as executor:
                rows = list(executor.map(compare, entry_numbers))
        else:
            rows = []
        resource_ids = {entry["entity"] for entry in entries.values()}
        rows.extend(
            {
                "source": "platform",
                "entity": entity,
                "fields": {
                    key: str(value) if value is not None else ""
                    for key, value in fields.items()
                },
                "category": "existing",
                "changed_fields": {},
            }
            for entity, fields in platform_by_id.items()
            if entity not in resource_ids
        )
        # The entity table keeps the last resource entry for each entity. Map
        # colours retain per-entry comparisons for repeated IDs.
        table_rows = {row["entity"]: row for row in rows}
        counts = {category: 0 for category in ("new", "changed", "in_both", "existing")}
        for row in table_rows.values():
            counts[row["category"]] += 1
        return {
            **summary,
            "status": "complete",
            "row_count": len(rows),
            "counts": counts,
            "columns": sorted(
                {field for row in platform_rows for field in row}
                | {field for entry in entries.values() for field in entry["fields"]}
            ),
        }, rows
    except Exception:
        logger.exception("Platform comparison unavailable for dataset %s", dataset)
        return summary, []
