import shutil
import sqlite3
import subprocess
import zipfile

import pytest

from application.security import resource_safety
from application.security.resource_safety import (
    UnsafeResourceError,
    check_resource_is_safe,
)

GEOJSON = (
    b'{"type": "FeatureCollection", "features": [{"type": "Feature", '
    b'"properties": {"reference": "TPO1"}, '
    b'"geometry": {"type": "Point", "coordinates": [0.1, 51.5]}}]}'
)


def _zip_with(tmp_path, name, content):
    path = tmp_path / "resource.zip"
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, content)
    return path


def _sqlite_with(path, definition, name="x"):
    """
    A SQLite file whose schema holds the given table definition. It is written
    straight into the schema table, as a crafted file would be, so the module
    it names does not need to exist here. name must match the definition's
    table name, or SQLite treats the schema as malformed.
    """
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE t(a)")
    connection.execute("PRAGMA writable_schema=1")
    connection.execute(
        "INSERT INTO sqlite_master VALUES('table', ?, ?, 0, ?)",
        (name, name, definition),
    )
    connection.commit()
    connection.close()
    return path


def _geopackage_like(path):
    """A database with the spatial index table a real GeoPackage has."""
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE t(fid INTEGER PRIMARY KEY, geom BLOB)")
    connection.execute(
        'CREATE VIRTUAL TABLE "rtree_t_geom" ' "USING rtree(id, minx, maxx, miny, maxy)"
    )
    connection.commit()
    connection.close()
    return path


@pytest.mark.parametrize(
    "content",
    [
        GEOJSON,
        b"reference,name\nTPO1,Visitor centre\n",
        b"<gml:FeatureCollection xmlns:gml='http://www.opengis.net/gml'/>",
    ],
)
def test_accepts_ordinary_files(tmp_path, content):
    path = tmp_path / "resource"
    path.write_bytes(content)
    check_resource_is_safe(path)


def test_accepts_ordinary_zip(tmp_path):
    check_resource_is_safe(_zip_with(tmp_path, "trees.geojson", GEOJSON))


@pytest.mark.parametrize(
    "name, definition",
    [
        ("x", "CREATE VIRTUAL TABLE x USING VirtualOGR('/etc/hostname')"),
        (
            "x",
            "CREATE VIRTUAL TABLE x USING VirtualText('/etc/hostname', 'UTF-8', 0, "
            "POINT, DOUBLEQUOTE, ',')",
        ),
        ("x", "CREATE VIRTUAL TABLE x USING VirtualShape('/tmp/a', 'UTF-8', 4326)"),
        ("x", "create   virtual table x using virtualogr('/etc/hostname')"),
        ("x", "CREATE/**/VIRTUAL TABLE x USING/**/VirtualOGR('/etc/hostname')"),
        # Decoy text that names an allowed module must not hide the real one.
        (
            "y using rtree(id)",
            'CREATE VIRTUAL TABLE "y using rtree(id)" '
            "USING VirtualOGR('/etc/hostname')",
        ),
        (
            "x",
            "CREATE VIRTUAL TABLE x /* USING rtree(id) */ "
            "USING VirtualOGR('/etc/hostname')",
        ),
    ],
)
def test_rejects_sqlite_with_unsafe_virtual_table(tmp_path, name, definition):
    path = _sqlite_with(tmp_path / "resource.gpkg", definition, name)
    with pytest.raises(UnsafeResourceError, match="virtual table"):
        check_resource_is_safe(path)


def test_accepts_geopackage_spatial_index(tmp_path):
    check_resource_is_safe(_geopackage_like(tmp_path / "resource.gpkg"))


def test_accepts_sqlite_without_virtual_tables(tmp_path):
    path = tmp_path / "resource.sqlite"
    connection = sqlite3.connect(path)
    connection.execute("CREATE TABLE trees(reference TEXT, is_virtual_tour TEXT)")
    connection.commit()
    connection.close()
    check_resource_is_safe(path)


def test_rejects_sqlite_whose_schema_cannot_be_read(tmp_path):
    path = _sqlite_with(tmp_path / "resource.gpkg", "CREATE TABLE (((")
    with pytest.raises(UnsafeResourceError, match="could not be checked"):
        check_resource_is_safe(path)


def test_rejects_sqlite_inside_zip_under_another_name(tmp_path):
    database = _sqlite_with(
        tmp_path / "db", "CREATE VIRTUAL TABLE x USING VirtualOGR('/etc/hostname')"
    )
    path = _zip_with(tmp_path, "boundary.gml", database.read_bytes())
    with pytest.raises(UnsafeResourceError, match="boundary.gml.*virtual table"):
        check_resource_is_safe(path)


def test_accepts_geopackage_inside_zip(tmp_path):
    database = _geopackage_like(tmp_path / "db")
    check_resource_is_safe(_zip_with(tmp_path, "boundary.gpkg", database.read_bytes()))


def test_skips_zip_members_it_cannot_read(tmp_path, monkeypatch):
    database = _sqlite_with(
        tmp_path / "db", "CREATE VIRTUAL TABLE x USING VirtualOGR('/etc/hostname')"
    )
    path = _zip_with(tmp_path, "boundary.gml", database.read_bytes())

    def encrypted(*args, **kwargs):
        raise RuntimeError("File is encrypted, password required for extraction")

    monkeypatch.setattr(zipfile.ZipFile, "open", encrypted)
    check_resource_is_safe(path)


def test_rejects_zip_that_expands_too_far(tmp_path, monkeypatch):
    monkeypatch.setattr(resource_safety, "MAX_EXPANDED_BYTES", 1000)
    path = _zip_with(tmp_path, "trees.csv", b"reference\n" + b"0" * 2000)
    with pytest.raises(UnsafeResourceError, match="expands to more than"):
        check_resource_is_safe(path)


def test_rejects_zip_with_too_many_members(tmp_path, monkeypatch):
    monkeypatch.setattr(resource_safety, "MAX_ARCHIVE_MEMBERS", 2)
    path = tmp_path / "resource.zip"
    with zipfile.ZipFile(path, "w") as archive:
        for index in range(3):
            archive.writestr(f"layer{index}.geojson", GEOJSON)
    with pytest.raises(UnsafeResourceError, match="more than 2 members"):
        check_resource_is_safe(path)


def test_leaves_no_temporary_files(tmp_path, monkeypatch):
    monkeypatch.setattr(resource_safety.tempfile, "tempdir", str(tmp_path / "tmp"))
    (tmp_path / "tmp").mkdir()
    database = _geopackage_like(tmp_path / "db")
    check_resource_is_safe(_zip_with(tmp_path, "boundary.gpkg", database.read_bytes()))
    assert list((tmp_path / "tmp").iterdir()) == []


@pytest.mark.skipif(shutil.which("ogr2ogr") is None, reason="needs GDAL")
def test_sqlite_payload_this_rejects_is_one_gdal_would_act_on(tmp_path):
    """
    The rejected database is real: converted as digital-land converts a
    GeoPackage, ogr2ogr returns another file's contents.
    """
    secret = tmp_path / "secret.csv"
    secret.write_text("field_1\nTOP-SECRET-VALUE\n")
    # Named like a saved resource, without an extension: a .gpkg name makes
    # GDAL use the GeoPackage driver, which wants GeoPackage metadata tables.
    path = _sqlite_with(
        tmp_path / "0f3a9c",
        f"CREATE VIRTUAL TABLE x USING VirtualOGR('{secret}')",
    )
    output = tmp_path / "out" / "out.csv"
    output.parent.mkdir()

    with pytest.raises(UnsafeResourceError):
        check_resource_is_safe(path)

    subprocess.run(
        ["ogr2ogr", "-f", "CSV", "-nln", "MERGED", str(output), str(path)],
        capture_output=True,
    )
    # A GDAL without VirtualOGR, or that refuses it, does not act on it.
    if not output.exists() or "TOP-SECRET-VALUE" not in output.read_text():
        pytest.skip("this GDAL does not act on the payload")
