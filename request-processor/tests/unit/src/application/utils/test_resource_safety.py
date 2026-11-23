import io
import shutil
import zipfile

import pytest

from application.utils import resource_safety
from application.utils.resource_safety import (
    UnsafeResourceError,
    check_resource_is_safe,
)


def _vrt_reading(source):
    return (
        "<OGRVRTDataSource><OGRVRTLayer name='secret'>"
        f"<SrcDataSource>{source}</SrcDataSource>"
        "</OGRVRTLayer></OGRVRTDataSource>"
    ).encode()


def _gdalg_reading(source):
    return (
        '{"type": "gdal_streamed_alg", "command_line": '
        f'"gdal vector pipeline ! read {source} ! write -f stream streamed_dataset"}}'
    ).encode()


GEOJSON = (
    b'{"type": "FeatureCollection", "features": [{"type": "Feature", '
    b'"properties": {"reference": "TPO1"}, '
    b'"geometry": {"type": "Point", "coordinates": [0.1, 51.5]}}]}'
)


def _zip_with(tmp_path, name, content):
    path = tmp_path / "resource.zip"
    # Compressed, so the member's text is not visible in the zip's own bytes.
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(name, content)
    return path


@pytest.mark.parametrize(
    "content",
    [
        _vrt_reading("/proc/self/environ"),
        _vrt_reading("/vsicurl/http://169.254.170.2/v2/credentials/abc"),
        _gdalg_reading("/proc/self/environ"),
        b"<?xml version='1.0'?>\n< ogrvrtdatasource>",
        b"<OGRWFSDataSource><URL>http://169.254.169.254/</URL></OGRWFSDataSource>",
        b"<GDAL_WMS><Service name='TMS'/></GDAL_WMS>",
        b"<VRTDataset rasterXSize='1' rasterYSize='1'/>",
        b"field_1\n/vsisparse//vsis3/bucket/key\n",
        b'{"type": "FeatureCollection", "x": "/VSIS3/bucket/key"}',
    ],
)
def test_rejects_files_that_point_gdal_elsewhere(tmp_path, content):
    path = tmp_path / "resource"
    path.write_bytes(content)
    with pytest.raises(UnsafeResourceError):
        check_resource_is_safe(path)


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


def test_rejects_pointer_inside_zip(tmp_path):
    path = _zip_with(tmp_path, "boundary.gml", _vrt_reading("/proc/self/environ"))
    assert b"OGRVRTDataSource" not in path.read_bytes()
    with pytest.raises(UnsafeResourceError, match="boundary.gml"):
        check_resource_is_safe(path)


def test_accepts_ordinary_zip(tmp_path):
    check_resource_is_safe(_zip_with(tmp_path, "trees.geojson", GEOJSON))


def test_skips_zip_members_it_cannot_read(tmp_path, monkeypatch):
    path = _zip_with(tmp_path, "trees.geojson", GEOJSON)

    def encrypted(*args, **kwargs):
        raise RuntimeError("File is encrypted, password required for extraction")

    monkeypatch.setattr(zipfile.ZipFile, "open", encrypted)
    check_resource_is_safe(path)


def test_finds_marker_split_across_chunks(tmp_path, monkeypatch):
    monkeypatch.setattr(resource_safety, "_CHUNK_SIZE", 8)
    path = tmp_path / "resource"
    path.write_bytes(b"x" * 13 + b"/vsicurl/http://169.254.170.2/")
    with pytest.raises(UnsafeResourceError):
        check_resource_is_safe(path)


def test_marker_stream_reads_whole_input():
    stream = io.BytesIO(b"a" * (3 * 1024 * 1024) + b"<OGRVRTDataSource>")
    assert resource_safety._stream_contains_marker(stream)


@pytest.mark.skipif(shutil.which("ogr2ogr") is None, reason="needs GDAL")
@pytest.mark.parametrize("payload", [_vrt_reading, _gdalg_reading])
def test_payloads_this_rejects_are_ones_gdal_would_act_on(tmp_path, payload):
    """
    The rejected payloads are real: opened directly, ogr2ogr returns another
    file's contents. (GDALG needs GDAL 3.11 or later.)
    """
    import subprocess

    secret = tmp_path / "secret.csv"
    secret.write_text("field_1\nTOP-SECRET-VALUE\n")
    path = tmp_path / "boundary.gml"
    path.write_bytes(payload(secret))
    output = tmp_path / "out.csv"

    with pytest.raises(UnsafeResourceError):
        check_resource_is_safe(path)

    subprocess.run(
        ["ogr2ogr", "-f", "CSV", str(output), str(path)], capture_output=True
    )
    # Older GDAL (e.g. distro packages) may not open the payload under this
    # file name, or not support its driver at all.
    if not output.exists() or "TOP-SECRET-VALUE" not in output.read_text():
        pytest.skip("this GDAL does not act on the payload")
