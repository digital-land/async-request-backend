"""
Rejects fetched or uploaded files that would make GDAL read other locations.

A URL can pass every check in request_model.url_safety and still serve a file
that tells GDAL to read somewhere else. GDAL picks a driver from a file's
contents, not its name, so a file called boundary.gml can be an OGR VRT or a
GDALG pipeline that reads a local file, or a /vsicurl/ URL on an internal
address. ogr2ogr would then return that data as rows of the converted CSV.

digital-land's converter also skips the riskiest drivers and blocks network
access. This check runs before it as a second layer: it does not depend on the
digital-land or GDAL version in the image (GDALG, for example, only exists from
GDAL 3.11), and it gives the user a clear error.
"""

import re
import zipfile

# Documents that GDAL opens as a pointer to another data source.
_POINTER_DOCUMENT = re.compile(
    rb"<\s*(?:OGRVRTDataSource|VRTDataset|OGRWFSDataSource|GDAL_WMS|GDAL_WMTS"
    rb"|WCS_GDAL)\b"
    rb"|gdal_streamed_alg",
    re.IGNORECASE,
)
# GDAL virtual file system paths, e.g. /vsicurl/, /vsis3/, /vsisparse/.
_GDAL_VIRTUAL_PATH = re.compile(rb"/vsi[a-z0-9_]*/", re.IGNORECASE)
_PATTERNS = (_POINTER_DOCUMENT, _GDAL_VIRTUAL_PATH)

_CHUNK_SIZE = 1024 * 1024
# Longer than any match, so a marker split across two chunks is still found.
_OVERLAP = 64


class UnsafeResourceError(ValueError):
    """The fetched file must not be passed to GDAL."""


def _stream_contains_marker(stream):
    tail = b""
    while True:
        chunk = stream.read(_CHUNK_SIZE)
        if not chunk:
            return False
        window = tail + chunk
        if any(pattern.search(window) for pattern in _PATTERNS):
            return True
        tail = window[-_OVERLAP:]


def check_resource_is_safe(path):
    """
    Raise UnsafeResourceError if the file at path, or any member of it when it
    is a zip, contains a GDAL pointer document or virtual file system path.
    """
    with open(path, "rb") as stream:
        if _stream_contains_marker(stream):
            raise UnsafeResourceError(
                "File refers GDAL to another data source, which is not allowed"
            )

    if zipfile.is_zipfile(path):
        with zipfile.ZipFile(path) as archive:
            for member in archive.infolist():
                if member.is_dir():
                    continue
                try:
                    with archive.open(member) as stream:
                        found = _stream_contains_marker(stream)
                except (RuntimeError, NotImplementedError, zipfile.BadZipFile):
                    # Encrypted, unsupported or corrupt: GDAL cannot read it
                    # either, so conversion will fail on its own.
                    continue
                if found:
                    raise UnsafeResourceError(
                        f"Zip member {member.filename!r} refers GDAL to "
                        "another data source, which is not allowed"
                    )
