"""
Rejects fetched or uploaded files that are unsafe to convert: SQLite files,
including zip members, with virtual tables other than spatial indexes, and zip
archives that expand too far.
"""

import os
import re
import shutil
import sqlite3
import tempfile
import zipfile
from urllib.parse import quote

# How far a zip may expand in total, and how many members it may have. A 30MB
# upload could otherwise decompress to tens of gigabytes.
MAX_EXPANDED_BYTES = 1024 * 1024 * 1024
MAX_ARCHIVE_MEMBERS = 10_000

_CHUNK_SIZE = 1024 * 1024
_SQLITE_MAGIC = b"SQLite format 3\x00"

# Any schema entry that mentions VIRTUAL must be one of the spatial index
# tables GeoPackage and SpatiaLite create, written in the plain form they use.
# Matching the whole definition, rather than picking out the module name,
# means decoy text in a table name or comment cannot hide the real module.
_VIRTUAL_KEYWORD = re.compile(r"\bVIRTUAL\b", re.IGNORECASE)
_NAME = r'(?:"[^"]*"|`[^`]*`|\[[^\]]*\]|\w+)'
_SAFE_VIRTUAL_TABLE = re.compile(
    r"\s*CREATE\s+VIRTUAL\s+TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"(?:{_NAME}\s*\.\s*)?{_NAME}\s+USING\s+"
    r"(?:rtree|rtree_i32|VirtualSpatialIndex|VirtualElementary|VirtualKNN2?)"
    r'\s*(?:\(\s*[\w\s,"]*\))?\s*;?\s*',
    re.IGNORECASE,
)


class UnsafeResourceError(ValueError):
    """The fetched file must not be passed to GDAL."""


def _subject(member):
    return "File" if member is None else f"Zip member {member!r}"


def _check_sqlite(path, member=None):
    uri = f"file:{quote(os.fspath(path))}?mode=ro&immutable=1"
    try:
        connection = sqlite3.connect(uri, uri=True)
        try:
            rows = connection.execute(
                "SELECT sql FROM sqlite_master WHERE sql IS NOT NULL"
            ).fetchall()
        finally:
            connection.close()
    except sqlite3.Error as e:
        raise UnsafeResourceError(
            f"{_subject(member)} is a SQLite database that could not be checked"
        ) from e

    for (sql,) in rows:
        if _VIRTUAL_KEYWORD.search(sql) and not _SAFE_VIRTUAL_TABLE.fullmatch(sql):
            raise UnsafeResourceError(
                f"{_subject(member)} contains a SQLite virtual table, "
                "which is not allowed"
            )


def _check_expansion(infos):
    if len(infos) > MAX_ARCHIVE_MEMBERS:
        raise UnsafeResourceError(
            f"Zip has more than {MAX_ARCHIVE_MEMBERS} members, which is not allowed"
        )
    # zipfile never returns more than a member's declared size, so the
    # declared sizes bound what reading the members can expand to.
    if sum(info.file_size for info in infos) > MAX_EXPANDED_BYTES:
        raise UnsafeResourceError(
            f"Zip expands to more than {MAX_EXPANDED_BYTES // 1024 // 1024}MB "
            "when decompressed, which is not allowed"
        )


def _check_member(archive, info, tmp_dir):
    try:
        stream = archive.open(info)
    except (RuntimeError, NotImplementedError, zipfile.BadZipFile):
        # Encrypted, unsupported or corrupt: GDAL cannot read it either, so
        # conversion fails on its own.
        return
    with stream:
        try:
            head = stream.read(len(_SQLITE_MAGIC))
        except zipfile.BadZipFile:
            return
        if not head.startswith(_SQLITE_MAGIC):
            return
        # sqlite3 needs a file to open.
        fd, path = tempfile.mkstemp(dir=tmp_dir)
        try:
            with os.fdopen(fd, "wb") as out:
                out.write(head)
                shutil.copyfileobj(stream, out, _CHUNK_SIZE)
            _check_sqlite(path, info.filename)
        finally:
            os.unlink(path)


def _check_zip(path):
    try:
        archive = zipfile.ZipFile(path)
    except (zipfile.BadZipFile, ValueError):
        # GDAL cannot open it as a zip either.
        return
    with archive, tempfile.TemporaryDirectory(prefix="resource-safety-") as tmp_dir:
        infos = [info for info in archive.infolist() if not info.is_dir()]
        _check_expansion(infos)
        for info in infos:
            _check_member(archive, info, tmp_dir)


def check_resource_is_safe(path):
    """
    Raise UnsafeResourceError if the file at path, or a member of it when it is
    a zip, is a SQLite database with an unsafe virtual table, or if it is a zip
    that expands too far.
    """
    with open(path, "rb") as stream:
        head = stream.read(len(_SQLITE_MAGIC))
    if head.startswith(_SQLITE_MAGIC):
        _check_sqlite(path)
    if zipfile.is_zipfile(path):
        _check_zip(path)
