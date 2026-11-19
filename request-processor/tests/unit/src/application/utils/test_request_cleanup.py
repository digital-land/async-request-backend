import pytest

from application.utils import request_cleanup
from application.utils.request_cleanup import clean_up, clean_up_request_files

VALID_ID = "AbCdEfGhIjKlMnOpQrStUv"


@pytest.fixture
def collection(tmp_path, monkeypatch):
    collection_dir = tmp_path / "collection"
    resource_dir = collection_dir / "resource"
    (resource_dir / VALID_ID).mkdir(parents=True)
    (resource_dir / VALID_ID / "file.csv").write_text("x")
    (resource_dir / "other-request").mkdir()
    (resource_dir / "other-request" / "keep.csv").write_text("x")
    (collection_dir / "keep.csv").write_text("x")
    monkeypatch.setattr(
        request_cleanup.Directories, "COLLECTION_DIR", str(collection_dir)
    )
    return collection_dir, resource_dir


@pytest.mark.parametrize("request_id", ["..", "../..", "/", "/tmp"])
def test_clean_up_request_files_refuses_paths_outside_resource_dir(
    collection, request_id
):
    collection_dir, resource_dir = collection
    clean_up_request_files(request_id)
    assert (collection_dir / "keep.csv").exists()
    assert (resource_dir / "other-request" / "keep.csv").exists()
    assert (resource_dir / VALID_ID / "file.csv").exists()


def test_clean_up_request_files_removes_request_dir(collection):
    _, resource_dir = collection
    clean_up_request_files(VALID_ID)
    assert not (resource_dir / VALID_ID).exists()
    assert (resource_dir / "other-request" / "keep.csv").exists()


def test_clean_up_request_files_refuses_symlinked_request_dir(collection):
    _, resource_dir = collection
    link_id = "B" * 22
    (resource_dir / link_id).symlink_to(resource_dir / "other-request")
    clean_up_request_files(link_id)
    assert (resource_dir / "other-request" / "keep.csv").exists()


def test_clean_up_request_files_does_not_follow_nested_symlinks(collection):
    _, resource_dir = collection
    (resource_dir / VALID_ID / "link").symlink_to(resource_dir / "other-request")
    clean_up_request_files(VALID_ID)
    assert not (resource_dir / VALID_ID).exists()
    assert (resource_dir / "other-request" / "keep.csv").exists()


@pytest.mark.parametrize("request_id", ["..", "../..", "/"])
def test_clean_up_refuses_parent_directories(tmp_path, request_id):
    base = tmp_path / "resource"
    (base / "sub").mkdir(parents=True)
    (base / "sub" / "keep.csv").write_text("x")
    (tmp_path / "keep.csv").write_text("x")
    clean_up(request_id, str(base), str(base / "sub"))
    assert (tmp_path / "keep.csv").exists()
    assert (base / "sub" / "keep.csv").exists()


def test_clean_up_removes_request_dir(tmp_path):
    base = tmp_path / "resource"
    (base / VALID_ID).mkdir(parents=True)
    (base / VALID_ID / "file.csv").write_text("x")
    (base / "keep.csv").write_text("x")
    clean_up(VALID_ID, str(base))
    assert not (base / VALID_ID).exists()
    assert (base / "keep.csv").exists()


def test_clean_up_refuses_symlinked_request_dir(tmp_path):
    base = tmp_path / "resource"
    (base / "other").mkdir(parents=True)
    (base / "other" / "keep.csv").write_text("x")
    (base / VALID_ID).symlink_to(base / "other")
    clean_up(VALID_ID, str(base))
    assert (base / "other" / "keep.csv").exists()


def test_clean_up_does_not_follow_nested_symlinks(tmp_path):
    base = tmp_path / "resource"
    outside = tmp_path / "outside"
    (outside / VALID_ID).mkdir(parents=True)
    (outside / VALID_ID / "keep.csv").write_text("x")
    (base / VALID_ID).mkdir(parents=True)
    (base / VALID_ID / "link").symlink_to(outside)
    clean_up(VALID_ID, str(base))
    assert (outside / VALID_ID / "keep.csv").exists()
    assert not (base / VALID_ID).exists()
