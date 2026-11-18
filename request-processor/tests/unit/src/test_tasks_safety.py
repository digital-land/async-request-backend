from types import SimpleNamespace

import pytest
import requests

from src import tasks

VALID_ID = "AbCdEfGhIjKlMnOpQrStUv"
UPLOAD_ID = "492f15d8-45e4-427e-bde0-f60d69889f40"

VRT_POINTING_ELSEWHERE = (
    b"<OGRVRTDataSource><OGRVRTLayer name='x'><SrcDataSource>"
    b"/vsicurl/http://169.254.170.2/v2/credentials/abc"
    b"</SrcDataSource></OGRVRTLayer></OGRVRTDataSource>"
)
UPLOADED_VRT_READING_S3 = (
    b"<OGRVRTDataSource><OGRVRTLayer name='x'><SrcDataSource>"
    b"/vsisparse//vsis3/bucket/492f15d8-45e4-427e-bde0-f60d69889f41"
    b"</SrcDataSource></OGRVRTLayer></OGRVRTDataSource>"
)


def _fake_fetch_writing(content):
    def fake_fetch(self, url, **kwargs):
        (self.resource_dir / "resource").write_bytes(content)
        return tasks.FetchStatus.OK, {"status": "200"}

    return fake_fetch


def _fake_s3_download(content):
    def download(bucket, key, path, max_size):
        with open(path, "wb") as f:
            f.write(content)

    return download


def test_fetch_resource_refuses_gdal_pointer_file(tmp_path, monkeypatch):
    # A public https URL serving a VRT named .gml.
    monkeypatch.setattr(
        tasks.Collector, "fetch", _fake_fetch_writing(VRT_POINTING_ELSEWHERE)
    )
    with pytest.raises(tasks.CustomException) as raised:
        tasks._fetch_resource(tmp_path, "https://example.org/boundary.gml")
    assert raised.value.detail["exceptionType"] == "UnsafeResourceError"
    assert "another data source" in raised.value.detail["errMsgDetail"]


def test_fetch_resource_accepts_ordinary_file(tmp_path, monkeypatch):
    content = b'{"type": "FeatureCollection", "features": []}'
    monkeypatch.setattr(tasks.Collector, "fetch", _fake_fetch_writing(content))
    file_name, log = tasks._fetch_resource(tmp_path, "https://example.org/a.json")
    assert file_name == "resource"
    assert log["plugin"] is None


@pytest.mark.parametrize(
    "content",
    [
        UPLOADED_VRT_READING_S3,
        b'{"type": "gdal_streamed_alg", "command_line": "gdal vector pipeline '
        b'! read /proc/self/environ ! write -f stream streamed_dataset"}',
    ],
)
def test_check_file_refuses_upload_that_points_gdal_elsewhere(
    tmp_path, monkeypatch, content
):
    saved = []
    monkeypatch.setenv("REQUEST_FILES_BUCKET_NAME", "bucket")
    monkeypatch.setattr(
        tasks.s3_transfer_manager,
        "download_with_default_configuration",
        _fake_s3_download(content),
    )
    monkeypatch.setattr(
        tasks, "save_response_to_db", lambda request_id, log: saved.append(log)
    )
    request_data = SimpleNamespace(uploaded_filename=UPLOAD_ID)

    with pytest.raises(tasks.CustomException):
        tasks.handle_check_file(SimpleNamespace(id=VALID_ID), request_data, tmp_path)

    assert saved[0]["exception_type"] == "UnsafeResourceError"
    assert saved[0]["message"] == "The uploaded file is not supported"


def test_check_file_accepts_ordinary_upload(tmp_path, monkeypatch):
    monkeypatch.setenv("REQUEST_FILES_BUCKET_NAME", "bucket")
    monkeypatch.setattr(
        tasks.s3_transfer_manager,
        "download_with_default_configuration",
        _fake_s3_download(b"reference,name\nTPO1,Oak\n"),
    )
    request_data = SimpleNamespace(uploaded_filename=UPLOAD_ID)
    assert (
        tasks.handle_check_file(SimpleNamespace(id=VALID_ID), request_data, tmp_path)
        == UPLOAD_ID
    )


@pytest.mark.parametrize(
    "url", ["file:///proc/self/environ", "http://169.254.169.254/latest/meta-data/"]
)
def test_fetch_resource_refuses_unsafe_urls(tmp_path, url):
    with pytest.raises(tasks.CustomException) as raised:
        tasks._fetch_resource(tmp_path, url)
    assert "not allowed" in str(raised.value)


def test_fetch_resource_removes_file_adapter(tmp_path, monkeypatch):
    seen = {}

    def fake_fetch(self, url, **kwargs):
        seen["adapters"] = list(self.session.adapters)
        # A redirect to file: would be resolved by the same session.
        with pytest.raises(requests.exceptions.InvalidSchema):
            self.session.get("file:///etc/hosts")
        return tasks.FetchStatus.FAILED, {"status": "404"}

    monkeypatch.setattr(tasks.Collector, "fetch", fake_fetch)
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(tmp_path, "https://example.org/data.csv")
    assert "file:" not in seen["adapters"]


def test_fetch_resource_reports_why_url_was_refused(tmp_path):
    with pytest.raises(tasks.CustomException) as raised:
        tasks._fetch_resource(tmp_path, "file:///etc/passwd")
    assert raised.value.detail["exceptionType"] == "UnsafeUrlError"
    assert raised.value.detail["errMsgDetail"] == "Only http and https URLs are allowed"


def test_fetch_resource_checks_dns_for_paged_wfs(tmp_path, monkeypatch):
    def refuse(url):
        raise tasks.UnsafeUrlError("resolves to a non-public address")

    monkeypatch.setattr(tasks, "check_host_resolves_publicly", refuse)
    with pytest.raises(tasks.CustomException):
        tasks._fetch_resource(
            tmp_path, "https://wfs.example.org/wfs", {"page_size": 100}
        )
