from src.application.core.utils import (
    get_request,
    check_content,
    validate_endpoint,
    validate_source,
)
import requests_mock
import os
import csv
from src.application.core import utils


def test_get_request():
    # Mocked URL and response
    url = "http://example.com"
    response_content = b"Mocked content"

    # Mock requests
    with requests_mock.Mocker() as m:
        # Mock the GET request to the URL
        m.get(url, content=response_content)

        # Call the function
        log, content = get_request(url)

        # Assertions
        assert log["status"] == "200"
        assert log["message"] == ""
        assert content == response_content


def test_check_content():
    content_layers = '{"layers": [{"id": 1}, {"id": 2}]}'
    assert not check_content(content_layers)

    content_empty = '{"data": []}'
    assert check_content(content_empty)

    content_xml = "<xml><data></data></xml>"
    assert check_content(content_xml)

    content_str = "This is a plain text string."
    assert check_content(content_str)


def test_hash_sha256():
    value = "test"
    assert (
        utils.hash_sha256(value)
        == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    )


def test_hash_md5():
    value = "test"
    assert utils.hash_md5(value) == "098f6bcd4621d373cade4e832627b4f6"


def test_save_content_saves_data_to_file(tmp_path):
    data = b"hello world"
    tmp_dir = tmp_path
    resource = utils.save_content(data, tmp_dir)
    path = os.path.join(tmp_dir, resource)
    assert os.path.exists(path)
    with open(path, "rb") as f:
        assert f.read() == data


def test_detect_encoding(tmp_path):
    file = tmp_path / "test.csv"
    file.write_text("hello,world\n", encoding="utf-8")
    encoding = utils.detect_encoding(str(file))
    assert encoding.lower() in ("utf-8", "ascii")


def test_detect_encoding_break(tmp_path):
    file = tmp_path / "utf16.csv"
    file.write_bytes(
        b"\xff\xfeh\x00e\x00l\x00l\x00o\x00,\x00w\x00o\x00r\x00l\x00d\x00\n\x00"
    )
    encoding = utils.detect_encoding(str(file))
    assert encoding.lower().startswith("utf-16")


def test_extract_dataset_field_rows(tmp_path):
    folder = tmp_path
    csv_file = folder / "dataset-field.csv"
    rows = [
        {"dataset": "test", "field": "name"},
        {"dataset": "other", "field": "ref"},
    ]
    with open(csv_file, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["dataset", "field"])
        writer.writeheader()
        for row in rows:
            writer.writerow(row)
    result = utils.extract_dataset_field_rows(str(folder), "test")
    assert result == [rows[0]]


def test_append_endpoint(tmp_path):
    endpoint_csv = tmp_path / "endpoint.csv"
    endpoint_url = "http://example.com"
    endpoint_key, new_row = utils.append_endpoint(str(endpoint_csv), endpoint_url)
    assert new_row["endpoint-url"] == endpoint_url


def test_append_source(tmp_path):
    source_csv = tmp_path / "source.csv"
    endpoint_key = "endpointkey"
    source_key, source_row = utils.append_source(
        str(source_csv), "coll", "org", endpoint_key
    )
    assert source_row["endpoint"] == endpoint_key


def test_append_endpoint_existing(tmp_path):
    endpoint_csv = tmp_path / "endpoint.csv"
    endpoint_url = "http://example.com"
    # Write an existing row to the CSV
    fieldnames = [
        "endpoint",
        "endpoint-url",
        "parameters",
        "plugin",
        "entry-date",
        "start-date",
        "end-date",
    ]
    existing_row = {
        "endpoint": utils.hash_sha256(endpoint_url),
        "endpoint-url": endpoint_url,
        "parameters": "",
        "plugin": "",
        "entry-date": "2024-01-01T00:00:00",
        "start-date": "",
        "end-date": "",
    }
    with open(endpoint_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(existing_row)

    _, new_row = utils.append_endpoint(str(endpoint_csv), endpoint_url)
    assert new_row is None
    with open(endpoint_csv, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        assert reader[0]["endpoint-url"] == endpoint_url


def test_append_source_existing(tmp_path):
    import csv

    source_csv = tmp_path / "source.csv"
    collection = "coll"
    organisation = "org"
    endpoint_key = "endpointkey"
    source_key = utils.hash_md5(f"{collection}|{organisation}|{endpoint_key}")

    fieldnames = [
        "source",
        "attribution",
        "collection",
        "documentation-url",
        "endpoint",
        "licence",
        "organisation",
        "pipelines",
        "entry-date",
        "start-date",
        "end-date",
    ]
    existing_row = {
        "source": source_key,
        "attribution": "",
        "collection": collection,
        "documentation-url": "",
        "endpoint": endpoint_key,
        "licence": "",
        "organisation": organisation,
        "pipelines": "",
        "entry-date": "2024-01-01T00:00:00",
        "start-date": "",
        "end-date": "",
    }
    with open(source_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerow(existing_row)

    _, new_row = utils.append_source(
        str(source_csv), collection, organisation, endpoint_key
    )
    assert new_row is None
    with open(source_csv, newline="", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 1
        assert reader[0]["source"] == source_key


def test_validate_endpoint_creates_file(monkeypatch, tmp_path):
    """Test that validate_endpoint creates endpoint.csv if it doesn't exist"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date, plugin=None
    ):
        return "endpoint_hash", {
            "endpoint": "endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": plugin or "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.utils.append_endpoint", fake_append_endpoint
    )

    assert not endpoint_csv_path.exists()

    validate_endpoint(url, str(pipeline_dir), plugin=None)

    assert endpoint_csv_path.exists()

    with open(endpoint_csv_path, "r", encoding="utf-8") as f:
        reader = csv.reader(f)
        headers = next(reader)
        assert headers == [
            "endpoint",
            "endpoint-url",
            "parameters",
            "plugin",
            "entry-date",
            "start-date",
            "end-date",
        ]


def test_validate_endpoint_appends(monkeypatch, tmp_path):
    """Test that validate_endpoint appends new endpoint when URL not found"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/new-endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"

    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "endpoint",
                "endpoint-url",
                "parameters",
                "plugin",
                "entry-date",
                "start-date",
                "end-date",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": "existing_hash",
                "endpoint-url": "http://example.com/existing",
                "parameters": "",
                "plugin": "",
                "entry-date": "2024-01-01T00:00:00",
                "start-date": "2024-01-01",
                "end-date": "",
            }
        )

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date, plugin=None
    ):
        return "new_endpoint_hash", {
            "endpoint": "new_endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": plugin or "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr(
        "src.application.core.utils.append_endpoint", fake_append_endpoint
    )

    result = validate_endpoint(url, str(pipeline_dir), plugin=None)

    assert result["endpoint_url_in_endpoint_csv"] is False
    assert "new_endpoint_entry" in result
    assert result["new_endpoint_entry"]["endpoint"] == "new_endpoint_hash"
    assert result["new_endpoint_entry"]["endpoint-url"] == url


def test_validate_endpoint_finds_existing(monkeypatch, tmp_path):
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"
    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    with open(endpoint_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "endpoint",
                "endpoint-url",
                "parameters",
                "plugin",
                "entry-date",
                "start-date",
                "end-date",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "endpoint": "endpoint_hash",
                "endpoint-url": url,
                "parameters": "",
                "plugin": "",
                "entry-date": "2024-01-01",
                "start-date": "2024-01-01",
                "end-date": "",
            }
        )

    monkeypatch.setattr(
        "src.application.core.utils.append_endpoint",
        lambda *a, **kw: (_ for _ in ()).throw(Exception("Should not be called")),
    )

    result = validate_endpoint(url, str(pipeline_dir), plugin=None)
    assert result["endpoint_url_in_endpoint_csv"] is True
    assert "existing_endpoint_entry" in result
    assert result["existing_endpoint_entry"]["endpoint-url"] == url


def test_validate_endpoint_empty_url(monkeypatch, tmp_path):
    """Test validate_endpoint with empty URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    result = validate_endpoint("", str(pipeline_dir), plugin=None)

    assert result == {}


def test_validate_endpoint_csv_read_error(monkeypatch, tmp_path):
    """Test validate_endpoint when reading CSV fails"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    url = "http://example.com/endpoint"

    endpoint_csv_path = pipeline_dir / "endpoint.csv"
    endpoint_csv_path.write_bytes(b"\x00\x00\x00")

    def fake_append_endpoint(
        endpoint_csv_path, endpoint_url, entry_date, start_date, end_date, plugin=None
    ):
        return "endpoint_hash", {
            "endpoint": "endpoint_hash",
            "endpoint-url": endpoint_url,
            "parameters": "",
            "plugin": plugin or "",
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }


def test_validate_source_creates_new_source(monkeypatch, tmp_path):
    """Test validate_source creates new source entry when it doesn't exist"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    def fake_append_source(
        source_csv_path,
        collection,
        organisation,
        endpoint_key,
        attribution,
        documentation_url,
        licence,
        pipelines,
        entry_date,
        start_date,
        end_date,
    ):
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr("src.application.core.utils.append_source", fake_append_source)

    result = validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
        start_date=None,
        licence=None,
    )

    assert result["documentation_url_in_source_csv"] is False
    assert "new_source_entry" in result
    assert result["new_source_entry"]["source"] == "source_hash_456"
    assert result["new_source_entry"]["collection"] == collection
    assert result["new_source_entry"]["organisation"] == organisation
    assert result["new_source_entry"]["pipelines"] == dataset


def test_validate_source_finds_existing_source(monkeypatch, tmp_path):
    """Test validate_source finds existing source entry"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    with open(source_csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "source",
                "attribution",
                "collection",
                "documentation-url",
                "endpoint",
                "licence",
                "organisation",
                "pipelines",
                "entry-date",
                "start-date",
                "end-date",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "source": "existing_source_hash",
                "attribution": "",
                "collection": collection,
                "documentation-url": documentation_url,
                "endpoint": "endpoint_hash_123",
                "licence": "",
                "organisation": organisation,
                "pipelines": dataset,
                "entry-date": "2024-01-01T00:00:00",
                "start-date": "2024-01-01",
                "end-date": "",
            }
        )

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr("src.application.core.utils.append_source", fake_append_source)

    result = validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
        start_date=None,
        licence=None,
    )

    assert result["documentation_url_in_source_csv"] is True
    assert "existing_source_entry" in result
    assert result["existing_source_entry"]["source"] == "existing_source_hash"
    assert result["existing_source_entry"]["collection"] == collection


def test_validate_source_no_endpoint_key(tmp_path):
    """Test validate_source returns empty dict when no endpoint key available"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {}

    result = validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
        start_date=None,
        licence=None,
    )

    assert result == {}


def test_validate_source_empty_documentation_url(monkeypatch, tmp_path):
    """Test validate_source handles empty documentation URL"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = ""
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    def fake_append_source(
        source_csv_path,
        collection,
        organisation,
        endpoint_key,
        attribution,
        documentation_url,
        licence,
        pipelines,
        entry_date,
        start_date,
        end_date,
    ):
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr("src.application.core.utils.append_source", fake_append_source)

    result = validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
        start_date=None,
        licence=None,
    )

    assert "documentation_url_in_source_csv" in result
    assert result["new_source_entry"]["documentation-url"] == ""


def test_validate_source_uses_new_endpoint_entry(monkeypatch, tmp_path):
    """Test validate_source uses endpoint from new_endpoint_entry when available"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": False,
        "new_endpoint_entry": {"endpoint": "new_endpoint_hash_789"},
    }

    captured_endpoint_key = None

    def fake_append_source(
        source_csv_path,
        collection,
        organisation,
        endpoint_key,
        attribution,
        documentation_url,
        licence,
        pipelines,
        entry_date,
        start_date,
        end_date,
    ):
        nonlocal captured_endpoint_key
        captured_endpoint_key = endpoint_key
        return "source_hash_456", {
            "source": "source_hash_456",
            "attribution": attribution,
            "collection": collection,
            "documentation-url": documentation_url,
            "endpoint": endpoint_key,
            "licence": licence,
            "organisation": organisation,
            "pipelines": pipelines,
            "entry-date": entry_date,
            "start-date": start_date,
            "end-date": end_date,
        }

    monkeypatch.setattr("src.application.core.utils.append_source", fake_append_source)

    result = validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
        start_date=None,
        licence=None,
    )

    assert captured_endpoint_key == "new_endpoint_hash_789"
    assert result["new_source_entry"]["endpoint"] == "new_endpoint_hash_789"


def test_validate_source_handles_csv_read_error(monkeypatch, tmp_path):
    """Test validate_source handles CSV read errors gracefully"""
    pipeline_dir = tmp_path / "pipeline"
    pipeline_dir.mkdir()
    source_csv_path = pipeline_dir / "source.csv"

    documentation_url = "http://example.com/doc"
    collection = "test-collection"
    organisation = "test-org"
    dataset = "test-dataset"

    endpoint_summary = {
        "endpoint_url_in_endpoint_csv": True,
        "existing_endpoint_entry": {"endpoint": "endpoint_hash_123"},
    }

    source_csv_path.write_bytes(b"\x00\x00\x00")

    def fake_append_source(*args, **kwargs):
        return "existing_source_hash", None

    monkeypatch.setattr("src.application.core.utils.append_source", fake_append_source)

    result = validate_source(
        documentation_url,
        str(pipeline_dir),
        collection,
        organisation,
        dataset,
        endpoint_summary,
        start_date=None,
        licence=None,
    )

    assert "documentation_url_in_source_csv" in result
