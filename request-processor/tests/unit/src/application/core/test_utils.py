from src.application.core.utils import get_request, check_content
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
