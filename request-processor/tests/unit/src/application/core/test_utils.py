from src.application.core.utils import get_request, check_content
import requests_mock
import os
import tempfile
import csv
import pytest
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

def test_hash_sha256_and_md5():
    value = "test"
    assert utils.hash_sha256(value) == "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
    assert utils.hash_md5(value) == "098f6bcd4621d373cade4e832627b4f6"


def test_save_and_save_content(tmp_path):
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

def test_append_endpoint_and_source(tmp_path):
    endpoint_csv = tmp_path / "endpoint.csv"
    source_csv = tmp_path / "source.csv"
    endpoint_url = "http://example.com"
    endpoint_key, new_row = utils.append_endpoint(str(endpoint_csv), endpoint_url)
    assert new_row["endpoint-url"] == endpoint_url
    source_key, source_row = utils.append_source(
        str(source_csv), "coll", "org", endpoint_key
    )
    assert source_row["endpoint"] == endpoint_key
