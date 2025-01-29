from src.application.core.utils import get_request, check_content
import requests_mock


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
