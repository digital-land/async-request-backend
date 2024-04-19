from src.application.core.utils import get_request
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
