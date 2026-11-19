import pytest
from pydantic import ValidationError

from request_model.schemas import RequestCreate


@pytest.fixture(params=["check_url", "check_file", "add_data"])
def params(request):
    values = {
        "type": request.param,
        "dataset": "tree",
        "collection": "tree-preservation-order",
    }
    if request.param == "check_file":
        values.update(
            original_filename="trees.csv",
            uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
        )
    else:
        values["url"] = "https://example.org/trees.csv"
    return values


@pytest.mark.parametrize(
    "field",
    ["documentation_url", "licence", "start_date", "organisation", "return_endpoint"],
)
@pytest.mark.parametrize("kind", ["check_url", "check_file"])
def test_add_data_metadata_is_not_accepted_on_checks(kind, field):
    params = {"type": kind, "dataset": "tree", "collection": "tree"}
    if kind == "check_url":
        params["url"] = "https://example.org/trees.csv"
    else:
        params.update(
            original_filename="trees.csv",
            uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
        )
    with pytest.raises(ValidationError) as error:
        RequestCreate.model_validate({"params": {**params, field: None}})
    assert error.value.errors()[0]["loc"] == ("params", kind, field)


def test_openapi_declares_closed_params_for_each_request_type():
    from main import app

    schemas = app.openapi()["components"]["schemas"]
    for name in ("CheckUrlParams", "CheckFileParams", "AddDataParams"):
        assert schemas[name]["additionalProperties"] is False
    for field in (
        "documentation_url",
        "licence",
        "start_date",
        "organisation",
        "return_endpoint",
    ):
        assert field in schemas["AddDataParams"]["properties"]
        assert field not in schemas["CheckUrlParams"]["properties"]
        assert field not in schemas["CheckFileParams"]["properties"]


@pytest.mark.parametrize("field", ["name", "email", "preview", "unknown_field"])
def test_http_rejects_unknown_params_before_persisting(params, field):
    from unittest.mock import Mock, patch

    from fastapi.testclient import TestClient
    from main import app, _get_db

    db = Mock()
    with patch.dict(app.dependency_overrides, {_get_db: lambda: db}):
        response = TestClient(app, raise_server_exceptions=False).post(
            "/requests", json={"params": {**params, field: "unexpected"}}
        )
    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["type"] == "extra_forbidden"
    assert error["loc"] == ["body", "params", params["type"], field]
    db.add.assert_not_called()
    db.commit.assert_not_called()


def test_unknown_request_envelope_fields_are_rejected(params):
    with pytest.raises(ValidationError) as error:
        RequestCreate.model_validate({"params": params, "unknown_field": "unexpected"})
    assert error.value.errors()[0]["type"] == "extra_forbidden"
    assert error.value.errors()[0]["loc"] == ("unknown_field",)


UNSAFE_URLS = [
    "file:///proc/self/environ",
    "FILE:///etc/passwd",
    "ftp://example.org/trees.csv",
    "gopher://example.org/",
    "http://169.254.170.2/v2/credentials/abc",
    "http://169.254.169.254/latest/meta-data/",
    "http://2852039166/latest/meta-data/",
    "http://0xa9fea9fe/",
    "http://127.0.0.1:8000/",
    "http://127.1/",
    "http://localhost/",
    "http://api.localhost/",
    "http://10.0.0.5/",
    "http://192.168.1.1/",
    "http://172.16.0.1/",
    "http://[::1]/",
    "http://[::ffff:169.254.169.254]/",
    "http://0.0.0.0/",
    "https://example.org\\@169.254.169.254/",
    "https:///no-host",
    "trees.csv",
    "https://example.org/trees.csv\n\x00",
    "https://example.org/trees.csv%0a",
    "https://example.org/trees.csv%00.json",
    "https://example.org/%7F",
    "https://example.org/vsicurl/x/vsicurl/http://169.254.170.2/",
    "https://example.org/data?src=/vsis3/bucket/key",
    "https://example.org/data?src=/VSISPARSE/x",
    # Credentials in the URL disguise the real host.
    "https://user:pass@example.org/trees.csv",
    "https://example.org@169.254.169.254/",
    "https://" + "a" * 2048 + ".example.org/",
    # Encoded hosts that HTTP clients decode to an internal address.
    "http://%31%36%39.254.170.2/v2/credentials/abc",
    "http://169.254.170.%32/",
    "http://%6c%6f%63%61%6c%68%6f%73%74/",
    "http://[fe80::1%25eth0]/",
    "http://１６９.254.170.2/",
    "http://169。254。169。254/",
    "http://LOCALHOST。/",
]


@pytest.mark.parametrize("kind", ["check_url", "add_data"])
@pytest.mark.parametrize("url", UNSAFE_URLS)
def test_unsafe_urls_are_rejected(kind, url):
    params = {"type": kind, "dataset": "tree", "collection": "tree", "url": url}
    with pytest.raises(ValidationError, match="Invalid url"):
        RequestCreate.model_validate({"params": params})


@pytest.mark.parametrize(
    "url",
    [
        "https://example.org/trees.csv",
        "http://example.org:8080/wfs?service=WFS&typeName=trees",
        "https://8.8.8.8/data.json",
        "https://example.org/my%20trees.csv",
        "https://example.org/visitor/vsi-data/trees.csv",
        "https://example.org/arcgis/rest/services/x/FeatureServer/0/query?where=1%3D1",
        "https://example.org./trees.csv",
        "https://bücher.example/trees.csv",
        "http://[2001:4860:4860::8888]/",
    ],
)
def test_public_http_urls_are_accepted(url):
    params = {"type": "check_url", "dataset": "tree", "collection": "tree", "url": url}
    RequestCreate.model_validate({"params": params})


def test_add_data_without_url_is_accepted():
    params = {
        "type": "add_data",
        "dataset": "tree",
        "collection": "tree",
        "resource": "a" * 64,
    }
    RequestCreate.model_validate({"params": params})


def _post_counting_metrics(body):
    from unittest.mock import Mock, patch

    import sentry_sdk
    from fastapi.testclient import TestClient
    from main import app, _get_db

    db = Mock()
    counted = []
    with patch.dict(app.dependency_overrides, {_get_db: lambda: db}), patch.object(
        sentry_sdk.metrics,
        "count",
        side_effect=lambda *args, **kwargs: counted.append((args, kwargs)),
    ):
        response = TestClient(app, raise_server_exceptions=False).post(
            "/requests", json=body
        )
    db.add.assert_not_called()
    return response, counted


def test_unsafe_url_is_refused_and_counted():
    params = {
        "type": "check_url",
        "dataset": "tree",
        "collection": "tree",
        "url": "http://169.254.170.2/v2/credentials/abc",
    }
    response, counted = _post_counting_metrics({"params": params})

    assert response.status_code == 422
    error = response.json()["detail"][0]
    assert error["type"] == "unsafe_url"
    assert error["msg"].startswith("Invalid url:")
    assert counted == [
        (
            ("async.security.rejected", 1),
            {
                "attributes": {
                    "service": "request-api",
                    "layer": "url",
                    "source": "api",
                }
            },
        )
    ]


def test_other_validation_errors_are_not_counted():
    params = {
        "type": "check_url",
        "dataset": "tree",
        "collection": "tree",
        "url": "https://example.org/trees.csv",
        "unknown_field": "x",
    }
    response, counted = _post_counting_metrics({"params": params})

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "extra_forbidden"
    assert counted == []


UPLOAD_ID = "492f15d8-45e4-427e-bde0-f60d69889f40"


def _check_file(**overrides):
    params = {
        "type": "check_file",
        "dataset": "tree",
        "collection": "tree",
        "original_filename": "trees.csv",
        "uploaded_filename": UPLOAD_ID,
    }
    return {"params": {**params, **overrides}}


@pytest.mark.parametrize(
    "uploaded_filename",
    [
        "../" + UPLOAD_ID,
        UPLOAD_ID + "/../../etc/passwd",
        "../../tmp/example.txt",
        "/etc/passwd",
        UPLOAD_ID + ".csv",
        "trees.csv",
        "",
    ],
)
def test_check_file_rejects_uploaded_filename_that_is_not_an_upload_id(
    uploaded_filename,
):
    with pytest.raises(ValidationError, match="Invalid uploaded_filename"):
        RequestCreate.model_validate(_check_file(uploaded_filename=uploaded_filename))


@pytest.mark.parametrize("original_filename", ["", "   ", "a" * 256])
def test_check_file_rejects_bad_original_filename(original_filename):
    with pytest.raises(ValidationError, match="Invalid original_filename"):
        RequestCreate.model_validate(_check_file(original_filename=original_filename))


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {"uploaded_filename": UPLOAD_ID.upper()},
        {"original_filename": "Tree Preservation Orders (2024) v2.geojson"},
        {"original_filename": "a" * 255},
    ],
)
def test_check_file_accepts_submit_uploads(overrides):
    RequestCreate.model_validate(_check_file(**overrides))


def test_upload_name_that_is_not_an_upload_id_is_counted():
    response, counted = _post_counting_metrics(
        _check_file(uploaded_filename="../../tmp/example.txt")
    )

    assert response.status_code == 422
    assert response.json()["detail"][0]["type"] == "unsafe_uploaded_filename"
    assert counted == [
        (
            ("async.security.rejected", 1),
            {
                "attributes": {
                    "service": "request-api",
                    "layer": "upload_name",
                    "source": "api",
                }
            },
        )
    ]


def test_bad_original_filename_is_not_counted():
    response, counted = _post_counting_metrics(_check_file(original_filename=""))

    assert response.status_code == 422
    assert counted == []
