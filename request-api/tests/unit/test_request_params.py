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
        values.update(original_filename="trees.csv", uploaded_filename="upload.csv")
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
        params.update(original_filename="trees.csv", uploaded_filename="upload.csv")
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
