from unittest.mock import Mock, patch
import base64
import time
from datetime import datetime

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import main
from auth import require_auth
from request_model import models


@pytest.fixture
def signing_key(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    monkeypatch.setenv("AUTH_JWT_PUBLIC_KEY", base64.b64encode(public).decode())
    monkeypatch.setenv("AUTH_TRUSTED_ISSUERS", "manage-test")
    monkeypatch.setenv("AUTH_JWT_AUDIENCE", "async-test")
    return key


def claims():
    return {
        "iss": "manage-test",
        "aud": "async-test",
        "sub": "123",
        "auth_provider": "github",
        "role": "staff",
        "iat": int(time.time()),
        "exp": int(time.time()) + 900,
    }


@pytest.mark.parametrize(
    "trusted,issuer,status",
    [
        ("manage,submit", "manage", 200),
        ("manage,submit", "submit", 200),
        (" manage, , submit, ", "submit", 200),
        ("manage,submit", "other", 401),
        ("manage,submit", "man", 401),
        ("manage,submit", "manage,submit", 401),
        ("", "manage", 503),
        (" , , ", "manage", 503),
    ],
)
def test_trusted_issuer_allowlist(signing_key, monkeypatch, trusted, issuer, status):
    monkeypatch.setenv("AUTH_TRUSTED_ISSUERS", trusted)
    app = FastAPI()

    @app.get("/protected")
    @require_auth(roles={"staff"})
    async def protected(http_request: Request):
        return {"issuer": http_request.state.principal["iss"]}

    payload = claims()
    payload["iss"] = issuer
    token = jwt.encode(payload, signing_key, algorithm="RS256")
    response = TestClient(app).get(
        "/protected", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == status


@pytest.fixture
def api():
    main.app.dependency_overrides[main._get_db] = lambda: Mock()
    with patch("main.crud.create_request") as create, patch(
        "main.AddDataTask.apply_async"
    ) as queue:
        yield TestClient(main.app), create, queue
    main.app.dependency_overrides.clear()


def body(kind="add_data"):
    return {
        "params": {
            "type": kind,
            "dataset": "tree",
            "collection": "tree",
            "url": "https://example.com/data.csv",
        }
    }


@pytest.mark.parametrize("role", ["staff", "admin"])
def test_valid_token_creates_add_data(api, signing_key, role):
    client, create, queue = api
    payload = claims()
    payload["role"] = role
    create.return_value = models.Request(
        id="test",
        type="add_data",
        status="NEW",
        params=body()["params"],
        created=datetime.now(),
        modified=datetime.now(),
        response=None,
    )
    response = client.post(
        "/requests",
        json=body(),
        headers={
            "Authorization": "Bearer "
            + jwt.encode(payload, signing_key, algorithm="RS256")
        },
    )
    assert response.status_code == 202
    assert response.json()["id"] == "test"
    queue.assert_called_once()


@pytest.mark.parametrize(
    "field,value,status",
    [
        ("iss", "other", 401),
        ("aud", "other", 401),
        ("exp", 1, 401),
        ("iat", 9999999999, 401),
        ("sub", "", 401),
        ("sub", 123, 401),
        ("role", "viewer", 403),
        ("role", ["admin"], 403),
    ],
)
def test_rejects_invalid_claims(api, signing_key, field, value, status):
    client, create, queue = api
    payload = claims()
    payload[field] = value
    response = client.post(
        "/requests",
        json=body(),
        headers={
            "Authorization": "Bearer "
            + jwt.encode(payload, signing_key, algorithm="RS256")
        },
    )
    assert response.status_code == status
    create.assert_not_called()
    queue.assert_not_called()


@pytest.mark.parametrize(
    "field", [name for name in claims() if name != "auth_provider"]
)
def test_requires_all_claims(api, signing_key, field):
    client, create, queue = api
    payload = claims()
    del payload[field]
    response = client.post(
        "/requests",
        json=body(),
        headers={
            "Authorization": "Bearer "
            + jwt.encode(payload, signing_key, algorithm="RS256")
        },
    )
    assert response.status_code == 401
    create.assert_not_called()
    queue.assert_not_called()


@pytest.mark.parametrize("provider", [None, "another-provider"])
def test_auth_provider_is_optional_metadata(signing_key, provider):
    app = FastAPI()

    @app.get("/protected")
    @require_auth(roles={"staff"})
    async def protected(http_request: Request):
        return {"actor": http_request.state.principal["sub"]}

    payload = claims()
    if provider is None:
        payload.pop("auth_provider")
    else:
        payload["auth_provider"] = provider
    token = jwt.encode(payload, signing_key, algorithm="RS256")
    response = TestClient(app).get(
        "/protected", headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 200
    assert response.json() == {"actor": "123"}


@pytest.mark.parametrize("kind", ["malformed", "wrong-key", "unsigned"])
def test_rejects_invalid_signature(api, signing_key, kind):
    client, create, queue = api
    if kind == "wrong-key":
        other_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        token = jwt.encode(claims(), other_key, algorithm="RS256")
    elif kind == "unsigned":
        token = jwt.encode(claims(), None, algorithm="none")
    else:
        token = "invalid"
    response = client.post(
        "/requests", json=body(), headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 401
    create.assert_not_called()
    queue.assert_not_called()


@pytest.mark.parametrize("kind", ["check_file", "check_url"])
def test_checks_remain_public(api, kind):
    client, create, _ = api
    payload = body(kind)
    if kind == "check_file":
        payload["params"].pop("url")
        payload["params"].update(original_filename="a.csv", uploaded_filename="b.csv")
    create.return_value = models.Request(
        id="test",
        type=kind,
        status="NEW",
        params=payload["params"],
        created=datetime.now(),
        modified=datetime.now(),
        response=None,
    )
    with patch("main.CheckDataFileTask.apply_async"), patch(
        "main.CheckDataUrlTask.apply_async"
    ):
        response = client.post("/requests", json=payload)
    assert response.status_code == 202


def test_missing_verification_key_fails_closed(api, signing_key, monkeypatch):
    client, create, _ = api
    monkeypatch.delenv("AUTH_JWT_PUBLIC_KEY")
    token = jwt.encode(claims(), signing_key, algorithm="RS256")
    response = client.post(
        "/requests", json=body(), headers={"Authorization": f"Bearer {token}"}
    )
    assert response.status_code == 503
    create.assert_not_called()


def test_unknown_operation_is_rejected_before_execution(api):
    client, create, queue = api
    response = client.post("/requests", json=body("unknown"))
    assert response.status_code == 422
    create.assert_not_called()
    queue.assert_not_called()


def test_async_decorator_exposes_verified_actor(signing_key):
    app = FastAPI()

    @app.get("/protected")
    @require_auth(roles={"staff"})
    async def protected(http_request: Request):
        return {"actor": http_request.state.principal["sub"]}

    client = TestClient(app)
    assert client.get("/protected").status_code == 401
    token = jwt.encode(claims(), signing_key, algorithm="RS256")
    response = client.get("/protected", headers={"Authorization": f"Bearer {token}"})
    assert response.json() == {"actor": "123"}


@pytest.mark.parametrize("service", [None, "manage", "provide"])
def test_add_data_requires_auth_before_creating_request(service):
    main.app.dependency_overrides[main._get_db] = lambda: Mock()
    try:
        with patch("main.crud.create_request") as create:
            response = TestClient(main.app, raise_server_exceptions=False).post(
                "/requests",
                json={
                    "params": {
                        "type": "add_data",
                        "dataset": "tree",
                        "collection": "tree",
                        "service": service,
                    }
                },
            )
        assert response.status_code == 401
        create.assert_not_called()
    finally:
        main.app.dependency_overrides.clear()
