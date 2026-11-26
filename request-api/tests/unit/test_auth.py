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
from auth import AuthenticationConfigError, authentication_disabled, require_auth
from request_model import models


@pytest.fixture
def signing_key(monkeypatch):
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    public = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    monkeypatch.setenv("MANAGE_JWT_PUBLIC_KEY", base64.b64encode(public).decode())
    monkeypatch.setenv("AUTH_JWT_AUDIENCE", "async-test")
    return key


def claims():
    return {
        "iss": "manage",
        "aud": "async-test",
        "sub": "123",
        "auth_provider": "github",
        "role": "staff",
        "iat": int(time.time()),
        "exp": int(time.time()) + 900,
    }


@pytest.mark.parametrize(
    "issuer,status",
    [
        ("manage", 200),
        ("other", 401),
        ("man", 401),
        ("manage,submit", 401),
        (" manage ", 401),
    ],
)
def test_issuer_matches_exactly(signing_key, issuer, status):
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
def test_checks_require_auth(api, kind):
    client, create, _ = api
    payload = body(kind)
    if kind == "check_file":
        payload["params"].pop("url")
        payload["params"].update(
            original_filename="a.csv",
            uploaded_filename="492f15d8-45e4-427e-bde0-f60d69889f40",
        )
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
    assert response.status_code == 401
    create.assert_not_called()


def test_missing_verification_key_fails_closed(api, signing_key, monkeypatch):
    client, create, _ = api
    monkeypatch.delenv("MANAGE_JWT_PUBLIC_KEY")
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


@pytest.mark.parametrize("setting", [None, "true", "invalid", "", "false", "FALSE"])
@pytest.mark.parametrize("async_handler", [False, True])
def test_explicit_local_auth_opt_out(monkeypatch, setting, async_handler):
    if setting is None:
        monkeypatch.delenv("AUTHENTICATION_ON", raising=False)
    else:
        monkeypatch.setenv("AUTHENTICATION_ON", setting)
    app = FastAPI()

    if async_handler:

        @app.get("/protected")
        @require_auth(roles={"staff"})
        async def protected(http_request: Request):
            return {"ok": True}

    else:

        @app.get("/protected")
        @require_auth(roles={"staff"})
        def protected(http_request: Request):
            return {"ok": True}

    response = TestClient(app).get("/protected")
    expected = 200 if setting in ("false", "FALSE") else 401
    assert response.status_code == expected


def test_local_add_data_without_token_is_queued(api, monkeypatch):
    monkeypatch.setenv("AUTHENTICATION_ON", "false")
    client, create, queue = api
    create.return_value = models.Request(
        id="local-test",
        type="add_data",
        status="NEW",
        params=body()["params"],
        created=datetime.now(),
        modified=datetime.now(),
        response=None,
    )
    response = client.post("/requests", json=body())
    assert response.status_code == 202
    create.assert_called_once()
    queue.assert_called_once()


@pytest.fixture
def auth_rejections(monkeypatch):
    import auth

    counted = []

    def count(name, value, unit=None, attributes=None):
        counted.append((name, attributes))

    monkeypatch.setattr(auth.sentry_sdk.metrics, "count", count)
    return counted


def _signed(signing_key, **changes):
    payload = claims()
    payload.update(changes)
    return jwt.encode(payload, signing_key, algorithm="RS256")


@pytest.mark.parametrize(
    "header, env_change, reason, issuer",
    [
        (lambda key: None, None, "missing", "unknown"),
        (lambda key: "Bearer invalid", None, "invalid", "unknown"),
        (lambda key: "Bearer " + _signed(key, iss="other"), None, "invalid", "unknown"),
        (lambda key: "Bearer " + _signed(key, exp=1), None, "invalid", "manage"),
        (lambda key: "Bearer " + _signed(key, role="viewer"), None, "role", "manage"),
        (
            lambda key: "Bearer " + _signed(key),
            "MANAGE_JWT_PUBLIC_KEY",
            "unavailable",
            "manage",
        ),
    ],
)
def test_auth_rejections_are_counted(
    api, signing_key, monkeypatch, auth_rejections, header, env_change, reason, issuer
):
    client, _, _ = api
    if env_change:
        monkeypatch.delenv(env_change)
    value = header(signing_key)
    headers = {"Authorization": value} if value else {}
    response = client.post("/requests", json=body(), headers=headers)
    assert response.status_code in (401, 403, 503)
    assert auth_rejections == [
        ("async.auth.rejected", {"reason": reason, "issuer": issuer})
    ]


def test_accepted_token_is_not_counted(api, signing_key, auth_rejections):
    client, create, _ = api
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
        headers={"Authorization": "Bearer " + _signed(signing_key)},
    )
    assert response.status_code == 202
    assert auth_rejections == []


@pytest.mark.parametrize(
    "environment",
    [None, "", "development", "staging", "production", " Production ", "prod"],
)
def test_auth_cannot_be_switched_off_outside_local(monkeypatch, environment):
    # Fails closed: a missing, misspelt or unknown environment counts as deployed.
    monkeypatch.setenv("AUTHENTICATION_ON", "false")
    if environment is None:
        monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
    else:
        monkeypatch.setenv("SENTRY_ENVIRONMENT", environment)
    with pytest.raises(AuthenticationConfigError):
        authentication_disabled()

    app = FastAPI()

    @app.get("/protected")
    @require_auth(roles={"staff"})
    def protected(http_request: Request):
        return {"ok": True}

    # A protected route fails rather than serving without authentication.
    response = TestClient(app, raise_server_exceptions=False).get("/protected")
    assert response.status_code == 500


@pytest.mark.parametrize("environment", ["local", "docker-compose-local", " Local "])
def test_auth_can_be_switched_off_locally(monkeypatch, environment):
    monkeypatch.setenv("AUTHENTICATION_ON", "false")
    monkeypatch.setenv("SENTRY_ENVIRONMENT", environment)
    assert authentication_disabled() is True


@pytest.mark.parametrize("environment", [None, "staging", "production"])
def test_auth_left_on_is_fine_anywhere(monkeypatch, environment):
    monkeypatch.delenv("AUTHENTICATION_ON", raising=False)
    if environment is None:
        monkeypatch.delenv("SENTRY_ENVIRONMENT", raising=False)
    else:
        monkeypatch.setenv("SENTRY_ENVIRONMENT", environment)
    assert authentication_disabled() is False


def test_api_refuses_to_start_with_auth_off_in_production():
    # In a separate interpreter, so the app other tests use is not reloaded.
    import os
    import subprocess
    import sys

    env = {
        **os.environ,
        "AUTHENTICATION_ON": "false",
        "SENTRY_ENVIRONMENT": "production",
        "PYTHONPATH": os.pathsep.join(sys.path),
    }
    result = subprocess.run(
        [sys.executable, "-c", "import main"], env=env, capture_output=True, text=True
    )
    assert result.returncode != 0
    assert "AuthenticationConfigError" in result.stderr
