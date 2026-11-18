import base64
import time
from unittest.mock import Mock, patch

import jwt
import pytest
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

import main
from auth import require_auth


@pytest.fixture
def client_tokens(monkeypatch):
    keys = {
        issuer: rsa.generate_private_key(public_exponent=65537, key_size=2048)
        for issuer in ("manage", "submit")
    }
    for issuer, key in keys.items():
        monkeypatch.setenv(
            f"{issuer.upper()}_JWT_PUBLIC_KEY",
            base64.b64encode(
                key.public_key().public_bytes(
                    serialization.Encoding.PEM,
                    serialization.PublicFormat.SubjectPublicKeyInfo,
                )
            ).decode(),
        )
    monkeypatch.setenv("AUTH_JWT_AUDIENCE", "async")
    monkeypatch.setenv("AUTHENTICATION_ON", "true")

    def token(issuer="submit", role="submit", signer=None, **overrides):
        payload = dict(
            iss=issuer,
            aud="async",
            sub=issuer,
            role=role,
            iat=int(time.time()),
            exp=int(time.time()) + 900,
        )
        payload.update(overrides)
        return {
            "Authorization": "Bearer "
            + jwt.encode(payload, keys[signer or issuer], algorithm="RS256")
        }

    return token


@pytest.mark.parametrize("issuer,role", [("manage", "staff"), ("submit", "submit")])
def test_each_issuer_uses_its_own_key(client_tokens, issuer, role):
    app = FastAPI()

    @app.get("/")
    @require_auth(roles={"staff", "submit"})
    def protected(http_request: Request):
        return http_request.state.principal["iss"]

    response = TestClient(app).get("/", headers=client_tokens(issuer, role))
    assert response.status_code == 200
    assert response.json() == issuer


@pytest.mark.parametrize(
    "issuer,role,signer,status",
    [
        ("manage", "staff", "submit", 401),
        ("submit", "staff", "submit", 403),
        ("submit", "admin", "submit", 403),
        ("submit", "submit", "manage", 401),
    ],
)
def test_key_and_role_are_bound_to_issuer(client_tokens, issuer, role, signer, status):
    app = FastAPI()

    @app.get("/")
    @require_auth(roles={"staff", "admin", "submit"})
    def protected(http_request: Request):
        return {}

    response = TestClient(app).get("/", headers=client_tokens(issuer, role, signer))
    assert response.status_code == status


@pytest.mark.parametrize(
    "path",
    [
        "/requests/id",
        "/requests/id/response-details",
        "/requests/id/geometries",
    ],
)
def test_processing_reads_require_token(path):
    with patch.dict(main.app.dependency_overrides, {main._get_db: lambda: Mock()}):
        response = TestClient(main.app, raise_server_exceptions=False).get(path)
    assert response.status_code == 401


@pytest.mark.parametrize(
    "params,link",
    [
        ({"type": "add_data", "dataset": "tree", "collection": "tree"}, None),
        (
            {
                "type": "check_url",
                "url": "https://example.org",
                "dataset": "tree",
                "collection": "tree",
            },
            "id",
        ),
    ],
)
def test_submit_cannot_run_staff_operations(client_tokens, params, link):
    with patch.dict(
        main.app.dependency_overrides, {main._get_db: lambda: Mock()}
    ), patch("main.crud.create_request") as create:
        response = TestClient(main.app, raise_server_exceptions=False).post(
            "/requests",
            json={"params": params, "add_data_submission_id": link},
            headers=client_tokens(),
        )
    assert response.status_code == 403
    create.assert_not_called()


@pytest.mark.parametrize("path", ["/docs", "/redoc", "/openapi.json"])
def test_documentation_is_not_public(path):
    assert TestClient(main.app).get(path).status_code == 404
