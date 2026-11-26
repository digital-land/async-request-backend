"""Verify client JWTs with issuer-specific keys and role limits."""

import base64
import binascii
import inspect
import os
from functools import wraps

import jwt
import sentry_sdk
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from fastapi import HTTPException

AUTH_REJECTED_METRIC = "async.auth.rejected"
_KNOWN_ISSUERS = {"manage", "submit"}
_AUTH_OPTIONAL_ENVIRONMENTS = {"local", "docker-compose-local"}


def _rejected(error, reason, issuer=None):
    """
    Count a refused request and return the error to raise.

    reason: "missing" (no bearer token), "invalid" (bad, expired or unknown
        token), "role" (valid token without a permitted role) or "unavailable"
        (verification is misconfigured, so every request fails).
    The issuer is only recorded when it is one we know, so a forged token
    can't add new values.
    """
    sentry_sdk.metrics.count(
        AUTH_REJECTED_METRIC,
        1,
        attributes={
            "reason": reason,
            "issuer": issuer if issuer in _KNOWN_ISSUERS else "unknown",
        },
    )
    return error


def _issuer_settings(issuer, unauthorized):
    if issuer == "manage":
        return os.environ["MANAGE_JWT_PUBLIC_KEY"], {"staff", "admin"}
    if issuer == "submit":
        return os.environ["SUBMIT_JWT_PUBLIC_KEY"], {"submit"}
    raise unauthorized


def _verification_config(issuer, unauthorized):
    try:
        encoded_key, roles = _issuer_settings(issuer, unauthorized)
        key = load_pem_public_key(base64.b64decode(encoded_key, validate=True))
        if not isinstance(key, RSAPublicKey) or key.key_size < 2048:
            raise ValueError("Expected RSA public key of at least 2048 bits")
        audience = os.environ["AUTH_JWT_AUDIENCE"]
        if not audience:
            raise ValueError("Missing JWT configuration")
        return key, roles, audience
    except (KeyError, ValueError, TypeError, binascii.Error):
        raise HTTPException(status_code=503, detail="Authentication unavailable")


def _unverified_issuer(token, unauthorized):
    """Read the issuer to pick the verification key; the token is verified after."""
    try:
        issuer = jwt.decode(token, options={"verify_signature": False}).get("iss")
    except jwt.InvalidTokenError:
        raise _rejected(unauthorized, "invalid")
    if not isinstance(issuer, str) or not issuer:
        raise _rejected(unauthorized, "invalid")
    return issuer


def verify_token(http_request, roles):
    scheme, _, token = http_request.headers.get("Authorization", "").partition(" ")
    unauthorized = HTTPException(
        status_code=401,
        detail="Valid authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if scheme.lower() != "bearer" or not token:
        raise _rejected(unauthorized, "missing")
    issuer = _unverified_issuer(token, unauthorized)
    try:
        key, allowed_roles, audience = _verification_config(issuer, unauthorized)
    except HTTPException as e:
        raise _rejected(e, "unavailable" if e.status_code == 503 else "invalid", issuer)
    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            issuer=issuer,
            audience=audience,
            options={"require": ["iss", "aud", "sub", "iat", "exp", "role"]},
        )
    except jwt.InvalidKeyError:
        raise _rejected(
            HTTPException(status_code=503, detail="Authentication unavailable"),
            "unavailable",
            issuer,
        )
    except jwt.InvalidTokenError:
        raise _rejected(unauthorized, "invalid", issuer)
    if not claims["sub"]:
        raise _rejected(unauthorized, "invalid", issuer)
    if (
        not isinstance(claims["role"], str)
        or claims["role"] not in roles
        or claims["role"] not in allowed_roles
    ):
        raise _rejected(
            HTTPException(status_code=403, detail="Insufficient role"), "role", issuer
        )
    http_request.state.principal = claims


class AuthenticationConfigError(RuntimeError):
    """Authentication is switched off in an environment that requires it."""


def authentication_disabled():
    """
    Return True if AUTHENTICATION_ON=false, which is only for local use.

    Raise AuthenticationConfigError if it is set anywhere else
    """
    if os.environ.get("AUTHENTICATION_ON", "true").lower() != "false":
        return False
    environment = os.environ.get("SENTRY_ENVIRONMENT", "").strip().lower()
    if environment not in _AUTH_OPTIONAL_ENVIRONMENTS:
        raise AuthenticationConfigError(
            "AUTHENTICATION_ON=false is only allowed when SENTRY_ENVIRONMENT is "
            f"one of {sorted(_AUTH_OPTIONAL_ENVIRONMENTS)}, "
            f"not {environment or '(not set)'!r}"
        )
    return True


def add_data_requires_auth(request):
    """Protect add-data processing and any request changing submission pointers."""
    return (
        request.params.type == "add_data" or request.add_data_submission_id is not None
    )


def require_auth(*, roles, when=None):
    """Place below @app.route; handlers accept http_request: Request.

    Optional `when` receives the validated body named `request`.
    """

    def decorate(handler):
        signature = inspect.signature(handler)

        def authorize(args, kwargs):
            if authentication_disabled():
                return
            arguments = signature.bind(*args, **kwargs).arguments
            if when is None or when(arguments["request"]):
                verify_token(arguments["http_request"], roles)

        if inspect.iscoroutinefunction(handler):

            @wraps(handler)
            async def wrapped(*args, **kwargs):
                authorize(args, kwargs)
                return await handler(*args, **kwargs)

        else:

            @wraps(handler)
            def wrapped(*args, **kwargs):
                authorize(args, kwargs)
                return handler(*args, **kwargs)

        return wrapped

    return decorate
