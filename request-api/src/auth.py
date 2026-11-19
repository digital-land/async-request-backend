"""Verify client JWTs with issuer-specific keys and role limits."""

import base64
import binascii
import inspect
import os
from functools import wraps

import jwt
from cryptography.hazmat.primitives.serialization import load_pem_public_key
from cryptography.hazmat.primitives.asymmetric.rsa import RSAPublicKey
from fastapi import HTTPException


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


def verify_token(http_request, roles):
    scheme, _, token = http_request.headers.get("Authorization", "").partition(" ")
    unauthorized = HTTPException(
        status_code=401,
        detail="Valid authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if scheme.lower() != "bearer" or not token:
        raise unauthorized
    try:
        issuer = jwt.decode(token, options={"verify_signature": False}).get("iss")
        if not isinstance(issuer, str) or not issuer:
            raise unauthorized
    except jwt.InvalidTokenError:
        raise unauthorized
    key, allowed_roles, audience = _verification_config(issuer, unauthorized)
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
        raise HTTPException(status_code=503, detail="Authentication unavailable")
    except jwt.InvalidTokenError:
        raise unauthorized
    if not claims["sub"]:
        raise unauthorized
    if (
        not isinstance(claims["role"], str)
        or claims["role"] not in roles
        or claims["role"] not in allowed_roles
    ):
        raise HTTPException(status_code=403, detail="Insufficient role")
    http_request.state.principal = claims


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
            if os.environ.get("AUTHENTICATION_ON", "true").lower() == "false":
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
