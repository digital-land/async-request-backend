"""Verify Manage-issued JWTs before executing protected handlers."""

import base64
import binascii
import inspect
import os
from functools import wraps

import jwt
from fastapi import HTTPException


def verify_token(http_request, roles):
    scheme, _, token = http_request.headers.get("Authorization", "").partition(" ")
    unauthorized = HTTPException(
        status_code=401,
        detail="Valid Manage authentication required",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if scheme.lower() != "bearer" or not token:
        raise unauthorized
    try:
        key = base64.b64decode(os.environ.get("AUTH_JWT_PUBLIC_KEY", ""), validate=True)
        issuers = [
            value.strip()
            for value in os.environ["AUTH_TRUSTED_ISSUERS"].split(",")
            if value.strip()
        ]
        audience = os.environ["AUTH_JWT_AUDIENCE"]
        if not key or not issuers or not audience:
            raise ValueError("Missing JWT configuration")
    except (KeyError, ValueError, binascii.Error):
        raise HTTPException(status_code=503, detail="Authentication unavailable")
    try:
        claims = jwt.decode(
            token,
            key,
            algorithms=["RS256"],
            issuer=issuers,
            audience=audience,
            options={"require": ["iss", "aud", "sub", "iat", "exp", "role"]},
        )
    except jwt.InvalidKeyError:
        raise HTTPException(status_code=503, detail="Authentication unavailable")
    except jwt.InvalidTokenError:
        raise unauthorized
    if not claims["sub"]:
        raise unauthorized
    if not isinstance(claims["role"], str) or claims["role"] not in roles:
        raise HTTPException(status_code=403, detail="Insufficient role")
    http_request.state.principal = claims


def add_data_requires_auth(request):
    return request.params.type == "add_data"


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
