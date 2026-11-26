import os
from functools import cache

from sqlalchemy import create_engine
from sqlalchemy.engine import make_url
from sqlalchemy.orm import sessionmaker

DEFAULT_STATEMENT_TIMEOUT_MS = 30_000


def _connect_args(url):
    if not make_url(url).get_backend_name().startswith("postgresql"):
        return {}
    timeout = int(
        os.environ.get("DATABASE_STATEMENT_TIMEOUT_MS", DEFAULT_STATEMENT_TIMEOUT_MS)
    )
    return {"options": f"-c statement_timeout={timeout}"}


@cache
def engine():
    url = os.environ["DATABASE_URL"]
    return create_engine(url, hide_parameters=True, connect_args=_connect_args(url))


@cache
def session_maker():
    return sessionmaker(autocommit=False, autoflush=False, bind=engine())
