import os

import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError

import database


def _engine():
    url = os.environ["DATABASE_URL"]
    return create_engine(url, connect_args=database._connect_args(url))


def test_connections_have_a_statement_timeout(postgres):
    with _engine().connect() as connection:
        timeout = connection.execute(text("SHOW statement_timeout")).scalar()
    assert timeout == "30s"


def test_slow_statement_is_cancelled(postgres, monkeypatch):
    monkeypatch.setenv("DATABASE_STATEMENT_TIMEOUT_MS", "200")
    with _engine().connect() as connection:
        with pytest.raises(OperationalError, match="statement timeout"):
            connection.execute(text("SELECT pg_sleep(2)"))
