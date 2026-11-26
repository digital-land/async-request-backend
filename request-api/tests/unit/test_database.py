import database


def test_postgres_connections_get_a_statement_timeout(monkeypatch):
    monkeypatch.delenv("DATABASE_STATEMENT_TIMEOUT_MS", raising=False)
    assert database._connect_args("postgresql://u:p@db/requests") == {
        "options": "-c statement_timeout=30000"
    }


def test_statement_timeout_can_be_configured(monkeypatch):
    monkeypatch.setenv("DATABASE_STATEMENT_TIMEOUT_MS", "5000")
    assert database._connect_args("postgresql+psycopg2://u:p@db/requests") == {
        "options": "-c statement_timeout=5000"
    }


def test_other_databases_get_no_postgres_options():
    assert database._connect_args("sqlite://") == {}
