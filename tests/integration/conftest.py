"""Real PostgreSQL fixtures that isolate changes in a disposable schema."""

import os
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from dotenv import dotenv_values
from psycopg import sql
from sqlalchemy.engine import make_url

from distributed_cloud_scheduler.config import Settings


@pytest.fixture
def integration_database_url(monkeypatch: pytest.MonkeyPatch) -> Iterator[str]:
    configured_url = os.environ.get("TEST_DATABASE_URL")
    if configured_url is None:
        configured_url = dotenv_values(".env").get("TEST_DATABASE_URL")
    if not configured_url:
        pytest.skip("Set TEST_DATABASE_URL to a dedicated PostgreSQL database ending in _test")

    url = make_url(configured_url)
    if url.drivername != "postgresql+psycopg":
        pytest.fail("TEST_DATABASE_URL must use the postgresql+psycopg driver")
    if not url.database or not url.database.endswith("_test"):
        pytest.fail("TEST_DATABASE_URL must name a dedicated database ending in _test")
    application_url = os.environ.get("DATABASE_URL")
    if application_url is None and Path(".env").is_file():
        application_url = Settings().database_url.get_secret_value()
    if application_url and make_url(application_url).database == url.database:
        pytest.fail("TEST_DATABASE_URL must name a different database from DATABASE_URL")

    schema = f"test_{uuid4().hex}"
    connection_url = url.set(drivername="postgresql").render_as_string(hide_password=False)
    with psycopg.connect(connection_url, autocommit=True, connect_timeout=5) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            isolated_url = url.update_query_dict({"options": f"-csearch_path={schema}"})
            isolated_value = isolated_url.render_as_string(hide_password=False)
            monkeypatch.setenv("DATABASE_URL", isolated_value)
            yield isolated_value
        finally:
            # The generated identifier is the only schema this fixture may remove.
            connection.execute(
                sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema))
            )
