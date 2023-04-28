import os
import shutil
import tempfile
from pathlib import Path

import pytest
import sqlalchemy as sa

from inbound.core.settings import Settings

ENV = os.environ


@pytest.fixture(scope="function")
def data_path():
    return os.environ["INBOUND_DATA_PATH"]


@pytest.fixture(scope="function")
def temp_db():
    f = tempfile.mkstemp(suffix=".db")[1]
    uri = "sqlite:///" + f
    engine = sa.create_engine(uri)
    con = engine.connect()
    con.execute(
        """CREATE TABLE temp (
        p BIGINT PRIMARY KEY,
        a REAL NOT NULL,
        b BIGINT NOT NULL,
        c TEXT NOT NULL);"""
    )
    con.execute(
        """CREATE TABLE temp2 (
        d REAL NOT NULL,
        e BIGINT NOT NULL,
        f TEXT NOT NULL);"""
    )
    df.to_sql("temp", uri, if_exists="append")
    df2.to_sql("temp_nopk", uri, if_exists="append", index=False)
    try:
        yield "temp", "temp_nopk", uri
    finally:
        if os.path.isfile(f):
            os.remove(f)


def pytest_sessionstart(session):
    """
    Called after the Session object has been created and
    before performing collection and entering the run test loop.
    """

    cwd = Path(__file__).parent
    DATA_DIR = str(cwd / "data")

    os.environ["INBOUND_DATA_PATH"] = DATA_DIR
    os.environ["INBOUND_JOBS_PATH"] = DATA_DIR + "/jobs"
    os.environ["DBT_PROFILES_DIR"] = str(cwd / "dbt")
    os.environ["INBOUND_PROFILES_DIR"] = str(cwd / "inbound")
    os.environ["INBOUND_PROJECT_DIR"] = str(cwd / "inbound")

    settings = Settings()
    secret_dir = tempfile.TemporaryDirectory()
    os.environ["GOOGLE_CLOUD_PROJECT"] = settings.spec.gcp.project_id
    os.environ["INBOUND_SECRET_PATH"] = secret_dir.name


def pytest_sessionfinish(session, exitstatus):
    """
    Called after test run finished, right before
    returning the exit status to the system.
    """

    secret_dir = os.environ["INBOUND_SECRET_PATH"]
    try:
        shutil.rmtree(secret_dir)
    except:
        pass

    os.environ = ENV
