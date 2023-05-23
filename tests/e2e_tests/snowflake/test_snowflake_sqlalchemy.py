import os

import numpy as np

from inbound.core.models import Profile, Spec
from inbound.snowflake import SnowflakeConnection
from tests.utils.dataframes import df

user = os.getenv("SNOWFLAKE_TEST_USER")
pwd = os.getenv("SNOWFLAKE_TEST_PASSWORD")
account = os.getenv("SNOWFLAKE_TEST_ACCOUNT")

spec = Spec(
    database=os.getenv("SNOWFLAKE_TEST_DATABASE"),
    database_schema=os.getenv("SNOWFLAKE_TEST_SCHEMA"),
    warehouse=os.getenv("SNOWFLAKE_TEST_WAREHOUSE"),
    role=os.getenv("SNOWFLAKE_TEST_ROLE"),
    name=f"snowflake {account}",
    connection_string=f"snowflake://{user}:{pwd}@{account}",
    table="test",
)

profile = Profile(type="snowflake", name=f"snowflake {account}", spec=spec)


def test_write_pandas_append():
    # split in 4 chunks
    chunks = np.array_split(df, 4)
    with SnowflakeConnection(profile=profile) as db:
        for index in range(len(chunks)):
            _, job_res = db.from_pandas(chunks[index], chunk_number=index)

        assert job_res.result == "DONE"


def test_roundtrip():
    with SnowflakeConnection(profile=profile) as db:
        db.from_pandas(df)
        res = db.to_pandas()
        df_res, _ = next(res)

        assert df_res.size > 0


def test_pandas_replace():
    with SnowflakeConnection(profile=profile) as db:
        res, job_res = db.from_pandas(df)
        assert job_res.result == "DONE"


def test_drop_table():
    with SnowflakeConnection(profile=profile) as db:
        db.from_pandas(df)
        res = db.drop(profile.spec.table)
        assert res.result == "DONE"


test_write_pandas_append()
