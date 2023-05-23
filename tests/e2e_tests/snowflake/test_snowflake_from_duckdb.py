import logging
import os

import numpy
import pandas

from inbound.core.models import Profile, Spec
from inbound.core.utils import generate_id
from inbound.duckdb import DuckDBConnection
from inbound.snowflake import SnowflakeConnection
from tests.utils.dataframes import df

duck_credentials = Spec(name="duckdb", database=None, table="test")
duck_profile = Profile(spec=duck_credentials)

LOGGER = logging.getLogger(__name__)

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
    table="test_from_duck_db",
    bucket="sync-service",
)

sf_profile = Profile(type="snowflake", name=f"snowflake {account}", spec=spec)


def test_pandas():
    job_id = generate_id()
    test_df = df.copy()
    iterations = -1
    with SnowflakeConnection(profile=sf_profile) as sf_db:
        sf_db.execute("USE DATABASE test_database")
        sf_db.execute("USE SCHEMA test_schema")
        with DuckDBConnection(profile=duck_profile) as duck_db:
            duck_db.from_pandas(test_df, job_id)
            res_iterator = duck_db.to_pandas()
            for index, (dfi, job_res) in enumerate(res_iterator):
                res, job_res = sf_db.from_pandas(dfi, job_id=job_id)
                LOGGER.info(f"Upload: {index}. Result: {res}. {job_res}")
                iterations = index

    assert iterations >= 0


# TODO: Unfinished
def u_copy():
    with SnowflakeConnection(profile=sf_profile) as sf_db:
        with DuckDBConnection(profile=duck_profile) as duck_db:
            duck_db.from_pandas(df)

            dir_name, result = duck_db.to_dir()

            if result.success:
                bucket_name, result = sf_db.from_dir(dir_name, "duckdb_test")

                # create_warehouse_database_and_schema(sf_db)

                # drop_warehouse_database_and_schema(sf_db)

                duck_db.drop(duck_profile.spec.table)

                assert True


test_pandas()
