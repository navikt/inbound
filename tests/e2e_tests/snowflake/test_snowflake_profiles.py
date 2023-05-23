import os
from pathlib import Path

import pandas

from inbound.core.logging import LOGGER
from inbound.core.models import Profile, Spec
from inbound.snowflake import SnowflakeConnection

user = os.getenv("SNOWFLAKE_TEST_USER")
pwd = os.getenv("SNOWFLAKE_TEST_PASSWORD")
account = os.getenv("SNOWFLAKE_TEST_ACCOUNT")

database = os.getenv("SNOWFLAKE_TEST_DATABASE")
warehouse = os.getenv("SNOWFLAKE_TEST_WAREHOUSE")
role = os.getenv("SNOWFLAKE_TEST_ROLE")
schema = os.getenv("SNOWFLAKE_TEST_SCHEMA")
name = f"snowflake {account}"

table = "test"
query = "select * from test"

table_qualified = f"{database}.{schema}.test"
query_qualified = f"select * from {database}.{schema}.test"

detailed_spec = Spec(
    account=account,
    user=user,
    password=pwd,
    database=database,
    schema=schema,
    warehouse=warehouse,
    role=role,
    name=name,
    table=table,
    query=query,
)
detailed_profile = Profile(type="snowflake", name=name, spec=detailed_spec)

sa_spec = Spec(
    role=role,
    name=name,
    connection_string=f"snowflake://{user}:{pwd}@{account}/{database}/{schema}?warehouse={warehouse}",
    table=table,
    query=query,
)
sa_profile = Profile(type="snowflake", name=name, spec=sa_spec)

dbt_spec = Spec(
    name=name,
    profile="test-snowflake-db",
    target="dev",
    profiles_dir=str(Path(__file__).parent),
    table=table,
    query=query,
)
dbt_profile = Profile(type="snowflake", name=name, spec=dbt_spec)


def test_dbt_profile():
    roundtrip(dbt_profile)


def test_sa_profile():
    with SnowflakeConnection(profile=sa_profile) as db:
        current_role = db.execute("select current_role()").fetchone()
        assert current_role[0] == "PUBLIC"

    with SnowflakeConnection(profile=sa_profile) as db:
        current_database = db.execute("select current_database()").fetchone()
        assert current_database[0] == None

    with SnowflakeConnection(profile=sa_profile) as db:
        current_schema = db.execute("select current_schema()").fetchone()
        assert current_schema[0].casefold() == schema.casefold()

    roundtrip(sa_profile)


def test_detailed_profile():
    with SnowflakeConnection(profile=detailed_profile) as db:
        current_role = db.execute("select current_role()").fetchone()
        assert current_role[0].casefold() == role.casefold()

    with SnowflakeConnection(profile=detailed_profile) as db:
        current_database = db.execute("select current_database()").fetchone()
        assert current_database[0].casefold() == database.casefold()

    with SnowflakeConnection(profile=detailed_profile) as db:
        current_schema = db.execute("select current_schema()").fetchone()
        assert current_schema[0].casefold() == schema.casefold()

    roundtrip(detailed_profile)


def roundtrip(profile: Profile):
    with SnowflakeConnection(profile=profile) as db:
        df_out = pandas.DataFrame()
        create_test_table(db)
        insert_data(db)
        res_iterator = db.to_pandas()
        df, job_res = next(res_iterator)
        if type(df) == pandas.DataFrame:
            df_out = df
        drop_table(db)

        assert df_out.size > 0
        assert df_out.iloc[0][1] == "Oda"


def create_test_table(db):
    db.execute(
        """
        create or replace table
        test(id integer, name string)
        """
    )


def insert_data(db):
    db.execute(
        """
        insert into test
        (id, name)
        values
        (1, 'Oda')
        """
    )


def select_data(db):
    return db.execute(
        """
        select * from test
        """
    )


def drop_table(db):
    db.execute(
        """
        drop table test
        """
    )


test_detailed_profile()
test_sa_profile()
