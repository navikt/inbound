import os
from unittest import TestCase

import pyodbc
import snowflake.connector

from inbound.core.job import Job
from inbound.core.models import Description
from inbound.mappers import MSSQLToSnowDescriptionMapper
from inbound.sinks.snowflake import SnowHandler, SnowSink
from inbound.taps.mssql import MSSQLTap

# MSSQL test database
SERVER = "127.0.0.1, 1433"
DATABASE = "model_replicatedmaster"
USERNAME = "SA"
PASSWORD = "Ex4mple!"
ENCRYPT = "no"
connection_string = f"DRIVER={{ODBC Driver 18 for SQL Server}};SERVER={SERVER};DATABASE={DATABASE};ENCRYPT={ENCRYPT};UID={USERNAME};PWD={PASSWORD}"


class TestMSSQLIntegration(TestCase):
    def test_mssql_generator(self):

        with pyodbc.connect(connection_string) as connection:
            query = """
            select 1
            """
            mssql_tap = MSSQLTap(connection=connection, query=query)
            for rows in mssql_tap.data_generator():
                assert len(rows) == 1

    def test_mssql_column_description(self):
        query = "select cast(1 as decimal(38,5)) as foo"
        with pyodbc.connect(connection_string) as connection:
            mssql_tap = MSSQLTap(connection=connection, query=query)
            result = mssql_tap.column_descriptions()
            expected = [
                Description(
                    name="foo",
                    type="<class 'decimal.Decimal'>",
                    precision=38,
                    scale=5,
                    nullable=True,
                )
            ]
            assert expected == result

    def test_highwatermarks(self):
        highwatermarks = [{"A": 1}]
        query = """
            select t.* from
            (select 1 as a
            union
            select 2 as a) as t
            where a > {{ highwatermark['A'] }}
        """
        with pyodbc.connect(connection_string) as connection:
            mssql_tap = MSSQLTap(
                connection=connection, query=query, highwatermarks=highwatermarks
            )
            result = [list(rows) for rows in mssql_tap.data_generator()]

            expected = 2

            assert result[0][0][0] == expected

    def test_empty_highwatermarks_should_raise_exception(self):
        highwatermarks = []
        query = """
            select *
            from (
                select 1 as a
                union
                select 2 as a
            )
            where a > {{ highwatermark['A'] }}
        """
        with pyodbc.connect(connection_string) as connection:
            self.assertRaises(
                ValueError,
                MSSQLTap,
                connection=connection,
                query=query,
                highwatermarks=highwatermarks,
            )

    def test_mssql_to_snow(self):

        query_template = """
            select 1 as a
            union all
            select 2 as a
        """

        snow_config = {
            "user": os.environ["DBT_USR"],
            "account": "wx23413.europe-west4.gcp",
            "role": "inbound_integration_test",
            "warehouse": "inbound_integration_test",
            "authenticator": "externalbrowser",
        }

        snow_table = "inbound_integration_test.test.test_mssql_to_snow"

        with pyodbc.connect(connection_string) as mssql_con:
            with snowflake.connector.connect(**snow_config) as snow_con:
                tap = MSSQLTap(mssql_con, query_template)
                sink = SnowSink(
                    connection_handler=SnowHandler(connection=snow_con),
                    table=snow_table,
                    transient=True,
                    transient_table_postfix="",
                )
                mapper = MSSQLToSnowDescriptionMapper()
                job = Job(tap=tap, sink=sink, description_mapper=mapper)
                job.run()
                with snow_con.cursor() as cur:
                    cur.execute(f"select * from {snow_table}")
                    result = cur.fetchall()
                    expected = [(1,), (2,)]
                    assert result == expected
                    cur.execute(f"drop table if exists {snow_table}")
