import os
from unittest import TestCase

import oracledb
import snowflake.connector

from inbound.core.job import Job
from inbound.mappers import OraToSnowDescriptionMapper
from inbound.sinks.snowflake import SnowHandler, SnowSink
from inbound.taps.oracle import OraTap


class TestOraToSnowIntegration(TestCase):
    def test_ora_to_snow(self):
        ora_config = {
            "dsn": "localhost:1521/FREEPDB1",
            "user": "system",
            "password": "example",
        }
        query_template = """
            select *
            from (
                select 1 as a from dual
                union all
                select 2 as a from dual
            )
        """

        snow_config = {
            "user": os.environ["DBT_USR"],
            "account": "wx23413.europe-west4.gcp",
            "role": "inbound_integration_test",
            "warehouse": "inbound_integration_test",
            "authenticator": "externalbrowser",
        }

        snow_table = "inbound_integration_test.test.test_ora_to_snow"

        with oracledb.connect(**ora_config) as ora_con:
            with snowflake.connector.connect(**snow_config) as snow_con:
                tap = OraTap(ora_con, query_template)
                sink = SnowSink(
                    connection_handler=SnowHandler(connection=snow_con),
                    table=snow_table,
                    append=False,
                )
                mapper = OraToSnowDescriptionMapper()
                job = Job(tap, sink, mapper)
                job.run()
                with snow_con.cursor() as cur:
                    cur.execute(f"select * from {snow_table}")
                    result = cur.fetchall()
                    expected = [(1,), (2,)]
                    assert result == expected
