import os
from unittest import TestCase

import snowflake.connector

from inbound.core.models import Description
from inbound.highwatermarks.snowflake import SnowHighwatermark
from inbound.sinks.snowflake import SnowHandler, SnowSink, snow_generate_highwatermark

con_config = {
    "user": os.environ["DBT_USR"],
    "account": "wx23413.europe-west4.gcp",
    "role": "inbound_integration_test",
    "warehouse": "inbound_integration_test",
    "authenticator": "externalbrowser",
}


class TestSnowIntegration(TestCase):

    def test_highwatermark_query(self):
        with snowflake.connector.connect(**con_config) as con:
            query = "select 1 as a"
            high = SnowHighwatermark(connection=con, query=query)
            result = high.generate_query_list()
            expected = [{"A": 1}]
            assert result == expected

    def test_highwatermark_is_empty_list_when_return_is_0_rows(self):
        with snowflake.connector.connect(**con_config) as con:
            query = "select 1 as a where 1=2"
            high = SnowHighwatermark(connection=con, query=query)
            result = high.generate_query_list()
            print(result)
            expected = []
            assert result == expected

    def test_highwatermark_query_is_none_should_raise_error(self):
        with snowflake.connector.connect(**con_config) as con:
            query = None
            with self.assertRaises(AssertionError) as cm:
                high = SnowHighwatermark(connection=con, query=query)
