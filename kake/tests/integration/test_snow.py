import os
from unittest import TestCase

import snowflake.connector

from kake.core.models import Description
from kake.sinks.snowflake import SnowHandler, SnowSink, snow_generate_highwatermark

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
            result = snow_generate_highwatermark(connection=con, query=query)
            expected = [{"A": 1}]
            assert result == expected

    def test_highwatermark_is_empty_list_when_return_is_0_rows(self):
        with snowflake.connector.connect(**con_config) as con:
            query = "select 1 as a where 1=2"
            result = snow_generate_highwatermark(connection=con, query=query)
            print(result)
            expected = []
            assert result == expected
