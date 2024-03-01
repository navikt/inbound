import os
from unittest import TestCase

import snowflake.connector

from inbound.core.models import Description
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
            result = snow_generate_highwatermark(connection=con, query=query)
            expected = [{"A": 1}]
            assert result == expected
