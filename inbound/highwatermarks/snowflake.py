from snowflake.connector import DictCursor, SnowflakeConnection

from inbound.sdk.highwatermark import Highwatermark


class SnowHighwatermark(Highwatermark):
    def __init__(self, connection: SnowflakeConnection, query: str):
        assert connection is not None, "Connection is None"
        self.connection = connection
        assert query is not None, "Query is None"
        self.query = query

    def generate_query_list(self) -> list[dict]:
        with self.connection.cursor(DictCursor) as cursor:
            cursor.execute(self.query)
            return cursor.fetchall()
