from snowflake.connector import DictCursor, SnowflakeConnection

from inbound.sdk.highwatermark import Highwatermark


class SnowHighwatermark(Highwatermark):
    def __init__(self, connection: SnowflakeConnection, query: str):
        self.connection = connection
        self.query = query

    def generate_query_list(self) -> list[dict]:
        with self.connection.cursor(DictCursor) as cursor:
            cursor.execute(self.query)
            return cursor.fetchall()
