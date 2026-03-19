from oracledb import Connection

from inbound.sdk.highwatermark import Highwatermark


class OraHighwatermark(Highwatermark):
    def __init__(self, connection: Connection, query: str):
        self.connection = connection
        self.query = query

    def generate_query_list(self) -> list[dict]:
        with self.connection.cursor() as cursor:
            cursor.execute(self.query)
            result = cursor.fetchall()
            if len(result) == 0:
                return []
            desc = cursor.description()
            return [dict(zip([col[0] for col in desc], row)) for row in result]
