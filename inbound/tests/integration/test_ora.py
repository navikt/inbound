from unittest import TestCase

import oracledb

from inbound.core.models import Description
from inbound.sdk.highwatermark import Highwatermark
from inbound.taps.oracle import OraTap

con_config = {
    "dsn": "localhost:1521/FREEPDB1",
    "user": "system",
    "password": "example",
}


class DummyHighWatermark(Highwatermark):
    def __init__(self, highwatermarks):
        self.highwatermarks = highwatermarks

    def generate_query_list(self):
        return self.highwatermarks


class TestOraIntegration(TestCase):

    def test_ora_generator_yields_list_of_tuples(self):
        with oracledb.connect(**con_config) as connection:
            query = """
            select *
            from (
                select 1 as a from dual
                union all
                select 2 as a from dual
                )
            """
            ora_tap = OraTap(connection=connection, query=query)
            result = next(ora_tap.data_generator())
            expected = [(1,), (2,)]
            assert result == expected

    def test_ora_column_description(self):
        query = "select cast(1 as number(38,5)) as foo from dual"
        with oracledb.connect(**con_config) as connection:
            ora_tap = OraTap(connection=connection, query=query)
            result = ora_tap.column_descriptions()
            expected = [
                Description(
                    name="FOO",
                    type="<DbType DB_TYPE_NUMBER>",
                    precision=38,
                    scale=5,
                    nullable=True,
                )
            ]
            assert expected == result

    def test_ora_column_description_with_group_by_query(self):
        query = "select sum(cast(1 as number(38,5))) as foo from dual group by 1"
        with oracledb.connect(**con_config) as connection:
            ora_tap = OraTap(connection=connection, query=query)
            result = ora_tap.column_descriptions()
            expected = [
                Description(
                    name="FOO",
                    type="<DbType DB_TYPE_NUMBER>",
                    precision=None,
                    scale=None,
                    nullable=True,
                )
            ]
            assert expected == result

    def test_ora_column_description_with_where_and_group_by_query(self):
        query = (
            "select sum(cast(1 as number(38,5))) as foo from dual where 1=1 group by 1"
        )
        with oracledb.connect(**con_config) as connection:
            ora_tap = OraTap(connection=connection, query=query)
            result = ora_tap.column_descriptions()
            expected = [
                Description(
                    name="FOO",
                    type="<DbType DB_TYPE_NUMBER>",
                    precision=None,
                    scale=None,
                    nullable=True,
                )
            ]
            assert expected == result

    def test_ora_column_description_with_empty_highwatermark(self):
        query = "select sum(cast(1 as number(38,5))) as foo from dual where 1=1 and {{ highwatermark['B'] }} group by 1"
        with oracledb.connect(**con_config) as connection:
            ora_tap = OraTap(connection=connection, query=query)
            result = ora_tap.column_descriptions()
            expected = [
                Description(
                    name="FOO",
                    type="<DbType DB_TYPE_NUMBER>",
                    precision=None,
                    scale=None,
                    nullable=True,
                )
            ]
            assert expected == result

    def test_highwatermarks(self):
        highwatermarks = DummyHighWatermark(highwatermarks=[{"A": 1}])
        query = """
            select *
            from (
                select 1 as a from dual
                union
                select 2 as a from dual
            )
            where a > {{ highwatermark['A'] }}
        """
        with oracledb.connect(**con_config) as connection:
            ora_tap = OraTap(
                connection=connection, query=query, highwatermark=highwatermarks
            )
            result = [rows for rows in ora_tap.data_generator()]
            expected = [[(2,)]]
            assert result == expected

    def test_empty_highwatermarks_should_not_yield(self):

        highwatermarks = DummyHighWatermark(highwatermarks=[])
        query = """
            select *
            from (
                select 1 as a from dual
                union
                select 2 as a from dual
            )
            where a > {{ highwatermark['A'] }}
        """
        with oracledb.connect(**con_config) as connection:
            ora_tap = OraTap(
                connection=connection, query=query, highwatermark=highwatermarks
            )
            result = [rows for rows in ora_tap.data_generator()]
            expected = []
            assert result == expected
