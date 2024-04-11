from typing import Any, Generator

from oracledb import Connection

from inbound.core.models import Description
from inbound.sdk.tap import Tap
from inbound.sdk.utils import get_query_list

#
# select tbl.*,
#   {{env(source_env}} as _inbound_kildemiljo,
#   current_timestamp as _inbound_lastet_tidspunkt,
#   {{env(job_id)}} as _inbound_jobb_id
# from {{schema}}.{{table}} tbl
# where 1=1
# {% if ddl %} and 1=2 {% endif %}
# {% if has_highwatermark %} for {{ element }} in {{ highwatermark_list}}: and {{ highwater mark experssion}} {% endif %}
#
# target_table_name
# source_table_name
# highwatermarks:
#   - mark ...
#


class OraTap(Tap):
    def __init__(
        self, connection: Connection, query: str, highwatermarks: list[dict] = [{}]
    ):
        if len(highwatermarks) == 0:
            raise ValueError("highwatermarks should not be an empty list")
        self.connection = connection
        self.queries = get_query_list(
            query_template=query, highwatermarks=highwatermarks
        )

    def column_descriptions(self) -> list[Description]:
        query = self.queries[0]
        q_lowercase_where = query.replace("WHERE", "where")
        query_split = q_lowercase_where.split(sep="where", maxsplit=1)
        query_select = query_split[0]
        query_group_by = None
        if len(query_split) == 2:
            query_where_group_by = query_split[1]
            q_lowercase_group_by = query_where_group_by.replace("GROUP BY", "group by")
            query_where_group_by_split = q_lowercase_group_by.split(
                sep="group by", maxsplit=1
            )
            query_where = query_where_group_by_split[0]
            if len(query_where_group_by_split) == 2:
                query_group_by = query_where_group_by_split[1]
        else:
            query_where_group_by = query_split[0]
            q_lowercase_group_by = query_where_group_by.replace("GROUP BY", "group by")
            query_where_group_by_split = q_lowercase_group_by.split(
                sep="group by", maxsplit=1
            )
            if len(query_where_group_by_split) == 2:
                query_select = query_where_group_by_split[0]
                query_group_by = query_where_group_by_split[1]
        desc_query = f"{query_select} where 1=2"
        if query_group_by is not None:
            desc_query = f"{desc_query} group by {query_group_by}"

        print(f"desc_query: {desc_query}")

        column_descriptions = []
        # TODO: Isoler IO
        with self.connection.cursor() as cur:
            cur.execute(desc_query)

            for col in cur.description:
                column_descriptions.append(
                    Description(
                        name=col[0],
                        type=str(col[1]),
                        precision=col[4],
                        scale=col[5],
                        nullable=col[6],
                    )
                )
        return column_descriptions

    def data_generator(self) -> Generator[list[tuple], Any, None]:
        # TODO: Isoler IO
        with self.connection.cursor() as cur:

            for query in self.queries:
                # TODO: Isoler IO
                cur.execute(query)

                while True:
                    # TODO: Isoler IO
                    data = cur.fetchmany(10000)

                    if len(data) == 0:
                        break
                    yield data
