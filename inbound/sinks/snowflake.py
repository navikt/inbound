import csv
import os
from datetime import datetime
from functools import partial
from pathlib import Path
from typing import Any, Generator

from jinja2 import Environment
from snowflake.connector import DictCursor, SnowflakeConnection

from inbound.core.models import Description
from inbound.sdk.sink import Sink


class SnowHandler:
    def __init__(self, connection: SnowflakeConnection) -> None:
        self.connection = connection

    def create_table(self, ddl: str):
        with self.connection.cursor() as cur:
            cur.execute(ddl)

    def ingest_file_to_table(
        self,
        table: str,
        database: str,
        schema: str,
        file_path: str,
        file_name: str,
    ):
        with self.connection.cursor() as cur:
            put_query = f"""
                PUT file://{file_path}/{file_name}
                @{database}.{schema}.%{table}/{file_name}
                AUTO_COMPRESS=TRUE OVERWRITE = TRUE
            """
            copy_into_query = f"""
                COPY INTO {database}.{schema}.{table}
                FROM @{database}.{schema}.%{table}/{file_name}
                FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY = '"' PARSE_HEADER = TRUE)
                MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            """
            cur.execute(put_query)
            cur.execute(copy_into_query)


class FileHandler:
    def __init__(self, file_path: str = "/tmp/inbound", file_name: str = "inbound.csv"):
        self.file_path = file_path
        self.file_name = file_name
        self.full_file_name = f"{file_path}/{file_name}"

    def create_dir(self):
        Path(self.file_path).mkdir(exist_ok=True)

    def create_file(self):
        return open(file=self.full_file_name, mode="w", encoding="utf-8", newline="")

    def close_file(self, file):
        file.close()

    def delete_file(self):
        os.remove(self.full_file_name)


class SnowSink(Sink):
    def __init__(
        self,
        table: str,
        append: bool,
        connection_handler: SnowHandler,
        tmp_file_max_size: int = 1024 * 1024 * 1024 * 4,  # 4GB
        csv_writer=None,
        ddl: str = None,
        file_handler: FileHandler = None,
    ):
        self.table = table
        self.append = append
        self.tmp_file_max_size = tmp_file_max_size
        self.csv_writer = csv_writer
        self.ddl = ddl
        self.snow_handler = connection_handler
        if file_handler is None:
            file_handler = FileHandler()
        self.file_handler = file_handler

    def ingest(
        self,
        data_generator: Generator[list[tuple], Any, None],
        column_description: list[Description],
    ):
        snow_database, snow_schema, snow_table = self.table.split(".")

        if self.ddl is None:
            self.ddl = self.create_ddl(
                database=snow_database,
                schema=snow_schema,
                table=snow_table,
                column_descriptions=column_description,
                append=self.append,
            )
        if self.csv_writer is None:
            self.csv_writer = partial(csv.writer)

        self.file_handler.create_dir()
        self.snow_handler.create_table(self.ddl)

        batch_results = []
        batch_counter = -1
        data_exists = True
        while data_exists:
            batch_counter = batch_counter + 1
            batch_rows = 0
            batch_start = datetime.now()
            file_size_bytes = 0
            file = self.file_handler.create_file()
            writer = self.csv_writer(file)
            col_names = tuple(c.name for c in column_description)
            writer.writerow(col_names)
            try:
                while True:
                    print("fetching data")
                    data = next(data_generator)
                    writer.writerows(data)
                    batch_rows = batch_rows + len(data)
                    print(f"writed {batch_rows} rows to tmp-file")
                    file_size_bytes = file.tell()
                    if file_size_bytes > self.tmp_file_max_size:
                        break
            except StopIteration:
                data_exists = False
            batch_stop = datetime.now()
            batch_results.append(
                {
                    "number": batch_counter,
                    "start": batch_start,
                    "stop": batch_stop,
                    "rows": batch_rows,
                    "size_bytes": file_size_bytes,
                }
            )
            print(f"Uploading new batch to snow: {batch_results}")
            self.file_handler.close_file(file=file)
            self.snow_handler.ingest_file_to_table(
                database=snow_database,
                schema=snow_schema,
                table=snow_table,
                file_path=self.file_handler.file_path,
                file_name=self.file_handler.file_name,
            )
            print("batch uploaded")
        self.file_handler.delete_file()

        return batch_results

    @staticmethod
    def create_ddl(
        table: str,
        database: str,
        schema: str,
        column_descriptions: list[Description],
        append: bool,
    ) -> str:
        # TODO: Endre slik at "not append"-tabeller er transient
        ddl_jinja = """
                create {%- if not append %} or replace table {% else %} table if not exists {%- endif %} {{ database -}}.{{- schema -}}.{{- table -}} (
                {%- for column in column_descriptions -%}
                    {{- column.name }} {{ column.type }}{% if column.type == 'number' %}({{ column.precision }}, {{ column.scale }}){% endif -%} {% if not loop.last %},{% endif -%}
                {%- endfor -%}
                )
            """.strip()
        ddl_template = Environment().from_string(source=ddl_jinja)

        create_table_ddl = ddl_template.render(
            table=table,
            database=database,
            schema=schema,
            column_descriptions=column_descriptions,
            append=append,
        )
        print(create_table_ddl)

        return create_table_ddl


def snow_generate_highwatermark(connection: SnowflakeConnection, query) -> list[dict]:
    with connection.cursor(DictCursor) as cur:
        cur.execute(query)
        return cur.fetchall()
