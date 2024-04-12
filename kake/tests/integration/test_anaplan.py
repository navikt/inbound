import csv
import os
from datetime import datetime
from pathlib import Path
from unittest import TestCase

import snowflake.connector

from kake.core.job import Job
from kake.core.models import Description, Metadata
from kake.mappers import AnaplanToSnowDescriptionMapper
from kake.sinks.csv import CsvSink
from kake.sinks.snowflake import SnowHandler, SnowSink
from kake.taps.anaplan import AnaplanTap

workspaceID = "8a868cd985f53e7701860542f59e276e"
modelID = "7C773072370243ED9FDD224F0A3B6E53"
exportID = "116000000001"
fileID = "116000000001"

username = os.environ["ANAPLAN_USR"]
password = os.environ["ANAPLAN_PWD"]


class TestAnaplan(TestCase):

    def test_column_description(self):

        tap = AnaplanTap(
            workspaceID=workspaceID,
            modelID=modelID,
            exportID=exportID,
            fileID=fileID,
            username=username,
            password=password,
        )
        result = tap.column_descriptions()
        expected = Description(
            name="export_code", type="TEXT", precision=None, scale=None, nullable=True
        )
        print(result)

        assert result[0] == expected

    def test_last(self):
        class FileHandlerMock:
            def __init__(
                self, file_path: str = "/tmp/inbound", file_name: str = "inbound.csv"
            ):
                self.file_path = file_path
                self.file_name = file_name
                self.full_file_name = f"{file_path}/{file_name}"

            def create_dir(self):
                Path(self.file_path).mkdir(exist_ok=True)

            def create_file(self):
                return open(
                    file=self.full_file_name, mode="w", encoding="utf-8", newline=""
                )

            def close_file(self, file):
                file.close()

            def delete_file(self): ...

        snow_config = {
            "user": os.environ["DBT_USR"],
            "account": "wx23413.europe-west4.gcp",
            "role": "regnskap_loader",
            "warehouse": "regnskap_loader",
            "authenticator": "externalbrowser",
        }

        snow_db = "dev_regnskap_raw"

        username = os.environ["ANAPLAN_USR"]
        password = os.environ["ANAPLAN_PWD"]

        workspaceID = "8a868cd985f53e7701860542f59e276e"
        modelID = "7C773072370243ED9FDD224F0A3B6E53"
        exportID = "116000000001"
        fileID = "116000000001"

        sink_table = f"{snow_db}.anaplan.eksport_budsjett__transient"

        # Må settes før connection er opprettet (kanskje ikke nødvendig)
        snowflake.connector.paramstyle = "qmark"

        with snowflake.connector.connect(**snow_config) as snow_connection:

            anaplan_tap = AnaplanTap(
                workspaceID=workspaceID,
                modelID=modelID,
                exportID=exportID,
                fileID=fileID,
                username=username,
                password=password,
            )

            snow_handler = SnowHandler(connection=snow_connection)

            metadata = Metadata(
                load_time=datetime.now(),
                source_env=anaplan_tap.base_url,
                run_id="foo",
                job_name="budget",
            )

            snow_sink = SnowSink(
                table=sink_table,
                append=False,
                connection_handler=snow_handler,
                file_handler=FileHandlerMock(),
            )

            mapper = AnaplanToSnowDescriptionMapper()

            job = Job(
                tap=anaplan_tap,
                sink=snow_sink,
                description_mapper=mapper,
                metadata=metadata,
            )
            job.run()
