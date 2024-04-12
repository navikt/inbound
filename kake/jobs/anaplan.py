import os
from datetime import datetime

import snowflake.connector

from ..core.job import Job
from ..core.models import Metadata
from ..mappers import AnaplanToSnowDescriptionMapper
from ..sinks.snowflake import SnowHandler, SnowSink
from ..taps.anaplan import AnaplanTap


def ingest_budget(job_id: str):

    snow_config = {
        "user": os.environ["SRV_USR"],
        "password": os.environ["SRV_PWD"],
        "account": "wx23413.europe-west4.gcp",
        "role": "regnskap_loader",
        "warehouse": "regnskap_loader",
    }

    snow_db = os.environ["REGNSKAP_RAW_DB"]

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
            run_id=job_id,
            job_name="budget",
        )

        snow_sink = SnowSink(
            table=sink_table,
            append=False,
            connection_handler=snow_handler,
        )

        mapper = AnaplanToSnowDescriptionMapper()

        job = Job(
            tap=anaplan_tap,
            sink=snow_sink,
            description_mapper=mapper,
            metadata=metadata,
        )
        job.run()
