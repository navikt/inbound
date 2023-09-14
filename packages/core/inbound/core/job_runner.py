import asyncio
import json
import os
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from enum import Enum
from typing import List, Protocol

from inbound.core import dbt_profile, jobs
from inbound.core.connection import Connection
from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import Profile, Spec
from inbound.core.soda_profile import get_soda_profile
from inbound.core.utils import generate_id
from inbound.gcs import GCSConnection
from inbound.snowflake import SnowflakeConnection


class Request(Protocol):
    ...


class Actions(Enum):
    INGEST = 1
    TRANSFORM = 2
    METADATA = 3
    SODA = 4
    RE_DATA = 5


async def _run_process(cmd_args):
    process = await asyncio.create_subprocess_shell(
        cmd_args, stdout=asyncio.subprocess.PIPE
    )
    res, _ = await process.communicate()
    return res.decode("utf-8")


# utility function to temporarily swith working directory
@contextmanager
def use_dir(path):
    current_working_dir = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(current_working_dir)


def write_pydantic_to_db(
    profile: Profile,
    table: str,
    job_id: str,
    time_start: datetime,
    time_end: datetime,
    blob: object,
    connection: Connection,
):
    time_end = datetime.now(timezone.utc)
    result = []
    with connection(profile=profile) as db:
        db.execute(
            f"""
            create table if not exists 
            {table}(job_id string, time_start timestamp_ltz default current_timestamp, time_end timestamp_ltz default current_timestamp, raw variant)
            """
        )
        db.execute(
            f"""insert into {table}(job_id, time_start, time_end, raw)
                select '{job_id}', '{time_start}', '{time_end}', parse_json($${blob.json()}$$);
            """
        )


def write_metadata_json_to_db(
    profile: Profile,
    table: str,
    job_id: str,
    time_start: datetime,
    time_end: datetime,
    blob: object,
    connection: Connection,
):
    if connection is None:
        LOGGER.error("Please provide a valid db connection class")
        return

    time_end = datetime.now(timezone.utc)
    result = []
    try:
        with connection(profile=profile) as db:
            db.execute(
                f"""
                create table if not exists 
                {table}(job_id string, time_start timestamp_ltz default current_timestamp, time_end timestamp_ltz default current_timestamp, raw variant)
                """
            )
            db.execute(
                f"""insert into {table}(job_id, time_start, time_end, raw)
                    select '{job_id}', '{time_start}', '{time_end}', parse_json($${blob}$$);
                """
            )
    except Exception as e:
        LOGGER.info()


def write_metadata_text_to_db(
    profile: Profile,
    table: str,
    job_id: str,
    time_start: datetime,
    time_end: datetime,
    text: str,
    connection: Connection,
):
    time_end = datetime.now(timezone.utc)
    result = []
    try:
        with connection(profile=profile) as db:
            db.execute(
                f"""
                create table if not exists 
                {table}(job_id string, time_start timestamp_ltz default current_timestamp,time_end timestamp_ltz default current_timestamp, raw text)
                """
            )
            db.execute(
                f"""insert into {table}(job_id, time_start, time_end, raw)
                    select '{job_id}', '{time_start}', '{time_end}', '{text}';
                """
            )
    except Exception as e:
        print(e)


def write_job_run_result_to_db(
    profile: Profile,
    job_id: str,
    time_start: datetime,
    time_end: datetime,
    actions: str,
    success: str,
    message: str,
    connection: Connection = SnowflakeConnection,
):
    time_end = datetime.now(timezone.utc)
    try:
        with connection(profile=profile) as db:
            db.execute(
                f"""
                create table if not exists 
                meta.job_run(job_id string, time_start timestamp_ltz default current_timestamp,time_end timestamp_ltz default current_timestamp, actions text, success text, message text)
                """
            )
            db.execute(
                f"""insert into meta.job_run(job_id, time_start, time_end, actions, success, message)
                    select '{job_id}', '{time_start}', '{time_end}', '{actions}', '{success}', '{message}';
                """
            )
    except Exception as e:
        LOGGER.error(f"Error persisting job result. {e}")


class JobRunner:
    def __init__(
        self,
        db: str,
        profile: str,
        target: str = "transformer",
        metadata_schema: str = "META",
        job_file_name: str | None = None,
        connection: Connection | None = None,
        actions: List[Actions] = [Actions.INGEST, Actions.TRANSFORM, Actions.METADATA],
        soda: callable = None,
    ):
        self.db = db
        self.metadata_schema = metadata_schema
        self.job_file_name = job_file_name
        self.connection = connection
        self.actions = actions
        self.soda = soda
        self.JOBS_DIR = os.getenv("INBOUND_JOBS_DIR", "./inbound/jobs")
        self.DBT_DIR = os.getenv("DBT_DIR", "./dbt")
        self.SODA_DIR = os.getenv("SODA_DIR", "./soda")
        self.DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR", ".")
        self.GCS_BUCKET = os.getenv("INBOUND_GCS_BUCKET", None)
        self.DBT_TARGET = target
        self.DBT_PROFILE = profile
        self.TEMP_DIR = tempfile.mkdtemp()

        self.job_id = generate_id()

        self.conn_params = dbt_profile.dbt_connection_params(
            self.DBT_PROFILE, self.DBT_TARGET, self.DBT_DIR
        )
        self.profile = Profile(
            type="snowflake", name="snowflake", spec=Spec(**self.conn_params)
        )

    async def ingest(self) -> JobResult:
        LOGGER.info("Running ingest")

        with use_dir(self.JOBS_DIR):
            time_start = datetime.now(timezone.utc)
            # run all jobs i directory
            if self.job_file_name is None:
                result = jobs.run_jobs(self.JOBS_DIR, self.DBT_PROFILES_DIR)
            else:  # run jobs in one file
                result = jobs.run_job(self.job_file_name, self.DBT_PROFILES_DIR)
            time_end = datetime.now(timezone.utc)

            # persist jobs metadata
            write_pydantic_to_db(
                self.profile,
                f"{self.db}.{self.metadata_schema}.ingest_run",
                self.job_id,
                time_start,
                time_end,
                result,
                self.connection,
            )
            return result

    async def transform(self) -> JobResult:
        LOGGER.info("Running transformations")

        try:
            with use_dir(self.DBT_DIR):
                await _run_process(
                    f"source activate && dbt run --profiles-dir {self.DBT_PROFILES_DIR} --target {self.DBT_TARGET}"
                )
                return JobResult(result="DONE")
        except:
            return JobResult(result="FAILED")

    async def dbt_generate_docs(self) -> JobResult:
        LOGGER.info("Storing metadata")

        time_start = datetime.now(timezone.utc)
        with use_dir(self.DBT_DIR):
            res = await _run_process(
                f"source activate && dbt docs generate --profiles-dir {self.DBT_PROFILES_DIR} --target {self.DBT_TARGET}  --target-path {self.TEMP_DIR}"
            )
            time_end = datetime.now(timezone.utc)

            # publish dbt results to Snowflake
            with open(f"{self.TEMP_DIR}/run_results.json") as run_results:
                result = json.dumps(json.load(run_results))

                write_metadata_json_to_db(
                    self.profile,
                    f"{self.db}.{self.metadata_schema}.transform_run",
                    self.job_id,
                    time_start,
                    time_end,
                    result,
                    self.connection,
                )

            # publish dbt catalog to Snowflake
            with open(f"{self.TEMP_DIR}/catalog.json") as catalog:
                result = json.dumps(json.load(catalog))

                write_metadata_json_to_db(
                    self.profile,
                    f"{self.db}.{self.metadata_schema}.dbt_catalog",
                    self.job_id,
                    time_start,
                    time_end,
                    result,
                    self.connection,
                )

            # publish dbt manifest to GCS and write url to Snowflake
            try:
                bucket = self.GCS_BUCKET
                blob = "dbt_manifest"
                format = "json"
                prefix = self.job_id
                profile = Profile(
                    spec=Spec(blob=blob, bucket=bucket, prefix=prefix, format=format)
                )
                with GCSConnection(profile=profile) as gcs:
                    gcs.upload_from_filename(f"{self.TEMP_DIR}/manifest.json")

                write_metadata_text_to_db(
                    self.profile,
                    f"{self.db}.{self.metadata_schema}.dbt_manifest",
                    self.job_id,
                    time_start,
                    time_end,
                    f"gs://{bucket}/{self.job_id}/{blob}.json",
                    self.connection,
                )
                return JobResult(result="DONE")
            except:
                return JobResult(result="FAILED")

    async def run_soda_tests(self) -> JobResult:
        LOGGER.info("Running soda tests")

        try:
            scan = self.soda()
            scan.disable_telemetry()
            scan.set_data_source_name("faktura")
            """  soda_profile = soda_profile_yml(
                "data_source faktura", "snowflake_faktura", "loader", "./dbt"
            ) """
            soda_profile = get_soda_profile(self.profile)
            scan.add_configuration_yaml_str(soda_profile)
            scan.add_sodacl_yaml_files("./soda")
            scan.set_verbose(False)

            time_test_start = datetime.now(timezone.utc)

            scan.execute()

            result = json.dumps(scan.get_scan_results())

            time_test_end = datetime.now(timezone.utc)

            write_metadata_json_to_db(
                self.profile,
                f"{self.db}.meta.soda_run",
                self.job_id,
                time_test_start,
                time_test_end,
                result,
                self.connection,
            )
            return JobResult(result="DONE")
        except:
            return JobResult(result="FAILED")

    async def run(self, request: Request = None) -> JobResult:
        time_start = datetime.now(timezone.utc)

        results = {}
        result = True
        res = JobResult(result="FAILED")

        # ingest data
        if Actions.INGEST in self.actions:
            res = await self.ingest()
            results["ingest"] = res.to_json()
            LOGGER.info(res)
            if request and res.result == "DONE":
                request.app.state.status[self.job_id] = "INGEST DONE"
            if res.result != "DONE":
                result = False

        if res.result == "DONE" or Actions.INGEST not in self.actions:
            # transform data
            if Actions.TRANSFORM in self.actions:
                res = await self.transform()
                results["transform"] = res.to_json()
                if request and res.result == "DONE":
                    request.app.state.status[self.job_id] = "TRANSFORM DONE"
                if res.result != "DONE":
                    result = False
                LOGGER.info(res)

            # generate docs
            if Actions.METADATA in self.actions:
                res = await self.dbt_generate_docs()
                results["metadata"] = res.to_json()
                if request and res.result == "DONE":
                    request.app.state.status[self.job_id] = "METADATA DONE"
                LOGGER.info(res)

            # run soda tests
            if Actions.SODA in self.actions:
                res = await self.run_soda_tests()
                results["soda"] = res.to_json()
                if request and res.result == "DONE":
                    request.app.state.status[self.job_id] = "SODA DONE"
                if res.result != "DONE":
                    result = False
                LOGGER.info(res)

            # signal job completed
            LOGGER.info("Job completed")
            if request is not None:
                request.app.state.status[self.job_id] = "DONE"

        # persist job results
        time_end = datetime.now(timezone.utc)

        write_job_run_result_to_db(
            self.profile,
            self.job_id,
            time_start,
            time_end,
            json.dumps(results, default=str),
            str(result),
            "",
        )

        return res
