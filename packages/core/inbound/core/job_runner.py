import json
import os
import shutil
import tempfile
from datetime import datetime, timezone
from enum import Enum
from typing import List, Protocol, Union

from inbound.core import connection_factory, connection_loader, dbt_profile, jobs
from inbound.core.dbt_runner import DbtRunner
from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.metadata import (
    write_job_run_result_to_db,
    write_metadata_json_to_db,
    write_metadata_text_to_db,
    write_pydantic_to_db,
)
from inbound.core.models import Profile, Spec
from inbound.core.utils import generate_id, use_dir
from inbound.gcs import GCSConnection


class Request(Protocol):
    ...


class Actions(Enum):
    INGEST = 1
    TRANSFORM = 2
    TEST = 3
    METADATA = 4


class JobRunner:
    def __init__(
        self,
        profile: str | None = None,
        target: str = os.getenv("DBT_TARGET", "loader"),
        job_file_name: str | None = None,
        actions: List[Actions] = [
            Actions.INGEST,
            Actions.TRANSFORM,
            Actions.TEST,
            Actions.METADATA,
        ],
        job_id: str = None,
        callback: str = None,
    ):
        self.job_file_name = job_file_name
        self.actions = actions
        self.JOBS_DIR = os.getenv("INBOUND_JOBS_DIR", "./inbound/jobs")
        self.DBT_DIR = os.getenv("DBT_DIR", "./dbt")
        self.DBT_TARGET = target
        self.GCS_BUCKET = os.getenv("INBOUND_GCS_BUCKET", None)
        self.TEMP_DIR = tempfile.mkdtemp()

        self.job_id = job_id or generate_id()

        self.metadata_conn_params = dbt_profile.dbt_connection_params(
            profile, target, self.DBT_DIR
        )
        self.metadata_profile = Profile(
            type=self.metadata_conn_params.get("type") or "snowflake",
            name="jobrunner",
            spec=Spec(**self.metadata_conn_params),
        )

        self.metadata_db = self.metadata_conn_params.get("database") or "meta"
        self.metadata_schema = self.metadata_conn_params.get("schema") or "job_runner"

        connection_loader.load_plugins([self.metadata_conn_params["type"]])
        self.connection = connection_factory.create(self.metadata_profile)
        self.callback = callback

        self.dbtRunner = DbtRunner(self.DBT_DIR, self.DBT_TARGET, self.TEMP_DIR)

    def get_job_id(self) -> str:
        return self.job_id

    @staticmethod
    def get_all_jobs(
        jobs_dir: str = os.getenv("INBOUND_JOBS_DIR", "./inbound/jobs")
    ) -> List[str]:
        job_definition_files = [
            os.path.join(d, x)
            for d, _, files in os.walk(jobs_dir)
            for x in files
            if x.endswith(".yml")
        ]
        return job_definition_files

    def ingest(self) -> JobResult:
        LOGGER.info("Running ingest")

        with use_dir(self.JOBS_DIR):
            time_start = datetime.now(timezone.utc)
            # run all jobs i directory
            if self.job_file_name is None:
                result = jobs.run_jobs(self.JOBS_DIR, self.TEMP_DIR)
            else:  # run jobs in one file
                result = jobs.run_job(self.job_file_name, self.TEMP_DIR)
            time_end = datetime.now(timezone.utc)

            table = f"{self.metadata_db}.{self.metadata_schema}.ingest_run"

            # persist jobs metadata
            write_pydantic_to_db(
                self.metadata_profile,
                table,
                self.job_id,
                time_start,
                time_end,
                result,
                self.connection,
            )
            return result

    def upload_dbt_artifacts(self) -> JobResult:
        LOGGER.info("Publish dbt results to metadata database")
        time_start = datetime.now(timezone.utc)
        try:
            with open(f"{self.TEMP_DIR}/run_results.json") as run_results:
                result = json.load(run_results)
                table = f"{self.metadata_db}.{self.metadata_schema}.transform_run"
                time_end = datetime.now(timezone.utc)
                write_metadata_json_to_db(
                    self.metadata_profile,
                    table,
                    self.job_id,
                    time_start,
                    time_end,
                    result,
                    self.connection,
                )
            return JobResult(result="DONE", time_start=time_start, time_end=time_end)
        except Exception as e:
            LOGGER.error(f"Error publishing dbt results to metadata database. {e}")

    def publish_dbt_catalog(self) -> JobResult:
        LOGGER.info("Publish dbt catalog to metadata database")
        time_start = datetime.now(timezone.utc)
        try:
            with open(f"{self.TEMP_DIR}/catalog.json") as catalog:
                result = json.load(catalog)
                table = f"{self.metadata_db}.{self.metadata_schema}.dbt_catalog"
                time_end = datetime.now(timezone.utc)
                write_metadata_json_to_db(
                    self.metadata_profile,
                    table,
                    self.job_id,
                    time_start,
                    time_end,
                    result,
                    self.connection,
                )
            return JobResult(result="DONE", time_start=time_start, time_end=time_end)
        except Exception as e:
            LOGGER.error(f"Error publishing dbt catalog. {e}")
            return JobResult(result="FAILED")

    def publish_dbt_manifest(self) -> JobResult:
        LOGGER.info("Publish dbt manifest to GCS and write url to metadata database")
        time_start = datetime.now(timezone.utc)
        try:
            bucket = self.GCS_BUCKET
            blob = "dbt_manifest"
            format = "json"
            prefix = self.job_id
            gcs_filename = f"gs://{bucket}/{self.job_id}/{blob}.json"
            table = f"{self.metadata_db}.{self.metadata_schema}.dbt_manifest"
            profile = Profile(
                spec=Spec(blob=blob, bucket=bucket, prefix=prefix, format=format)
            )
            with GCSConnection(profile=profile) as gcs:
                gcs.upload_from_filename(f"{self.TEMP_DIR}/manifest.json")

            time_end = datetime.now(timezone.utc)
            write_metadata_text_to_db(
                self.metadata_profile,
                table,
                self.job_id,
                time_start,
                time_end,
                gcs_filename,
                self.connection,
            )
            return JobResult(result="DONE", time_start=time_start, time_end=time_end)
        except Exception as e:
            LOGGER.error(f"Error uploading dbt_manifest to gcs. {e}")
            return JobResult(result="FAILED")

    def run(self, request: Request = None) -> JobResult:
        time_start = datetime.now(timezone.utc)

        results = {}
        result = True
        res = JobResult(result="FAILED")

        # ingest data
        if Actions.INGEST in self.actions:
            res = self.ingest()
            results["ingest"] = res.to_json()
            LOGGER.info(res)
            if request and res.result == "DONE":
                request.app.state.status[self.job_id] = "INGEST DONE"
            if res.result != "DONE":
                result = False

        # transform data
        if Actions.TRANSFORM in self.actions:
            res = self.dbtRunner.transform()
            results["transform"] = res.to_json()
            if request and res.result == "DONE":
                request.app.state.status[self.job_id] = "TRANSFORM DONE"

        # run dbt tests
        if Actions.TEST in self.actions:
            res = self.dbtRunner.run_dbt_tests()
            if request and res.result == "DONE":
                request.app.state.status[self.job_id] = "TESTS DONE"
            LOGGER.info(res)

        # generate docs and publish artifacts
        if Actions.METADATA in self.actions:
            res = self.dbtRunner.generate_dbt_artifacts()
            if res.result == "DONE":
                res = self.upload_dbt_artifacts()
            if res.result == "DONE":
                res = self.publish_dbt_catalog()
            if res.result == "DONE":
                res = self.publish_dbt_manifest()
            if request and res.result == "DONE":
                request.app.state.status[self.job_id] = "METADATA DONE"
            LOGGER.info(res)

        # signal job completed
        LOGGER.info("Job completed")
        if request is not None:
            request.app.state.status[self.job_id] = "DONE"

        # persist job results
        time_end = datetime.now(timezone.utc)
        LOGGER.info(f"Writing metadata to db for job {self.job_id}")
        write_job_run_result_to_db(
            self.metadata_profile,
            self.job_id,
            time_start,
            time_end,
            json.dumps(results, default=str),
            str(result),
            "",
            self.connection,
        )

        try:
            shutil.rmtree(self.TEMP_DIR)
        except Exception as e:
            LOGGER.info(f"Error deleting temp directory: {self.TEMP_DIR}. {e}")

        return JobResult(result="DONE", time_start=time_start, time_end=time_end)
