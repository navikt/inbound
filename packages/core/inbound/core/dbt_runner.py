import os
from datetime import datetime, timezone
from typing import List

from dbt.cli.main import dbtRunner, dbtRunnerResult

from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.metadata import write_metadata_json_to_db
from inbound.core.utils import use_dir


class DbtRunner:
    def __init__(
        self,
        dbt_dir: str,
        dbt_target: str,
        temp_dir: str = None,
    ):
        self.dbt_dir = dbt_dir
        self.dbt_target = dbt_target
        self.output_dir = temp_dir or os.getenv("DBT_TARGET_PATH")

    def run_dbt_method(self, method: List[str]) -> dbtRunnerResult:
        with use_dir(self.dbt_dir):
            dbt = dbtRunner()
            args = method + [
                "--target",
                self.dbt_target,
                "--target-path",
                self.output_dir,
            ]
        res: dbtRunnerResult = dbt.invoke(args)
        return res

    def transform(self) -> JobResult:
        LOGGER.info("Running dbt transformations")
        time_start = datetime.now(timezone.utc)
        try:
            self.run_dbt_method(["run"])
            time_end = datetime.now(timezone.utc)
            return JobResult(result="DONE", time_start=time_start, time_end=time_end)
        except Exception as e:
            LOGGER.info(f"Error running dbt transformations. {e}")
            return JobResult(result="FAILED")

    def run_dbt_tests(self) -> JobResult:
        LOGGER.info("Running dbt tests")
        time_start = datetime.now(timezone.utc)
        try:
            res = self.run_dbt_method(["test"])
            time_end = datetime.now(timezone.utc)

            table = f"{self.metadata_db}.{self.metadata_schema}.dbt_tests"
            write_metadata_json_to_db(
                self.metadata_profile,
                table,
                self.job_id,
                time_start,
                time_end,
                res.result.to_dict(),
                self.connection,
            )
            return JobResult(result="DONE", time_start=time_start, time_end=time_end)
        except Exception as e:
            LOGGER.info(f"Error running dbt tests. {e}")
            return JobResult(result="FAILED")

    def generate_dbt_artifacts(self) -> JobResult:
        LOGGER.info(f"Generting dbt docs")
        time_start = datetime.now(timezone.utc)
        try:
            res = self.run_dbt_method(["docs", "generate"])
            time_end = datetime.now(timezone.utc)
            if not res.success:
                LOGGER.error(
                    f"Error generating dbt docs. Success: {res.success}. Out dir: {self.output_dir}"
                )
                return JobResult(result="FAILED")

            return JobResult(result="DONE", time_start=time_start, time_end=time_end)
        except Exception as e:
            LOGGER.error(f"Error generating dbt docs. {e}")
            return JobResult(result="FAILED")
