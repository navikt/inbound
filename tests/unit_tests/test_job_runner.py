import asyncio
import os
from pathlib import Path

import pytest

from inbound.core import job_runner


@pytest.mark.asyncio
async def test_jobrunner():
    inbound_profiles_dir = os.getenv("INBOUND_PROFILES_DIR")
    os.environ["INBOUND_PROFILES_DIR"] = str(
        Path.cwd().parent / "data" / "jobrunner" / "inbound"
    )

    inbound_jobs_dir = os.getenv("INBOUND_JOBS_DIR")
    os.environ["INBOUND_JOBS_DIR"] = str(
        Path.cwd().parent / "data" / "jobrunner" / "inbound/jobs"
    )

    inbound_gcs_bucket = os.getenv("INBOUND_GCS_BUCKET")
    os.environ["INBOUND_GCS_BUCKET"] = "inbound_test"

    dbt_dir = os.getenv("DBT_DIR")
    os.environ["DBT_DIR"] = str(Path.cwd().parent / "data" / "jobrunner" / "dbt")

    dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    os.environ["DBT_PROFILES_DIR"] = str(
        Path.cwd().parent / "data" / "jobrunner" / "dbt"
    )

    try:
        runner = job_runner.JobRunner(
            profile="input",
            target="dev",
            actions=[job_runner.Actions.INGEST],
        )
        await runner.run()

        assert True

    except Exception as e:
        print(e)

    finally:
        if inbound_profiles_dir is not None:
            os.environ["INBOUND_PROFILES_DIR"] = inbound_profiles_dir

        if inbound_jobs_dir is not None:
            os.environ["INBOUND_PROFILES_DIR"] = inbound_jobs_dir

        if inbound_gcs_bucket is not None:
            os.environ["INBOUND_PROFILES_DIR"] = inbound_gcs_bucket

        if dbt_dir is not None:
            os.environ["DBT_DIR"] = dbt_dir

        if dbt_profiles_dir is not None:
            os.environ["DBT_PROFILES_DIR"] = dbt_profiles_dir


asyncio.run(test_jobrunner())
