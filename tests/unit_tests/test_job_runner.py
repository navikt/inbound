import glob
import os
import tempfile
from pathlib import Path

import duckdb

from inbound.core import job_runner


def test_jobrunner(data_path):
    inbound_profiles_dir = os.getenv("INBOUND_PROFILES_DIR")
    os.environ["INBOUND_PROFILES_DIR"] = f"{data_path}/jobrunner/inbound"

    inbound_jobs_dir = os.getenv("INBOUND_JOBS_DIR")
    os.environ["INBOUND_JOBS_DIR"] = f"{data_path}/jobrunner/inbound/jobs"

    inbound_gcs_bucket = os.getenv("INBOUND_GCS_BUCKET")
    os.environ["INBOUND_GCS_BUCKET"] = "artifacts_dev"

    dbt_dir = os.getenv("DBT_DIR")
    os.environ["DBT_DIR"] = f"{data_path}/jobrunner/dbt"

    dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    os.environ["DBT_PROFILES_DIR"] = f"{data_path}/jobrunner/dbt"

    dbt_project_dir = os.getenv("DBT_PROJECT_DIR")
    os.environ["DBT_PROJECT_DIR"] = f"{data_path}/jobrunner/dbt"

    dbt_target = os.getenv("DBT_TARGET")
    os.environ["DBT_TARGET"] = "transformer"

    env_path = os.getenv("VIRTUAL_ENVIRONMENT_PATH")
    os.environ["VIRTUAL_ENVIRONMENT_PATH"] = str(
        Path.cwd().parent.parent / ".venv" / "bin"
    )

    data_source_file = os.getenv("DATA_SOURCE_FILE")
    os.environ["DATA_SOURCE_FILE"] = f"{data_path}/test_jobrunner.csv"

    database_file = os.getenv("DATABASE_FILE")
    os.environ["DATABASE_FILE"] = f"{data_path}/test.duckdb"
    for f in glob.glob(f"{data_path}/test.duckdb*"):
        os.remove(f)
    duckdb.connect(database=os.environ["DATABASE_FILE"])

    try:
        runner = job_runner.JobRunner(
            profile="inbound_test",
            target="dev",
            actions=[job_runner.Actions.INGEST, job_runner.Actions.TRANSFORM],
        )
        runner.run()

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

        if dbt_project_dir is not None:
            os.environ["DBT_PROJECT_DIR"] = dbt_project_dir

        if dbt_target is not None:
            os.environ["DBT_TARGET"] = dbt_target

        if env_path is not None:
            os.environ["VIRTUAL_ENVIRONMENT_PATH"] = env_path

        if data_source_file is not None:
            os.environ["DATA_SOURCE_FILE"] = data_source_file

        if database_file is not None:
            os.environ["DATABASE_FILE"] = database_file


# test_jobrunner(Path.cwd().parent / "data")
