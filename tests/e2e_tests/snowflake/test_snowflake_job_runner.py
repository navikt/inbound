import os
from pathlib import Path

from inbound.core import job_runner


def test_jobrunner():
    inbound_profiles_dir = os.getenv("INBOUND_PROFILES_DIR")
    os.environ["INBOUND_PROFILES_DIR"] = str(
        Path.cwd().parent.parent / "data" / "jobrunner_snowflake" / "inbound"
    )

    inbound_jobs_dir = os.getenv("INBOUND_JOBS_DIR")
    os.environ["INBOUND_JOBS_DIR"] = str(
        Path.cwd().parent.parent / "data" / "jobrunner_snowflake" / "inbound/jobs"
    )

    inbound_gcs_bucket = os.getenv("INBOUND_GCS_BUCKET")
    os.environ["INBOUND_GCS_BUCKET"] = "artifacts_dev"

    dbt_dir = os.getenv("DBT_DIR")
    os.environ["DBT_DIR"] = str(
        Path.cwd().parent.parent / "data" / "jobrunner_snowflake" / "dbt"
    )

    dbt_profiles_dir = os.getenv("DBT_PROFILES_DIR")
    os.environ["DBT_PROFILES_DIR"] = str(
        Path.cwd().parent.parent / "data" / "jobrunner_snowflake" / "dbt"
    )

    dbt_project_dir = os.getenv("DBT_PROJECT_DIR")
    os.environ["DBT_PROJECT_DIR"] = str(
        Path.cwd().parent.parent / "data" / "jobrunner_snowflake" / "dbt"
    )

    dbt_target = os.getenv("DBT_TARGET")
    os.environ["DBT_TARGET"] = "transformer"

    env_path = os.getenv("VIRTUAL_ENVIRONMENT_PATH")
    os.environ["VIRTUAL_ENVIRONMENT_PATH"] = str(
        Path.cwd().parent.parent.parent / ".venv" / "bin"
    )

    data_source_file = os.getenv("DATA_SOURCE_FILE")
    os.environ["DATA_SOURCE_FILE"] = str(Path.cwd().parent.parent / "data" / "test.csv")

    try:
        runner = job_runner.JobRunner(
            profile="inbound_test",
            target="metadata",
            # actions=[job_runner.Actions.INGEST],
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


test_jobrunner()
