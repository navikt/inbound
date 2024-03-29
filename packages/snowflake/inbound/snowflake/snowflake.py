import functools
import os
import shutil
from pathlib import Path
from typing import Any, Tuple

import pandas
from snowflake.connector.pandas_tools import pd_writer, write_pandas
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

from inbound.core import JobResult, Profile, connection_factory, logging
from inbound.core.dbt_profile import dbt_connection_params
from inbound.core.models import Profile, Spec
from inbound.gcs import GCSConnection
from inbound.sqlalchemy import SQLAlchemyConnection

LOGGER = logging.LOGGER

sf_conn_params = [
    "account",
    "region",
    "user",
    "password",
    "database",
    "warehouse",
    "role",
    "schema",
]


# TODO : check python connector https://www.snowflake.com/blog/fetching-query-results-from-snowflake-just-got-a-lot-faster-with-apache-arrow/
class SnowflakeConnection(SQLAlchemyConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile)

        # use sqlalchemy conn string if provided
        if profile.spec.connection_string is not None:
            return

        # use dbt profile if provided
        if profile.spec.profile and profile.spec.target:
            try:
                params = dbt_connection_params(
                    profile.spec.profile, profile.spec.target, profile.spec.profiles_dir
                )
                profile.spec.connection_string = URL(**params)
                LOGGER.info(
                    f"Loaded dbt profile for profile={profile.spec.profile} and target={profile.spec.target} with profiles_dir {profile.spec.profiles_dir}."
                )
                LOGGER.info(
                    f"Profile type: {params.get('type')}. Database: : {params.get('database')}. Schema: {params.get('schema')}. User: {params.get('user')}. Warehouse: {params.get('warehouse')}"
                )
                return
            except Exception as e:
                LOGGER.error(
                    f"Error reading dbt profile for profile={profile.spec.profile} and target={profile.spec.target} with profiles_dir {profile.spec.profiles_dir}. {e}"
                )

        # fallback to use parameters provided in spec
        params = dict()
        spec_dict = profile.spec.model_dump(by_alias=True)
        for param in sf_conn_params:
            if param in spec_dict.keys():
                params[param] = spec_dict.get(param)

        try:
            profile.spec.connection_string = URL(**params)
            return
        except Exception as e:
            LOGGER.error("Error creating connection string from {spec}. {e}")

        # TODO: add more auth options

    def from_dir(self, temp_dir_name: str, table: str) -> Tuple[str, JobResult]:
        spec = Spec()
        if self.spec.bucket:
            spec.bucket = self.spec.bucket

        with GCSConnection(Profile(spec=spec)) as gcs:
            blob_name = f"uploads/{os.urandom(24).hex()}"

            try:
                file_names = os.listdir(temp_dir_name)
                for file_name in file_names:
                    blob_name = f"uploads/{file_name}"
                    LOGGER.info(f"Uploading file {file_name} to {blob_name}")
                    file_full_path = str(Path(temp_dir_name) / file_name)
                    res, job_res = gcs.upload_from_filename(file_full_path, blob_name)
                    if not job_res.success:
                        return "FAILED", JobResult()
                    try:
                        path = f"{gcs.root}/{file_name}"
                        sql = f"copy into {table} from '{path}'"

                        self.connection.execute(sql)
                        return "DONE", JobResult(result="DONE")
                    except Exception as e:
                        LOGGER.error(f"Error executing {sql}. {e}")
            except Exception as e:
                LOGGER.error(
                    f"Error copying from dir to snowflake table {table}. Dir {temp_dir_name}. {e}"
                )
                return "FAILED", JobResult()
            finally:
                shutil.rmtree(temp_dir_name)

        return JobResult()

    def to_sql(
        self,
        df: pandas.DataFrame,
        table: str,
    ) -> None:
        LOGGER.info(f"Snowflake: Writing dataframe to table {table}")
        try:
            """with self.connection.engine.raw_connection() as con:
            write_pandas(
                conn=con,
                df=df,
                table_name=table,
                database=con.connection.database,
                schema=con.connection.schema,
                compression="snappy",
                auto_create_table=True,
                # parallel=99
                quote_identifiers=False,
            )"""
            if df.empty:
                return
            df.to_sql(
                table,
                con=self.connection,
                index=False,
                if_exists="append",
                # method="multi",
                method=functools.partial(
                    pd_writer, quote_identifiers=False, auto_create_table=True
                ),
            )
            self.connection.execute("COMMIT")
        except Exception as e:
            LOGGER.info(f"Snowflake: Error writing dataframe to table {table}. {e}")


def register() -> None:
    """Register connector"""
    connection_factory.register("snowflake", SnowflakeConnection)
