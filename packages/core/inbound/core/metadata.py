import hashlib
import json
import time
import tracemalloc
from collections import defaultdict
from datetime import datetime, timezone
from typing import Tuple

import pandas

from inbound.core.connection import Connection
from inbound.core.job_result import JobResult
from inbound.core.logging import LOGGER
from inbound.core.models import Profile, Spec
from inbound.core.package import get_pacage_name, get_package_version


def get_metadata_table_name(profile: Profile, table: str) -> str:
    if profile.type == "duckdb":
        return table.rsplit(".", 1)[1]
    return table


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
    metadata_table = get_metadata_table_name(profile, table)
    try:
        LOGGER.info(f"Persisting job metadata to table {metadata_table}")
        with connection as db:
            if profile.type == "snowflake":
                if profile.spec.warehouse is not None:
                    LOGGER.info(f"Use snowflake warehouse {profile.spec.warehouse}")
                    db.execute(f"use warehouse {profile.spec.warehouse}")
                db.execute(
                    f"""
                    create table if not exists 
                    {metadata_table}(job_id string, time_start timestamp_ltz, time_end timestamp_ltz, raw variant)
                    """
                )
                db.execute(
                    f"""insert into {metadata_table}(job_id, time_start, time_end, raw)
                        select '{job_id}', '{time_start}', '{time_end}', parse_json($${blob.json()}$$);
                    """
                )
            else:
                db.execute(
                    f"""
                    create table if not exists 
                    {metadata_table}(job_id string, time_start timestamp_ltz, time_end timestamp_ltz, raw JSON)
                    """
                )
                db.execute(
                    f"""insert into {metadata_table}(job_id, time_start, time_end, raw)
                        values ('{job_id}', '{time_start}', '{time_end}', '{blob.json()}');
                    """
                )
    except Exception as e:
        LOGGER.error(
            f"Error persisting job pydantic metadata to table {metadata_table} with profile {profile.type}.{e}"
        )


def write_metadata_json_to_db(
    profile: Profile,
    table: str,
    job_id: str,
    time_start: datetime,
    time_end: datetime,
    blob: dict,
    connection: Connection,
):
    if connection is None:
        LOGGER.error("Please provide a valid db connection class")
        return

    time_end = datetime.now(timezone.utc)
    metadata_table = get_metadata_table_name(profile, table)

    try:
        LOGGER.info(f"Persisting metadata json to table {metadata_table}")
        with connection as db:
            if profile.type == "snowflake":
                if profile.spec.warehouse is not None:
                    LOGGER.info(f"Use snowflake warehouse {profile.spec.warehouse}")
                    db.execute(f"use warehouse {profile.spec.warehouse}")
                db.execute(
                    f"""
                    create table if not exists 
                    {metadata_table}(job_id string, time_start timestamp_ltz default current_timestamp, time_end timestamp_ltz default current_timestamp, raw variant)
                    """
                )
                db.execute(
                    f"""insert into {metadata_table}(job_id, time_start, time_end, raw)
                        select '{job_id}', '{time_start}', '{time_end}', parse_json($${json.dumps(blob)}$$);
                    """
                )
            else:
                db.execute(
                    f"""
                    create table if not exists 
                    {metadata_table}(job_id string, time_start timestamptz, time_end timestamptz, raw JSON)
                    """
                )
                db.execute(
                    f"""insert into {metadata_table}(job_id, time_start, time_end, raw)
                        values ('{job_id}', '{time_start}', '{time_end}', '{blob}');
                    """
                )
    except Exception as e:
        LOGGER.error(
            f"Error persisting metadata json to table {metadata_table} with profile {profile.type}.{e}"
        )


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
    metadata_table = get_metadata_table_name(profile, table)
    try:
        LOGGER.info(f"Persisting metadata text to table {metadata_table}")
        with connection as db:
            if profile.type == "snowflake" and profile.spec.warehouse is not None:
                LOGGER.info(f"Use snowflake warehouse {profile.spec.warehouse}")
                db.execute(f"use warehouse {profile.spec.warehouse}")
            db.execute(
                f"""
                create table if not exists 
                {metadata_table}(job_id string, time_start timestamp_ltz default current_timestamp,time_end timestamp_ltz default current_timestamp, raw text)
                """
            )
            db.execute(
                f"""insert into {metadata_table}(job_id, time_start, time_end, raw)
                    values ('{job_id}', '{time_start}', '{time_end}', '{text}');
                """
            )
    except Exception as e:
        LOGGER.error(
            f"Error persisting metadata text to table {metadata_table} with profile {profile.type}.{e}"
        )


def write_job_run_result_to_db(
    profile: Profile,
    job_id: str,
    time_start: datetime,
    time_end: datetime,
    actions: str,
    success: str,
    message: str,
    connection: Connection,
):
    time_end = datetime.now(timezone.utc)
    metadata_table = get_metadata_table_name(profile, "meta.job_run")
    try:
        LOGGER.info(
            f"Persisting job result to db. Profile: {profile.type}. Table: {metadata_table}"
        )
        with connection as db:
            if profile.type == "snowflake" and profile.spec.warehouse is not None:
                LOGGER.info(f"Use snowflake warehouse {profile.spec.warehouse}")
                db.execute(f"use warehouse {profile.spec.warehouse}")
            db.execute(
                f"""
                create table if not exists 
                {metadata_table}(job_id string, time_start timestamp_ltz default current_timestamp,time_end timestamp_ltz default current_timestamp, actions text, success text, message text)
                """
            )
            db.execute(
                f"""insert into {metadata_table}(job_id, time_start, time_end, actions, success, message)
                    select '{job_id}', '{time_start}', '{time_end}', '{actions}', '{success}', '{message}';
                """
            )
    except Exception as e:
        LOGGER.info(
            f"Error persisting job result to db. Profile type: {profile.type}. Table: {metadata_table}. {e}"
        )


def enriched_with_metadata(
    spec: Spec, df: pandas.DataFrame, job_id: str = None
) -> Tuple[pandas.DataFrame, JobResult]:
    start_date_time = datetime.datetime.now()
    job_res = JobResult(job_id=job_id, start_date_time=start_date_time)
    if df.empty:
        return df, job_res

    if spec.format == "meta+json" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame(index=range(len(df)))
        for key, value in spec.meta.items():
            df_out[key] = str(value)
        df_out["loaded"] = start_date_time
        df_out["data"] = df.to_dict("records")

        job_res.task_name = "Enrich dataframe. Format: meta+json"
        job_res.end_date_time = datetime.datetime.now()
        job_res.memory = tracemalloc.get_traced_memory()
        job_res.result = "DONE"

        return df_out, job_res

    if spec.format == "meta" and type(spec.meta) == defaultdict:
        df_out = pandas.DataFrame()
        for key, value in spec.meta.items():
            df_out[key] = value
        df_out["loaded"] = start_date_time

        job_res.task_name = "Enrich dataframe. Format: meta"
        job_res.rows = df_out.size
        job_res.end_date_time = datetime.datetime.now()
        job_res.memory = tracemalloc.get_traced_memory()
        job_res.result = "DONE"

        return df_out.concat(df), job_res

    if spec.format == "log":
        try:
            df_out = pandas.DataFrame()

            if spec.row_id:
                if type(spec.row_id) is str:
                    df_out["ROW_ID"] = df[spec.row_id]
                elif all(isinstance(s, str) for s in spec.row_id):
                    df_out["ROW_ID"] = (
                        df[[x for x in df.columns if x in spec.row_id]]
                        .apply(lambda x: "_".join(x.astype(str)), axis=1)
                        .replace(" ", "_")
                    )
            else:
                for id_col in ["id", "ID"]:
                    if id_col in df.columns:
                        df_out["ROW_ID"] = df[id_col]
            df_out["RAW"] = df.to_json(
                orient="records", lines=True, force_ascii=False, date_format="iso"
            ).splitlines()

            df_out["SOURCE"] = spec.source
            df_out["INTERFACE"] = spec.interface
            df_out["LOADER"] = get_pacage_name() + "-" + get_package_version()
            df_out["JOB_ID"] = job_id
            df_out["LOAD_TIME"] = datetime.datetime.now().timestamp()
            df_out["HASH"] = [
                hashlib.md5(data.encode("utf-8")).hexdigest() for data in df_out["RAW"]
            ]

            job_res.task_name = "Enrich dataframe. Format: log"
            job_res.rows = df_out.size
            job_res.end_date_time = datetime.datetime.now()
            job_res.memory = tracemalloc.get_traced_memory()
            job_res.result = "DONE"

            df = df_out
            return df, job_res

        except Exception as e:
            LOGGER.error(f"Error converting dataframe to log format. {e}")
            return pandas.DataFrame, JobResult("FAILED")

    else:
        job_res.task_name = "Process: skip"
        job_res.end_date_time = datetime.datetime.now()
        job_res.memory = tracemalloc.get_traced_memory()
        job_res.result = "DONE"
        return df, job_res
