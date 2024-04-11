import os
from datetime import datetime

import oracledb
import snowflake.connector

from inbound.core.job import Job
from inbound.core.models import Metadata
from inbound.mappers import OraToSnowDescriptionMapper
from inbound.sinks.snowflake import SnowHandler, SnowSink, snow_generate_highwatermark
from inbound.taps.oracle import OraTap

oracledb.init_oracle_client()
oracledb.defaults.fetch_decimals = True


def full_table_name(sink_table: str) -> str:
    sink_db = os.environ["REGNSKAP_RAW_DB"]
    sink_schema = "oebs"
    return f"{sink_db}.{sink_schema}.{sink_table}"


def last_fra_oebs_til_regnskap_raw(
    tap_query: str,
    sink_table: str,
    job_id: str,
    job_name: str,
    append: bool,
    highwatermark_query: str = None,
):
    table = full_table_name(sink_table=sink_table)
    if not append:
        table = f"{table}__transient"

    snow_config = {
        "user": os.environ["SRV_USR"],
        "password": os.environ["SRV_PWD"],
        "account": "wx23413.europe-west4.gcp",
        "role": "regnskap_loader",
        "warehouse": "regnskap_loader",
    }
    oebs_config = {
        "dsn": os.environ["OEBS_DSN"],
        "user": os.environ["OEBS_USR"],
        "password": os.environ["OEBS_PWD"],
    }

    with oracledb.connect(**oebs_config) as ora_connection:
        with snowflake.connector.connect(**snow_config) as snow_connection:
            highwatermarks = [{}]
            if highwatermark_query is not None:
                highwatermarks = snow_generate_highwatermark(
                    connection=snow_connection, query=highwatermark_query
                )
            if len(highwatermarks) == 0:
                run_start = datetime.now()
                return {
                    "tap": None,
                    "sink": None,
                    "start": run_start,
                    "stop": run_start,
                    "columns": None,
                    "batches": None,
                }
            tap = OraTap(
                connection=ora_connection,
                query=tap_query,
                highwatermarks=highwatermarks,
            )
            handler = SnowHandler(connection=snow_connection)
            sink = SnowSink(connection_handler=handler, table=table, append=append)
            mapper = OraToSnowDescriptionMapper()
            metadata = Metadata(
                load_time=datetime.now(),
                source_env=oebs_config.get("dsn"),
                run_id=job_id,
                job_name=job_name,
            )

            job = Job(
                tap=tap,
                sink=sink,
                description_mapper=mapper,
                metadata=metadata,
                raw=False,
            )
            return job.run()


def ingest_suppliers(job_id: str):
    tap_query = "select * from apps.xxrtv_fist_ap_lev_v"
    sink_table = "xxrtv_fist_ap_lev_v"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="suppliers",
    )


def ingest_customers(job_id: str):
    tap_query = "select * from apps.xxrtv_fist_ar_kun_v"
    sink_table = "xxrtv_fist_ar_kun_v"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="customers",
    )


def ingest_accounts_payable_open(job_id: str):
    tap_query = """
        select *
        from apps.XXRTV_FIST_AP_FAK_GAMMEL_V ap
        where 1=1
        and exists (
            select 1
            from apps.XXRTV_FIST_GL_PERIODE_STATUS_V p
            WHERE p.period_name = ap.REGNSKAPSPERIODE
            AND p.ledger_id = ap.ledger_id
            and p.closing_status != 'P'
            AND p.period_year >= 2023
        )
    """
    sink_table = "xxrtv_fist_ap_fak_v_open"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="accounts_payable_open",
    )


def ingest_accounts_receivable_open(job_id: str):
    tap_query = """
        select *
        from apps.XXRTV_FIST_AR_FAK_GAMMEL_V ap
        where 1=1
        and exists (
            select 1
            from apps.XXRTV_FIST_GL_PERIODE_STATUS_V p
            WHERE p.period_name = ap.REGNSKAPSPERIODE
            AND p.ledger_id = ap.ledger_id
            and p.closing_status != 'P'
            AND p.period_year >= 2023
        )
    """
    sink_table = "xxrtv_fist_ar_fak_v_open"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="accounts_recieveables_open",
    )


def ingest_accounts_receivable_closed(job_id: str):
    tap_query = """
        select *
        from apps.XXRTV_FIST_AR_FAK_GAMMEL_V
        where 1=1
            and regnskapsperiode = '{{ highwatermark["PERIOD_NAME"] }}'
            and ledger_id = {{ highwatermark["LEDGER_ID"] }}
    """
    sink_table = "xxrtv_fist_ar_fak_v_closed"
    full_sink_table_name = full_table_name(sink_table=sink_table)

    highwatermark_table = "xxrtv_fist_gl_periode_status_v__transient"
    full_highwatermark_table_name = full_table_name(sink_table=highwatermark_table)
    highwatermark_query = f"""
        select period_name, ledger_id from {full_highwatermark_table_name} as p
        where closing_status = 'P'
        and period_year >= 2023
        and period_name not like 'AVSL%'
        and not exists (
            select 1 from {full_sink_table_name} as b
            where b.regnskapsperiode = p.period_name
            and b.ledger_id = p.ledger_id
        )
    """
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=True,
        highwatermark_query=highwatermark_query,
        job_name="accounts_recieveables_closed",
    )


def ingest_accounts_payable_closed(job_id: str):
    tap_query = """
        select *
        from apps.XXRTV_FIST_AP_FAK_GAMMEL_V
        where 1=1
            and regnskapsperiode = '{{ highwatermark["PERIOD_NAME"] }}'
            and ledger_id = {{ highwatermark["LEDGER_ID"] }}
    """
    sink_table = "xxrtv_fist_ap_fak_v_closed"
    full_sink_table_name = full_table_name(sink_table=sink_table)

    highwatermark_table = "xxrtv_fist_gl_periode_status_v__transient"
    full_highwatermark_table_name = full_table_name(sink_table=highwatermark_table)
    highwatermark_query = f"""
        select period_name, ledger_id from {full_highwatermark_table_name} as p
        where closing_status = 'P'
        and period_year >= 2023
        and period_name not like 'AVSL%'
        and not exists (
            select 1 from {full_sink_table_name} as b
            where b.regnskapsperiode = p.period_name
            and b.ledger_id = p.ledger_id
        )
    """
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=True,
        highwatermark_query=highwatermark_query,
        job_name="accounts_payable_closed",
    )


def ingest_general_ledger_closed(job_id: str):
    tap_query = """
        select *
        from apps.xxrtv_fist_map_hovedbok_v
        where 1=1
            and actual_flag != 'B'
            and regnskapsperiode = '{{ highwatermark["PERIOD_NAME"] }}'
            and ledger_id = {{ highwatermark["LEDGER_ID"] }}
    """
    sink_table = "xxrtv_fist_map_hovedbok_v_closed"
    full_sink_table_name = full_table_name(sink_table=sink_table)

    highwatermark_table = "xxrtv_fist_gl_periode_status_v__transient"
    full_highwatermark_table_name = full_table_name(sink_table=highwatermark_table)
    highwatermark_query = f"""
        select period_name, ledger_id from {full_highwatermark_table_name} as p
        where closing_status = 'P'
        and period_year >= 2023
        and not exists (
            select 1 from {full_sink_table_name} as b
            where b.regnskapsperiode = p.period_name
            and b.ledger_id = p.ledger_id
        )
    """
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=True,
        highwatermark_query=highwatermark_query,
        job_name="general_ledger_closed",
    )


def ingest_balance_open(job_id: str):
    tap_query = """
        select *
        from apps.xxrtv_fist_map_balanse_v
        where 1=1
            and actual_flag != 'B'
            and closing_status != 'P'
            and periode_start >= to_date('2023-01-01', 'YYYY-MM-DD')
    """
    sink_table = "xxrtv_fist_map_balanse_v_open"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="balance_open",
    )


def ingest_balance_closed(job_id: str):
    tap_query = """
        select *
        from apps.xxrtv_fist_map_balanse_v
        where 1=1
            and actual_flag != 'B'
            and regnskapsperiode = '{{ highwatermark["PERIOD_NAME"] }}'
            and ledger_id = {{ highwatermark["LEDGER_ID"] }}
    """
    sink_table = "xxrtv_fist_map_balanse_v_closed"
    full_sink_table_name = full_table_name(sink_table=sink_table)

    highwatermark_table = "xxrtv_fist_gl_periode_status_v__transient"
    full_highwatermark_table_name = full_table_name(sink_table=highwatermark_table)
    highwatermark_query = f"""
        select period_name, ledger_id from {full_highwatermark_table_name} as p
        where closing_status = 'P'
        and period_year >= 2023
        and not exists (
            select 1 from {full_sink_table_name} as b
            where b.regnskapsperiode = p.period_name
            and b.ledger_id = p.ledger_id
        )
    """
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=True,
        highwatermark_query=highwatermark_query,
        job_name="balance_closed",
    )


def ingest_row_count(job_id: str):
    tap_query = """
        select
            hovedbok,
            regnskapsperiode,
            actual_flag,
            count(*) as n,
            sum(kredit) as kredit,
            sum(debet) as debet
        from apps.xxrtv_fist_map_hovedbok_v
        where periode_start > = to_date('20230101', 'YYYYMMDD')
        group by
            actual_flag,
            hovedbok,
            regnskapsperiode
    """
    sink_table = "hovedbok_n_rows"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="general_ledger_row_count",
    )


def ingest_segment(job_id: str):
    tap_query = "select * from apps.xxrtv_fist_gl_segment_v"
    sink_table = "xxrtv_fist_gl_segment_v"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="segment",
    )


def ingest_periode_status(job_id: str):
    tap_query = "select * from apps.xxrtv_fist_gl_periode_status_v"
    sink_table = "xxrtv_fist_gl_periode_status_v"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="period_status",
    )


def ingest_hierarki(job_id: str):
    tap_query = "select * from apps.xxrtv_gl_hierarki_v"
    sink_table = "xxrtv_gl_hierarki_v"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="hierarchy",
    )


def ingest_general_ledger_open(job_id: str):
    tap_query = f"""
        select *
        from apps.xxrtv_fist_map_hovedbok_v
        where 1=1
            and actual_flag != 'B'
            and closing_status != 'P'
            and periode_start >= to_date('2023-01-01', 'YYYY-MM-DD')
    """
    sink_table = "xxrtv_fist_map_hovedbok_v_open"
    return last_fra_oebs_til_regnskap_raw(
        tap_query=tap_query,
        sink_table=sink_table,
        job_id=job_id,
        append=False,
        job_name="general_ledger_open",
    )
