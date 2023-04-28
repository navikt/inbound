import datetime
import os
import sys
import tracemalloc
from typing import Any, Tuple

import oracledb
import pandas

from inbound.core import JobResult, Profile, connection_factory, logging
from inbound.core.models import SyncMode
from inbound.sqlalchemy import SQLAlchemyConnection

LOGGER = logging.LOGGER


class OracleConnection(SQLAlchemyConnection):
    def __init__(self, profile: Profile):
        super().__init__(profile)

        self.engine = None
        self.connection = None
        self.method = "multi"

        # TODO: delete when upgraded to sqlalchemy 2.0
        if (
            self.profile.spec.connection_string
            and "oracle" in self.profile.spec.connection_string
            and not "cx_oracle" in self.profile.spec.connection_string
        ):
            oracledb.version = "8.3.0"
            sys.modules["cx_Oracle"] = oracledb

        if (
            self.profile.spec.connection_string
            and "oracle" in self.profile.spec.connection_string
            and "cx_oracle" in self.profile.spec.connection_string
        ):
            try:
                oracledb.version = "8.3.0"
                if os.environ.get("INBOUND_ORACLE_CLIENT_LIB_PATH") is not None:
                    oracledb.init_oracle_client(
                        os.environ.get("INBOUND_ORACLE_CLIENT_LIB_PATH")
                    )
                else:
                    oracledb.init_oracle_client()
            except Exception as e:
                LOGGER.error(
                    f"Error. Please make sure the cx_Oracle module and client libraries are installed. {e}"
                )

    def to_sql(
        self,
        df: pandas.DataFrame,
        table: str,
    ) -> None:
        try:
            df.to_sql(
                table,
                con=self.connection,
                index=False,
                if_exists="append",
                method=self.method,
            )
        except:
            # Oracle xe does not support multi row inserts
            self.method = None
            df.to_sql(
                table,
                con=self.connection,
                index=False,
                if_exists="append",
                method=self.method,
            )


def register() -> None:
    """Register connector"""
    connection_factory.register("oracle", OracleConnection)
