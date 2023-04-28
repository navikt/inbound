"""inbound oracle to snowflake connector"""

from .oracle_to_snowflake import sync_table

__all__ = [
    "sync_table",
]
