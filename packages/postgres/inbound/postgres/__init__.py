"""inbound postgres connector"""

from .postgres import PostgresConnection, register

__all__ = ["PostgresConnection", "register"]
