"""
my_project.readers subpackage

Contains the DBReader protocol, concrete readers for different databases,
and the ConnectionFactory for creating those readers.
"""

from .base_reader import DBReader
from .postgres_reader import PostgresDBReader
from .oracle_reader import OracleDBReader
from .connection_factory import ConnectionFactory
from logging_config import (setup_logging,get_logger)


__all__ = [
    "DBReader",
    "PostgresDBReader",
    "OracleDBReader",
    "ConnectionFactory",
]
