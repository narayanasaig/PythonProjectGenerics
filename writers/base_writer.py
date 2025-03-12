# writers/base_writer.py
from typing import Protocol, Any
import pandas as pd

class DBWriter(Protocol):
    """
    A protocol/interface for writing data to various destinations (S3, Postgres, Oracle, etc.)

    Methods:
        connect() -> Any:
            create a connection/session.

        write_as_dataframe(df: pd.DataFrame, **kwargs) -> None:
            Write a pandas DataFrame to the target.

        close() -> None:
            Clean up resources (close connection, etc.).
    """

    def connect(self) -> Any:
        ...

    def write_as_dataframe(self, df: pd.DataFrame, **kwargs) -> None:
        ...

    def close(self) -> None:
        ...
