# readers/base_reader.py


from typing import Protocol, TypeVar, Generic, Any, List, Tuple, Optional
import pandas as pd
#from numpy import PyArray_SearchSorted

from python_project_generics.logging_config import get_logger


logger = get_logger(__name__)

TConn = TypeVar("TConn")

class DBReader(Protocol[TConn], Generic[TConn]):
    """
    A generic interface for reading data from a database.

    Methods:
        connect() -> TConn:
            Obtain the underlying connection object (psycopg2 or cx_Oracle).

        execute_query(query: str, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
            Execute a query (SELECT) with placeholders. Returns rows if any.

        fetch_as_dataframe(query: str, params: Optional[Tuple[Any, ...]] = None) -> pd.DataFrame:
            SELECT query -> DataFrame.

        close() -> None:
            Clean up resources (close connection pool, etc.).
    """

    def connect(self) -> TConn:
        pass

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        pass

    def fetch_as_dataframe(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        pass

    def close(self) -> None:
        pass
