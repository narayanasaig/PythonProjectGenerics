# readers/base_reader.py

from typing import Protocol, TypeVar, Generic, Any, List, Tuple, Optional
import pandas as pd

from python_project_generics.logging_config import get_logger

logger = get_logger(__name__)

TConn = TypeVar("TConn")


class DBReader(Protocol[TConn], Generic[TConn]):
    """
    A generic interface for reading data from various sources.

    This protocol defines the common methods that all readers must implement,
    regardless of whether they connect to databases (PostgreSQL, Oracle) or
    file storage systems (S3).

    Methods:
        connect() -> TConn:
            Obtain the underlying connection object.

        execute_query(query: Optional[str] = None, params: Optional[Tuple[Any, ...]] = None) -> List[Tuple[Any, ...]]:
            Execute a query with placeholders. Returns rows if any.

        fetch_as_dataframe(query: Optional[str] = None, params: Optional[Tuple[Any, ...]] = None) -> pd.DataFrame:
            Fetch data and return as DataFrame.

        close() -> None:
            Clean up resources (close connection pool, etc.).
    """

    def connect(self) -> TConn:
        """
        Establish a connection to the data source.

        Returns:
            The connection object specific to the data source.
        """
        ...

    def execute_query(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Execute a query on the data source.

        Parameters:
            query: The query to execute. If None, uses a pre-configured query.
            params: Parameters for the query. If None, uses pre-configured parameters.

        Returns:
            List of result rows. Empty list for non-query operations or no results.
        """
        ...

    def fetch_as_dataframe(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.

        Parameters:
            query: The query to execute. If None, uses a pre-configured query.
            params: Parameters for the query. If None, uses pre-configured parameters.

        Returns:
            DataFrame containing the query results or file contents.
        """
        ...

    def close(self) -> None:
        """
        Close the connection and release resources.
        """
        ...