# readers/postgres_reader.py

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Optional
import psycopg2
from asn1crypto.core import Boolean
from psycopg2 import pool
import pandas as pd
from .base_reader import DBReader
from logging_config import get_logger
from DBConnections.postgre_auth_pool import PostgreAuthPool

logger = get_logger(__name__)


@dataclass
class PostgresDBReader(DBReader[psycopg2.extensions.connection]):
    """
    DBReader for Postgres/Aurora Postgres using psycopg2 + connection pooling.

    Provides methods to execute queries and fetch results as dataframes.
    """
    host: str
    port: int
    user: str
    password:  Optional[str]
    database: str
    pooling: Dict[str, Any]
    iam_auth: Optional[Boolean]
    region: str
    sslrootcert: Optional[str]
    sslmode: Optional[str]
    token_ttl: Optional[int]
    sql: Optional[str] = None  # Optional SQL query provided at initialization
    params: Optional[Tuple[Any, ...]] = None  # Optional params for the SQL query
    extras: Dict[str, Any] = field(default_factory=dict)

    _connection_pool: Optional[pool.SimpleConnectionPool] = field(default=None, init=False)

    def __post_init__(self):
        """Validate parameters and convert params to tuple if needed"""
        logger.info(f"[PostgresDBReader] Initialized for {self.database}@{self.host}:{self.port}")

        # Convert params from list to tuple if needed
        if self.params is not None and not isinstance(self.params, tuple):
            self.params = tuple(self.params)
            logger.debug("[PostgresDBReader] Converted params to tuple")

    def connect(self) -> psycopg2.extensions.connection:
        """
        Acquire or initialize the connection pool, return a connection.
        """
        if self._connection_pool is None:
            minconn = self.pooling.get("minconn", 1)
            maxconn = self.pooling.get("maxconn", 5)
            logger.info(f"[PostgresDBReader] Initializing pool: minconn={minconn}, maxconn={maxconn}")
            iam_auth = self.extras.get("iam_auth", False)
            if iam_auth:
                region = self.extras.get("region", "us-east-1")
                sslrootcert = self.extras.get("sslrootcert", None)
                sslmode = self.extras.get("sslmode", "require")
                token_ttl = self.extras.get("token_ttl", 900)
                logger.info(f"sslrootcert: {sslrootcert} ; sslmode: {sslmode} ; token_ttl: {token_ttl}; region: {region} ")
                logger.info("[PostgresDBReader] Using IAM-based PostgreAuthPool.")
                self._connection_pool = PostgreAuthPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    database=self.database,
                    region=region,
                    sslrootcert=sslrootcert,
                    sslmode=sslmode,
                    token_ttl=token_ttl,
                    # pass in any extra psycopg2 args you like
                )
            else:
                logger.info("[PostgresDBReader] Using standard password-based pool.")
                self._connection_pool = pool.SimpleConnectionPool(
                    minconn,
                    maxconn,
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,  # password is used only for standard auth
                    database=self.database,
                    **self.extras
                )
        logger.debug("[PostgresDBReader] Getting connection from pool.")
        return self._connection_pool.getconn()

    def execute_query(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Executes a SQL query with Postgres placeholders (%s).
        Returns rows if SELECT, otherwise an empty list.

        Parameters:
        -----------
        query : str, optional
            SQL query to execute. If None, uses the sql attribute
        params : tuple, optional
            Parameters for the query. If None, uses the params attribute

        Returns:
        --------
        List[Tuple[Any, ...]]
            Rows returned by the query (empty list for non-SELECT queries)
        """
        # Use provided query/params or instance attributes
        sql = query if query is not None else self.sql
        parameters = params if params is not None else self.params

        if sql is None:
            raise ValueError("[PostgresDBReader] No SQL query provided")

        conn = self.connect()
        rows = []

        try:
            with conn.cursor() as cur:
                logger.debug(f"[PostgresDBReader] Executing: {sql} | params={parameters}")
                cur.execute(sql, parameters or ())

                if cur.description:
                    rows = cur.fetchall()
                    logger.info(f"[PostgresDBReader] Query returned {len(rows)} rows.")
                else:
                    conn.commit()
                    logger.info("[PostgresDBReader] No result set. Changes committed.")
        except Exception as e:
            logger.error(f"[PostgresDBReader] Error executing query: {e}")
            if conn and not conn.closed:
                try:
                    conn.rollback()
                except Exception:
                    pass  # Swallow secondary exceptions during cleanup
            raise
        finally:
            if conn and self._connection_pool and not conn.closed:
                try:
                    self._connection_pool.putconn(conn)
                except Exception as e:
                    logger.error(f"[PostgresDBReader] Error returning connection to pool: {e}")

        return rows

    def fetch_as_dataframe(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Executes a query and returns the results as a pandas DataFrame.

        Parameters:
        -----------
        query : str, optional
            SQL query to execute. If None, uses the sql attribute
        params : tuple, optional
            Parameters for the query. If None, uses the params attribute

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing query results
        """
        # Use provided query/params or instance attributes
        sql = query if query is not None else self.sql
        parameters = params if params is not None else self.params

        if sql is None:
            raise ValueError("[PostgresDBReader] No SQL query provided")

        # Get column names and data
        rows = self.execute_query(sql, parameters)

        if not rows:
            logger.info("[PostgresDBReader] No rows returned. Returning empty DataFrame.")
            return pd.DataFrame()

        # Get column names
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, parameters or ())
                col_names = [desc[0] for desc in cur.description]
                logger.debug(f"[PostgresDBReader] Column names: {col_names}")
        finally:
            if self._connection_pool:
                self._connection_pool.putconn(conn)

        # Create DataFrame
        df = pd.DataFrame.from_records(rows, columns=col_names)
        logger.info(f"[PostgresDBReader] Created DataFrame with shape {df.shape}")
        return df

    def close(self) -> None:
        """
        Close the connection pool.
        """
        if self._connection_pool:
            logger.info("[PostgresDBReader] Closing all connections in the pool.")
            self._connection_pool.closeall()
            self._connection_pool = None