# readers/oracle_reader.py

from dataclasses import dataclass, field
from typing import Any, Dict, List, Tuple, Optional
import cx_Oracle
import pandas as pd
from .base_reader import DBReader
from DBConnections.oracle_kerberose_pool import OracleKerberosPool
from logging_config import get_logger

logger = get_logger(__name__)


@dataclass
class OracleDBReader(DBReader[cx_Oracle.Connection]):
    """
    DBReader for Oracle using cx_Oracle SessionPool.

    Example config:
      db_type: "oracle"
      dsn: "hostname/orclpdb1"
      user: "oracle_user"
      password: "oracle_password"
      pooling:
        min: 1
        max: 5
        increment: 1
      sql: "SELECT * FROM my_table WHERE id = :1"  # Optional
      params: [123]  # Optional
    """
    dsn: str
    pooling: Dict[str, Any]
    use_kerberos: bool = True,
    external_auth = True,
    sql: Optional[str] = None  # Optional SQL query provided at initialization
    params: Optional[List[Any]] = None  # Optional params for the SQL query

    _session_pool: Optional[cx_Oracle.SessionPool] = field(default=None, init=False)

    def __post_init__(self):
        """Validate parameters and convert params to tuple if needed"""
        logger.info(f"[OracleDBReader] Initialized for {self.dsn}")

        # Convert params from list to tuple if needed
        if self.params is not None and not isinstance(self.params, tuple):
            self.params = tuple(self.params)
            logger.debug("[OracleDBReader] Converted params to tuple")

    def connect(self) -> cx_Oracle.Connection:
        """
        Get an Oracle connection from the session pool.
        Creates the pool if it doesn't exist yet.
        """
        if self._session_pool is None:
            min_sess = self.pooling.get("min", 1)
            max_sess = self.pooling.get("max", 5)
            increment = self.pooling.get("increment", 1)

            logger.info(
                f"[OracleDBReader] Initializing SessionPool: min={min_sess}, max={max_sess}, increment={increment}")
            self._session_pool = OracleKerberosPool(
                dsn=self.dsn,
                min=min_sess,
                max=max_sess,
                increment=increment,
                external_auth=True,
                threaded=True,
                getmode=cx_Oracle.SPOOL_ATTRVAL_NOWAIT
            )
        logger.debug("[OracleDBReader] Acquiring a connection from the pool.")
        return self._session_pool.getconn()

    def execute_query(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Execute a SQL query with Oracle bind variables.

        Parameters:
        -----------
        query : str, optional
            SQL query to execute. If None, uses the sql attribute
        params : tuple, optional
            Parameters for the query. If None, uses the params attribute

        Returns:
        --------
        List[Tuple[Any, ...]]
            Rows returned by the query (empty list for non-query statements)
        """
        # Use provided query/params or instance attributes
        sql = query if query is not None else self.sql
        parameters = params if params is not None else self.params

        if sql is None:
            raise ValueError("[OracleDBReader] No SQL query provided")

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                logger.debug(f"[OracleDBReader] Executing: {sql} | params={parameters}")
                cur.execute(sql, parameters or ())
                if cur.description:
                    rows = cur.fetchall()
                    logger.info(f"[OracleDBReader] Query returned {len(rows)} rows.")
                    return rows
                else:
                    conn.commit()
                    logger.info("[OracleDBReader] No result set. Transaction committed.")
                    return []
        except Exception as e:
            logger.error(f"[OracleDBReader] Error executing query: {e}")
            raise
        finally:
            if self._session_pool:
                self._session_pool.putconn(conn)

    def fetch_as_dataframe(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a pandas DataFrame.

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
            raise ValueError("[OracleDBReader] No SQL query provided")

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                logger.debug(f"[OracleDBReader] fetch_as_dataframe: {sql}, {parameters}")
                cur.execute(sql, parameters or ())
                rows = cur.fetchall()

                if not rows:
                    logger.info("[OracleDBReader] No rows returned. Empty DataFrame.")
                    return pd.DataFrame()

                col_names = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=col_names)
                logger.info(f"[OracleDBReader] DataFrame shape: {df.shape}")
                return df
        except Exception as e:
            logger.error(f"[OracleDBReader] Error fetching DataFrame: {e}")
            raise
        finally:
            if self._session_pool:
                self._session_pool.putconn(conn)

    def close(self) -> None:
        """
        Close the Oracle session pool.
        """
        if self._session_pool:
            logger.info("[OracleDBReader] Closing session pool.")
            self._session_pool.closeall()
            self._session_pool = None