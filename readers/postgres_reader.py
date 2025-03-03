# readers/postgres_reader.py


from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
import psycopg2
from psycopg2 import pool
import pandas as pd
import awswrangler.postgresql  # pip install awswrangler psycopg2-binary
from .base_reader import DBReader
from python_project_generics.logging_config  import get_logger


logger = get_logger(__name__)

@dataclass
class PostgresDBReader(DBReader[psycopg2.extensions.connection]):
    """
    DBReader for Postgres/Aurora Postgres using psycopg2 + connection pooling.
    Uses AWS Wrangler for DataFrame queries if desired.

    Example config:
      db_type: "postgres"
      host: "localhost"
      port: 5432
      user: "dev_user"
      password: "dev_password"
      database: "dev_db"
      pooling:
        minconn: 1
        maxconn: 5
    """
    host: str
    port: int
    user: str
    password: str
    database: str
    pooling: Dict[str, Any]

    _connection_pool: Optional[pool.SimpleConnectionPool] = None

    def connect(self) -> psycopg2.extensions.connection:
        """
        Acquire or initialize the connection pool, return a connection.
        """
        if self._connection_pool is None:
            minconn = self.pooling.get("minconn", 1)
            maxconn = self.pooling.get("maxconn", 5)
            logger.info(f"[PostgresDBReader] Initializing pool: minconn={minconn}, maxconn={maxconn}")
            self._connection_pool = pool.SimpleConnectionPool(
                minconn,
                maxconn,
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                database=self.database
            )
        logger.debug("[PostgresDBReader] Getting connection from pool.")
        return self._connection_pool.getconn()

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Executes a SQL query with Postgres placeholders (%s).
        Returns rows if SELECT, otherwise an empty list.
        """
        conn = self.connect()
        rows=[]
        try:
            with conn.cursor() as cur:
                logger.debug(f"[PostgresDBReader] Executing: {query} | params={params}")
                cur.execute(query, params)
                if cur.description:
                    rows = cur.fetchall()
                    logger.info(f"[PostgresDBReader] Query returned {len(rows)} rows.")
                    return rows
                else:
                    conn.commit()
                    logger.info("[PostgresDBReader] No result set. Changes committed.")
                    return rows
        except Exception as e:
            logger.error(f"[PostgresDBReader] Error executing query: {e}")
            conn.rollback()
            return rows
        finally:
            if self._connection_pool:
                logger.debug("[PostgresDBReader] Releasing connection back to pool.")
                self._connection_pool.putconn(conn)


    def fetch_as_dataframe(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        fetch a DataFrame from Postgres.
        """
        rows=self.execute_query(query, params)
        logger.info(f"[PostgresDBReader] Fetched {len(rows)} rows.")
        if not rows:
            return pd.DataFrame()

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                col_names=[desc[0] for desc in cur.description]
                logger.info(f"[PostgresDBReader] Fetched {col_names} .")
        finally:
            if self._connection_pool:
                self._connection_pool.putconn(conn)
        return pd.DataFrame.from_records(rows, columns=col_names)


    def close(self) -> None:
        """
        Close the connection pool.
        """
        if self._connection_pool:
            logger.info("[PostgresDBReader] Closing all connections in the pool.")
            self._connection_pool.closeall()
            self._connection_pool = None
