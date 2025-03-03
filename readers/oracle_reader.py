# readers/oracle_reader.py


from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional
import cx_Oracle
import pandas as pd
from .base_reader import DBReader

from python_project_generics.logging_config  import get_logger


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
    """
    dsn: str
    user: str
    password: str
    pooling: Dict[str, Any]

    _session_pool: Optional[cx_Oracle.SessionPool] = None

    def connect(self) -> cx_Oracle.Connection:
        if self._session_pool is None:
            min_sess = self.pooling.get("min", 1)
            max_sess = self.pooling.get("max", 5)
            increment = self.pooling.get("increment", 1)

            logger.info(f"[OracleDBReader] Initializing SessionPool: min={min_sess}, max={max_sess}, increment={increment}")
            self._session_pool = cx_Oracle.SessionPool(
                user=self.user,
                password=self.password,
                dsn=self.dsn,
                min=min_sess,
                max=max_sess,
                increment=increment,
                threaded=True,
                getmode=cx_Oracle.SPOOL_ATTRVAL_NOWAIT
            )
        logger.debug("[OracleDBReader] Acquiring a connection from the pool.")
        return self._session_pool.acquire()

    def execute_query(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                logger.debug(f"[OracleDBReader] Executing: {query} | params={params}")
                cur.execute(query, params or ())
                if cur.description:
                    rows = cur.fetchall()
                    logger.info(f"[OracleDBReader] Query returned {len(rows)} rows.")
                    return rows
                else:
                    conn.commit()
                    logger.info("[OracleDBReader] No result set. Transaction committed.")
                    return []
        finally:
            if self._session_pool:
                self._session_pool.release(conn)

    def fetch_as_dataframe(
        self,
        query: str,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Manual approach to building a DataFrame since AWS Wrangler doesn't support Oracle.
        """
        conn = self.connect()
        try:
            with conn.cursor() as cur:
                logger.debug(f"[OracleDBReader] fetch_as_dataframe: {query}, {params}")
                cur.execute(query, params or ())
                rows = cur.fetchall()
                if not rows:
                    logger.info("[OracleDBReader] No rows returned. Empty DataFrame.")
                    return pd.DataFrame()

                col_names = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=col_names)
                logger.info(f"[OracleDBReader] DataFrame shape: {df.shape}")
                return df
        finally:
            if self._session_pool:
                self._session_pool.release(conn)

    def close(self) -> None:
        if self._session_pool:
            logger.info("[OracleDBReader] Closing session pool.")
            self._session_pool.close()
            self._session_pool = None
