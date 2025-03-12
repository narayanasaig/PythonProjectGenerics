from dataclasses import dataclass, field
from typing import Optional, Any, Dict, List, Union
import pandas as pd
import psycopg2
from psycopg2 import pool
from psycopg2.extras import execute_values

from logging_config import get_logger
from .base_writer import DBWriter
from DBConnections.postgre_auth_pool import *
#from .iam_pool import IAMAuthSimpleConnectionPool

logger = get_logger(__name__)

@dataclass
class PostgresDBWriter(DBWriter):
    """
    A writer class for Postgres/Aurora Postgres with optional IAM token auth using psycopg2 + connection pooling,
    performing a single-step MERGE (no staging).
    """
    host: str
    port: int
    user: str
    password: Optional[str]
    database: str
    table: str  # The default target table for writes
    pooling: Dict[str, Any] = field(default_factory=dict)
    extras: Dict[str, Any] = field(default_factory=dict)

    _connection_pool: Optional[pool.SimpleConnectionPool] = field(default=None, init=False)

    def connect(self) -> psycopg2.extensions.connection:
        """
        Acquire or initialize the connection pool, return a connection.
        - If extras['iam_auth'] == True, use IAMAuthSimpleConnectionPool
        - Otherwise, use regular SimpleConnectionPool
        """
        if self._connection_pool is None:
            minconn = self.pooling.get("minconn", 1)
            maxconn = self.pooling.get("maxconn", 5)
            logger.info(f"[PostgresDBWriter] Initializing pool: minconn={minconn}, maxconn={maxconn}")

            iam_auth = self.extras.get("iam_auth", False)
            if iam_auth:
                # Required parameters
                region = self.extras["region"]
                sslrootcert = self.extras.get("sslrootcert", None)
                sslmode = self.extras.get("sslmode", "require")
                token_ttl = self.extras.get("token_ttl", 900)  # default 15 min

                self._connection_pool = IAMAuthSimpleConnectionPool(
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
                    # You can include other psycopg2 args if needed
                )
            else:
                # Standard password-based auth
                self._connection_pool = pool.SimpleConnectionPool(
                    minconn=minconn,
                    maxconn=maxconn,
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                    database=self.database,
                    **self.extras  # any additional psycopg2 args
                )

        logger.debug("[PostgresDBWriter] Getting connection from pool.")
        return self._connection_pool.getconn()

    def write_as_dataframe(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Write/Upsert DataFrame into self.table using a single-step PostgreSQL MERGE statement .

        Supported kwargs:
          - if_exists: "append" (default) or "replace"
          - merge_on: The column(s) used for matching in MERGE's ON clause. (Default: "id_rec")
          - chunk_size: number of rows per MERGE statement (default: None, meaning all at once)
        """
        if df.empty:
            logger.warning("[PostgresDBWriter] Empty DataFrame. Nothing to write.")
            return

        if_exists = kwargs.get("if_exists", "append")
        merge_on = kwargs.get("merge_on", "id_rec")  # Could be string or list
        chunk_size = kwargs.get("chunk_size", None)

        conn = self.connect()
        try:
            with conn.cursor() as cur:
                # Prepare data for merge
                # We'll do multiple MERGE statements if chunk_size is specified
                if chunk_size and chunk_size > 0:
                    # chunk the dataframe
                    start = 0
                    total = len(df)
                    while start < total:
                        end = min(start + chunk_size, total)
                        subset = df.iloc[start:end]
                        self._merge_batch(cur, subset, merge_on)
                        start = end
                    logger.info(f"[PostgresDBWriter] Processed {len(df)} rows in chunks of {chunk_size} via MERGE.")
                else:
                    # single MERGE for the entire DF
                    self._merge_batch(cur, df, merge_on)

                conn.commit()
                logger.info(f"[PostgresDBWriter] Upserted {len(df)} rows into {self.table} via MERGE.")
        except Exception as e:
            logger.error(f"[PostgresDBWriter] Error in MERGE upsert: {e}")
            if conn and not conn.closed:
                conn.rollback()
            raise
        finally:
            if conn and self._connection_pool and not conn.closed:
                logger.debug("[PostgresDBWriter] Returning connection to pool.")
                self._connection_pool.putconn(conn)

    def _merge_batch(self, cur, df: pd.DataFrame, merge_on: Union[str, List[str]]) -> None:
        """
        Build and execute a single MERGE statement for the given subset of rows.
        """
        if isinstance(merge_on, str):
            merge_on = [merge_on]

        # Example:
        # MERGE INTO target_table AS t
        # USING (VALUES %s) AS s(col1, col2, col3, ...)
        #   ON (t.id = s.id)
        # WHEN MATCHED THEN
        #   UPDATE SET col2 = s.col2, col3 = s.col3, ...
        # WHEN NOT MATCHED THEN
        #   INSERT (col1, col2, col3) VALUES (s.col1, s.col2, s.col3);

        df_cols = list(df.columns)
        # Build the ON clause
        on_clauses = [f"t.{col} = s.{col}" for col in merge_on]
        on_clause = " AND ".join(on_clauses)

        # Build the set clause for WHEN MATCHED
        # Typically we update everything except the merge_on columns
        set_parts = []
        for c in df_cols:
            if c not in merge_on:
                set_parts.append(f"{c} = s.{c}")
        set_clause = ", ".join(set_parts) if set_parts else ""

        # Build the insert columns
        insert_cols_str = ", ".join(df_cols)
        insert_vals_str = ", ".join([f"s.{c}" for c in df_cols])

        # We'll create a placeholder to insert the "VALUES %s" part using psycopg2.extras.execute_values
        # Something like: USING (VALUES %s) as s(...)
        col_list_str = ", ".join(df_cols)
        merge_sql_template = f"""
        MERGE INTO {self.table} AS t
        USING (
            VALUES %s
        ) AS s({col_list_str})
          ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {set_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_cols_str})
          VALUES ({insert_vals_str});
        """

        # Convert df to list of tuples
        data = [tuple(x) for x in df.to_numpy()]

        # We will rely on execute_values to fill in "(%s, %s, ...), ..."
        # But we need to override the template to produce the correct placeholders
        # and insert them into "VALUES %s" in the MERGE statement above.
        def merge_value_template(row_count: int) -> str:
            # row_count is how many rows we have in the subset
            # This function returns " (%s,%s,...) , (%s,%s,...) , ..."
            # so the main query just has "VALUES <that_string>"
            placeholders = []
            row_placeholder = "(" + ",".join(["%s"] * len(df_cols)) + ")"
            for _ in range(row_count):
                placeholders.append(row_placeholder)
            return ",".join(placeholders)

        # Build the placeholders
        values_str = merge_value_template(len(data))

        # Final MERGE SQL
        merge_sql = merge_sql_template.replace("%s", values_str, 1)

        # Flatten data so we can pass it to cursor.execute
        flat_data = []
        for row in data:
            flat_data.extend(row)

        # Execute the MERGE
        logger.debug(f"[PostgresDBWriter] MERGE statement with {len(data)} rows.")
        cur.execute(merge_sql, flat_data)

    def close(self) -> None:
        """
        Close the connection pool.
        """
        if self._connection_pool:
            logger.info("[PostgresDBWriter] Closing all connections in the pool.")
            self._connection_pool.closeall()
            self._connection_pool = None
