import cx_Oracle
import pandas as pd
from dataclasses import dataclass, field
from typing import Any, Dict, Optional, List
from logging_config import get_logger
from DBConnections.oracle_kerberose_pool import OracleKerberosPool
from .base_writer import DBWriter
logger = get_logger(__name__)

@dataclass
class OracleDBWriter(DBWriter):
    """
    A production-oriented Oracle writer that:
      1) Uses cx_Oracle SessionPool for connection pooling.
      2) Bulk inserts data (with optional sequence-based PK generation).
      3) Bulk updates data based on one or more primary key columns (composite PK).

    Usage:
      - insert_data(df, pk_col='ID', sequence_name='SEQ_ID') -> handles inserts.
      - update_data(df, pk_cols=['PK1', 'PK2']) -> handles updates using composite PK.

    Example:
      writer = OracleDBWriter(...)

      # Insert using sequence
      df_insert = pd.DataFrame({'NAME': ['Alice'], 'VALUE': [100]})
      writer.insert_data(df_insert, pk_col='ID', sequence_name='MY_SEQ')

      # Update composite PK
      df_update = pd.DataFrame({
          'PK1': [1, 2],
          'PK2': ['A', 'B'],
          'NAME': ['NewName1', 'NewName2'],
          'VALUE': [999, 888]
      })
      writer.update_data(df_update, pk_cols=['PK1', 'PK2'])
    """

    dsn: str
    table: str
    pooling: Dict[str, Any] = field(default_factory=dict)
    use_kerberos: bool = True

    _session_pool: Optional[cx_Oracle.SessionPool] = field(default=None, init=False)

    def connect(self) -> cx_Oracle.Connection:
        """
        Acquire a connection from the Oracle session pool,
        initializing the pool if not already created.
        """
        if self._session_pool is None:
            min_sess = self.pooling.get("min", 1)
            max_sess = self.pooling.get("max", 5)
            increment = self.pooling.get("increment", 1)

            logger.info(
                "[OracleDBWriter] Initializing SessionPool: "
                f"dsn={self.dsn}, "
                f"min={min_sess}, max={max_sess}, increment={increment}"
            )
            self._session_pool = OracleKerberosPool(
                dsn=self.dsn,
                min=min_sess,
                max=max_sess,
                increment=increment,
                external_auth=True,
                threaded=True,
                getmode=cx_Oracle.SPOOL_ATTRVAL_NOWAIT
            )

        logger.debug("[OracleDBWriter] Acquiring a connection from the pool.")
        return self._session_pool.getconn()

    def write_as_dataframe(self, df: pd.DataFrame, **kwargs) -> None:
        """
        A single method that dispatches to insert_data() or update_data()
        based on a mandatory 'mode' kwarg.

        Example usage:
          writer.write_as_dataframe(df, mode="insert", sequence_name="MY_SEQ", pk_col="ID")
          writer.write_as_dataframe(df, mode="update", pk_cols=["PK1", "PK2"])
        """

        # Define validation functions
        def validate_update_mode(df, kwargs):
            pk_cols = kwargs.get("pk_cols")
            assert pk_cols, "update mode requires 'pk_cols' in kwargs!"
            return lambda: self.update_data(df, pk_cols=pk_cols, **kwargs)

        def validate_insert_mode(df, kwargs):
            return lambda: self.insert_data(df, **kwargs)

        # Create a dictionary of mode handlers
        mode_handlers = {
            "update": validate_update_mode,
            "insert": validate_insert_mode
        }

        # Validate mode exists
        mode = kwargs.get("mode")
        assert mode, "You must pass 'mode' keyword argument. Allowed values: 'insert' or 'update'"

        # Get the appropriate handler or raise error
        handler = mode_handlers.get(mode)
        assert handler, f"Invalid mode='{mode}'. Allowed values are 'insert' or 'update'."

        # Execute the handler with arguments
        try:
            handler(df, kwargs)()
        except AssertionError as e:
            raise ValueError(str(e))

    def insert_data(
        self,
        df: pd.DataFrame,
        pk_col: Optional[str] = None,
        sequence_name: Optional[str] = None,
        chunk_size: int = 50000
    ) -> None:
        """
        Bulk insert rows from 'df' into 'self.table'.

        - If 'sequence_name' is provided, then 'pk_col' must also be provided;
          auto-fill that column with IDs from the sequence.
        - If no 'sequence_name', doing a straightforward insert. The main must
          ensure the DataFrame has valid PK values if needed.
        - Data is inserted in chunks of 'chunk_size' rows, committing each chunk.
        """
        if df.empty:
            logger.info("[OracleDBWriter] No rows to insert (DataFrame is empty).")
            return

        if sequence_name and not pk_col:
            raise ValueError(
                "A sequence_name was provided but no pk_col. "
                "pk_col is mandatory if sequence_name is used."
            )

        # If using a "sequence", we actually use your custom Sequences table
        if sequence_name and pk_col:
            if pk_col not in df.columns:
                df[pk_col] = None
            self._fill_df_pk_with_sequence(df, pk_col, sequence_name)

        insert_sql = self._build_insert_sql(list(df.columns))

        conn = self.connect()
        try:
            total_rows = len(df)
            start_idx = 0

            with conn.cursor() as cur:
                while start_idx < total_rows:
                    end_idx = start_idx + chunk_size
                    df_chunk = df.iloc[start_idx:end_idx]
                    data_tuples = df_chunk.to_records(index=False).tolist()

                    logger.info(
                        f"[OracleDBWriter] Inserting rows {start_idx} to {end_idx-1} ..."
                    )
                    cur.executemany(insert_sql, data_tuples)
                    conn.commit()

                    start_idx = end_idx

            logger.info(f"[OracleDBWriter] Insert complete. Total inserted: {total_rows}")

        except Exception as e:
            logger.error(f"[OracleDBWriter] Insert operation failed: {e}")
            conn.rollback()
            raise
        finally:
            if self._session_pool:
                self._session_pool.putconn(conn)

    def update_data(
        self,
        df: pd.DataFrame,
        pk_cols: List[str],
        chunk_size: int = 50000
    ) -> None:
        """
        Bulk update rows in 'self.table' using one or more primary key columns
        (composite PK).

        - 'pk_cols' is a list of column names in df that form the PK.
        - The code excludes these PK columns from the SET clause and uses them
          in the WHERE clause.
        - Data is updated in chunks of 'chunk_size' rows, committing each chunk.

        Example:
          pk_cols = ['PK1'] -> single PK
          pk_cols = ['PK1', 'PK2'] -> composite PK
        """
        if df.empty:
            logger.info("[OracleDBWriter] No rows to update (DataFrame is empty).")
            return

        if not pk_cols:
            raise ValueError("pk_cols cannot be empty for update_data().")

        # Ensure PK columns exist in the DataFrame
        missing_keys = [c for c in pk_cols if c not in df.columns]
        if missing_keys:
            raise ValueError(
                f"The DataFrame is missing required PK columns for update: {missing_keys}"
            )

        # Build the UPDATE SQL
        all_cols = list(df.columns)
        update_sql = self._build_update_sql(all_cols, pk_cols)

        conn = self.connect()
        try:
            total_rows = len(df)
            start_idx = 0

            with conn.cursor() as cur:
                while start_idx < total_rows:
                    end_idx = start_idx + chunk_size
                    df_chunk = df.iloc[start_idx:end_idx]
                    data_tuples = df_chunk.to_records(index=False).tolist()

                    # reorder each row so that non-PK columns come first (SET), then PK columns (WHERE)
                    shifted_tuples = self._reorder_tuple_for_update(data_tuples, all_cols, pk_cols)

                    logger.info(
                        f"[OracleDBWriter] Updating rows {start_idx} to {end_idx-1} ..."
                    )
                    cur.executemany(update_sql, shifted_tuples)
                    conn.commit()

                    start_idx = end_idx

            logger.info(f"[OracleDBWriter] Update complete. Total updated: {total_rows}")

        except Exception as e:
            logger.error(f"[OracleDBWriter] Update operation failed: {e}")
            conn.rollback()
            raise
        finally:
            if self._session_pool:
                self._session_pool.putconn(conn)

    def close(self) -> None:
        """
        Close the Oracle session pool.
        """
        if self._session_pool:
            logger.info("[OracleDBWriter] Closing session pool.")
            self._session_pool.closeall()
            self._session_pool = None

    # --------------------------------------------------------------------------
    # Private Helpers
    # --------------------------------------------------------------------------
    def _build_insert_sql(self, columns: List[str]) -> str:
        """
        Build parameterized INSERT like:
          INSERT INTO table (col1, col2, ...) VALUES (:1, :2, ...)
        """
        col_str = ", ".join(columns)
        placeholders = ", ".join([f":{i+1}" for i in range(len(columns))])
        return f"INSERT INTO {self.table} ({col_str}) VALUES ({placeholders})"

    def _build_update_sql(self, columns: List[str], pk_cols: List[str]) -> str:
        """
        Build parameterized UPDATE statement for composite PK.

        Example of single PK:
          UPDATE table
          SET colA = :1, colB = :2
          WHERE pk1 = :3

        Example of composite PK:
          UPDATE table
          SET colA = :1, colB = :2
          WHERE pk1 = :3 AND pk2 = :4
        """
        # Exclude PK columns from SET
        set_cols = [c for c in columns if c not in pk_cols]

        # build SET col=? placeholders
        set_clause = ", ".join(f"{col} = :{i+1}" for i, col in enumerate(set_cols))

        # build WHERE pk=? placeholders
        # these come after all set_cols placeholders
        where_parts = []
        for j, pk_col in enumerate(pk_cols, start=1):
            placeholder_index = len(set_cols) + j
            where_parts.append(f"{pk_col} = :{placeholder_index}")

        where_clause = " AND ".join(where_parts)

        return f"UPDATE {self.table} SET {set_clause} WHERE {where_clause}"

    def _reorder_tuple_for_update(
        self,
        data_tuples: List[tuple],
        all_cols: List[str],
        pk_cols: List[str]
    ) -> List[tuple]:
        """
        Reorder each row's values to match our parameter order:
          1) all non-PK columns (in the order they appear in df)
          2) all PK columns (in the order pk_cols is provided)

        If df columns are [pk1, colA, colB, pk2],
        and pk_cols = [pk1, pk2],
        then set_cols = [colA, colB],
        placeholders = :1 (colA), :2 (colB), :3 (pk1), :4 (pk2).
        """
        set_col_indexes = []
        for i, c in enumerate(all_cols):
            if c not in pk_cols:
                set_col_indexes.append(i)

        pk_col_indexes = []
        for pk in pk_cols:
            pk_col_indexes.append(all_cols.index(pk))

        reordered_rows = []
        for row in data_tuples:
            set_part = tuple(row[i] for i in set_col_indexes)
            pk_part = tuple(row[i] for i in pk_col_indexes)
            reordered_rows.append(set_part + pk_part)

        return reordered_rows

    def _fill_df_pk_with_sequence(
        self,
        df: pd.DataFrame,
        pk_col: str,
        sequence_name: str
    ) -> None:
        """
        Allocate unique IDs from 'sequence_name' and assign them to df[pk_col].
        """
        count_needed = len(df)
        if count_needed == 0:
            return

        conn = self.connect()
        try:
            with conn.cursor() as cur:

                seq_values = self._allocate_custom_sequence_block(
                    cursor=cur,
                    entity_name=sequence_name,  ## the "Sequences" table entityName is passed here
                    block_size=count_needed
                )
            df[pk_col] = seq_values
        finally:
            if self._session_pool:
                self._session_pool.putconn(conn)

    def _allocate_custom_sequence_block(
        self,
        cursor,
        entity_name: str,
        block_size: int
    ) -> List[int]:
        """
          1) SELECT LAST_THRESHOLD from Sequences table WHERE ENTITYNAME=entity_name FOR UPDATE
          2) new_start = LAST_THRESHOLD + 1
          3) new_end = LAST_THRESHOLD + block_size
          4) update Sequences table: set LAST_THRESHOLD = new_end, last_changed_at = sysdate
          5) commit
          6) Build a range from new_start to new_end
        """
        # 1) Lock the row for this entity
        select_sql = """
          SELECT LAST_THRESHOLD FROM SEQUENCES WHERE ENTITYNAME = :ent FOR UPDATE
        """

        cursor.execute(select_sql, ent=entity_name)
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No entry found in SEQUENCES table for ENTITYNAME='{entity_name}'")

        old_threshold = row[0]
        if old_threshold is None:
            old_threshold = 0

        new_start = old_threshold + 1
        new_end = old_threshold + block_size  # We allocate 'block_size' new IDs

        # 2) Update the Sequences table
        update_sql = """
          UPDATE SEQUENCES SET LAST_THRESHOLD = :new_thr, LAST_CHANGED_AT = SYSDATE WHERE ENTITYNAME = :ent
        """
        cursor.execute(update_sql, new_thr=new_end, ent=entity_name)
        # We'll commit after the insert. Or we can do it now if we want
        # But let's do a partial commit so the row is unlocked:
        cursor.connection.commit()

        logger.info(f"[OracleDBWriter] For entity='{entity_name}', allocated new IDs from {new_start} to {new_end}.")

        # 3) Build a list of allocated IDs
        allocated_ids = list(range(new_start, new_end + 1))

        return allocated_ids
