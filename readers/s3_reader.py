# readers/s3_reader.py

from dataclasses import dataclass, field
from typing import Optional, Tuple, Any, List, Dict, Callable
import io
import os
import pandas as pd
import boto3
import awswrangler as wr
from botocore.client import BaseClient
from python_project_generics.logging_config import get_logger

from .base_reader import DBReader

logger = get_logger(__name__)


@dataclass
class WranglerS3Reader(DBReader[BaseClient]):
    """
    S3-based Reader conforming to the DBReader protocol, using AWS Wrangler for
    multi-format file reads (CSV, Parquet, JSON, ORC, Excel).
    """
    bucket: str
    prefix: Optional[str] = None
    file_pattern: Optional[str] = None
    region_name: Optional[str] = None
    sql: Optional[str] = None
    params: Optional[List[Any]] = None
    session_kwargs: dict = field(default_factory=dict)
    _s3_client: Optional[BaseClient] = field(default=None, init=False)

    def __post_init__(self):
        """Validate required parameters and initialize logger"""
        # The SQL parameter for S3 reader can be used to store a full S3 URI
        # This makes it consistent with how SQL queries are stored for database readers

        # Convert params from list to tuple if needed
        if self.params is not None and not isinstance(self.params, tuple):
            self.params = tuple(self.params)
            logger.debug("[WranglerS3Reader] Converted params to tuple")

        # If bucket is not provided, check if sql contains a full S3 URI
        if not self.bucket and self.sql and self.sql.startswith("s3://"):
            logger.info(f"[WranglerS3Reader] Using SQL parameter as S3 URI: {self.sql}")
        elif not self.bucket:
            raise ValueError("S3 bucket is required when a full S3 URI is not provided in the sql parameter")

        logger.info(f"[WranglerS3Reader] Initialized with bucket={self.bucket}, "
                    f"prefix={self.prefix}, pattern={self.file_pattern}")

    def connect(self) -> BaseClient:
        """
        Return or create a boto3 S3 client (the 'connection' object).
        """
        if not self._s3_client:
            session_args = self.session_kwargs.copy()
            if self.region_name:
                session_args['region_name'] = self.region_name

            session = boto3.Session(**session_args)
            self._s3_client = session.client("s3")
            logger.debug(f"[WranglerS3Reader] Created new S3 client")
        return self._s3_client

    def execute_query(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Stub for row-based output. Returns an empty list, as S3 is not used for row-oriented queries.
        """
        logger.debug("[WranglerS3Reader] execute_query called but not applicable for S3")
        return []

    def _build_s3_path(self) -> str:
        """
        Builds a complete S3 path using one of these approaches:
        1. Use the sql attribute if it's a full S3 URI (starting with s3://)
        2. Otherwise, build a path from bucket, prefix, and file_pattern components

        Returns:
            str: A complete S3 URI
        """
        # If sql is a full S3 URI, use it directly
        if self.sql and self.sql.startswith("s3://"):
            logger.debug(f"[WranglerS3Reader] Using SQL as S3 path: {self.sql}")
            return self.sql

        # Otherwise build path from components
        if not self.bucket:
            raise ValueError("S3 bucket is required when not using full S3 URI in sql parameter")

        path = f"s3://{self.bucket}"

        if self.prefix:
            # Ensure prefix doesn't start with a slash but ends with one
            clean_prefix = self.prefix.strip('/')
            if clean_prefix:
                path = f"{path}/{clean_prefix}"

        # If a specific file pattern is provided, append it
        if self.file_pattern:
            # Don't add an extra slash if path already ends with one
            if not path.endswith('/'):
                path = f"{path}/"
            path = f"{path}{self.file_pattern}"

        logger.debug(f"[WranglerS3Reader] Built S3 path: {path}")
        return path

    def fetch_as_dataframe(
            self,
            query: Optional[str] = None,
            params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Read a file from S3 as a DataFrame.

        Parameters:
        -----------
        query : str, optional
            Full S3 URI (s3://) to read. If None, uses either:
            1. self.sql if it's a valid S3 URI, or
            2. Constructs path from bucket, prefix, and file_pattern

        params : tuple, optional
            Optional parameters:
            - First element can specify file format ('csv', 'parquet', etc.)
            - If None, uses self.params

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing the file contents
        """
        self.connect()

        # Determine the S3 path to use:
        # 1. Explicitly provided query parameter
        # 2. Otherwise use _build_s3_path() which checks:
        #    a. self.sql if it's a valid S3 URI
        #    b. Or constructs from bucket/prefix/file_pattern
        s3_path = query if query else self._build_s3_path()

        # Use provided params or instance params
        format_params = params if params is not None else self.params

        # Determine the file format
        file_format = self._determine_format(s3_path, format_params)
        logger.info(f"[WranglerS3Reader] Reading {file_format} from {s3_path}")

        format_options = {}

        def read_excel_s3(path: str) -> pd.DataFrame:
            raw_bytes = wr.s3.read_binary(path=path, boto3_session=None)
            return pd.read_excel(io.BytesIO(raw_bytes))

        handlers: Dict[str, Callable[[], pd.DataFrame]] = {
            "csv": lambda: wr.s3.read_csv(path=s3_path, **format_options),
            "parquet": lambda: wr.s3.read_parquet(path=s3_path, **format_options),
            "json": lambda: wr.s3.read_json(path=s3_path, **format_options),
            "orc": lambda: wr.s3.read_orc(path=s3_path, **format_options),
            "excel": lambda: read_excel_s3(s3_path),
            "xlsx": lambda: read_excel_s3(s3_path),
            "xls": lambda: read_excel_s3(s3_path),
        }

        handler = handlers.get(file_format)
        if not handler:
            raise ValueError(f"[WranglerS3Reader] Unsupported file format: {file_format}")

        try:
            result_df = handler()
            logger.info(f"[WranglerS3Reader] Successfully read DataFrame with shape {result_df.shape}")
            return result_df
        except wr.exceptions.NoFilesFound:
            logger.warning(f"[WranglerS3Reader] No files found at {s3_path}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"[WranglerS3Reader] Error reading {file_format} from '{s3_path}': {e}")
            raise IOError(f"[WranglerS3Reader] Error reading {file_format} from '{s3_path}': {e}") from e

    def close(self) -> None:
        """
        Close the S3 client, releasing underlying HTTP connections.
        """
        if self._s3_client:
            session = getattr(self._s3_client, '_session', None)
            if session:
                adapter = getattr(session, 'adapters', {}).get('https://', None)
                if adapter:
                    adapter.close()
            self._s3_client = None
            logger.debug("[WranglerS3Reader] Closed S3 client")

    # ---------------------------------------------------------------------
    # HELPER: Determine file format from params or from the extension
    # ---------------------------------------------------------------------
    @staticmethod
    def _determine_format(path: str, params: Optional[Any] = None) -> str:
        """
        Determine the file format from:
        1. First parameter in params (if it's a string)
        2. File extension in the path
        3. Default to 'csv' if unable to determine
        """
        # Check if format is explicitly provided in params
        if params and len(params) > 0 and isinstance(params[0], str):
            return params[0].lower()

        # Infer from extension
        path_no_prefix = path.replace("s3://", "", 1)
        _, ext = os.path.splitext(path_no_prefix)
        if ext.startswith("."):
            return ext[1:].lower()

        return "csv"  # Default format