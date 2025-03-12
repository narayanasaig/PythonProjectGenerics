# writers/s3_writer.py
from dataclasses import dataclass, field
from typing import Optional, Any, Dict, Callable
import os
import pandas as pd
import boto3
import awswrangler as wr
from botocore.client import BaseClient
from logging_config import get_logger

from .base_writer import DBWriter

logger = get_logger(__name__)

@dataclass
class WranglerS3Writer(DBWriter):
    """
    Writes a pandas DataFrame to S3 using AWS Wrangler in various formats (CSV, Parquet, JSON, ORC, Excel).
    """

    bucket: str
    prefix: Optional[str] = None
    region_name: Optional[str] = None
    session_kwargs: dict = field(default_factory=dict)
    _s3_client: Optional[BaseClient] = field(default=None, init=False)

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
            logger.debug("[WranglerS3Writer] Created new S3 client")
        return self._s3_client

    def write_as_dataframe(
        self,
        df: pd.DataFrame,
        file_pattern: Optional[str] = None,
        file_format: Optional[str] = None,
        **kwargs
    ) -> None:
        """
        Write a DataFrame to S3.

        Parameters:
        -----------
        df : pd.DataFrame
            DataFrame to write
        file_pattern : str, optional
            File name or pattern (e.g., 'output.parquet', 'myfolder/data.csv')
        file_format : str, optional
            'csv', 'parquet', 'json', 'orc', 'excel', 'xlsx', etc.
            If not provided, defaults to 'csv'.
        **kwargs:
            Extra arguments for the underlying awswrangler call (e.g., dataset=True).
        """
        if df.empty:
            logger.warning("[WranglerS3Writer] Attempted to write an empty DataFrame to S3.")
            return

        self.connect()

        # Build final S3 path from bucket, prefix, and file_pattern
        s3_path = self._build_s3_path(file_pattern)

        # Determine file format if not provided
        file_fmt = file_format or self._determine_format(s3_path)
        logger.info(f"[WranglerS3Writer] Writing DataFrame of shape {df.shape} to {s3_path} as {file_fmt}")

        if file_fmt in ("excel", "xlsx", "xls"):
            # AWS Wrangler currently has wr.s3.to_excel but it's experimental
            # or we can do it manually via to_bytes:
            # We'll do manual approach:
            from io import BytesIO
            buffer = BytesIO()
            df.to_excel(buffer, index=False)
            buffer.seek(0)
            wr.s3.upload(local_file=None, path=s3_path, file_object=buffer)
            return

        # Dispatch dictionary for different formats
        handlers: Dict[str, Callable[[], None]] = {
            "csv": lambda: wr.s3.to_csv(df=df, path=s3_path, index=False, **kwargs),
            "parquet": lambda: wr.s3.to_parquet(df=df, path=s3_path, index=False, **kwargs),
            "json": lambda: wr.s3.to_json(df=df, path=s3_path, orient="records", **kwargs),
            "orc": lambda: wr.s3.to_orc(df=df, path=s3_path, index=False, **kwargs),
        }

        # Check handler
        handler = handlers.get(file_fmt.lower())
        if not handler:
            raise ValueError(f"[WranglerS3Writer] Unsupported file format: {file_fmt}")

        handler()
        logger.info(f"[WranglerS3Writer] Successfully wrote DataFrame to {s3_path}")

    def close(self) -> None:
        """Close the S3 client, releasing underlying HTTP connections if possible."""
        if self._s3_client:
            session = getattr(self._s3_client, '_session', None)
            if session:
                adapter = getattr(session, 'adapters', {}).get('https://', None)
                if adapter:
                    adapter.close()
            self._s3_client = None
            logger.debug("[WranglerS3Writer] Closed S3 client")

    def _build_s3_path(self, file_pattern: Optional[str]) -> str:
        """
        Constructs the full S3 path from bucket/prefix/file_pattern.
        """
        if not self.bucket:
            raise ValueError("[WranglerS3Writer] 'bucket' is required for S3 writes.")

        path = f"s3://{self.bucket}"
        if self.prefix:
            clean_prefix = self.prefix.strip('/')
            path = f"{path}/{clean_prefix}"

        if file_pattern:
            if not file_pattern.startswith('/'):
                path = path + "/"
            path = path + file_pattern
        else:
            # default file name if none is given
            path = path + "/output.csv"

        return path

    @staticmethod
    def _determine_format(path: str) -> str:
        """
        Determine the file format from the extension or default to 'csv'.
        """
        _, ext = os.path.splitext(path)
        if ext.startswith("."):
            return ext[1:].lower()
        return "csv"   # default
