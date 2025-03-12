# services/writer_service.py
from typing import Optional, Any
import pandas as pd
from writers.writer_factory import WriterFactory
from writers.base_writer import DBWriter
from python_project_generics.logging_config import get_logger

logger = get_logger(__name__)

class WriterService:
    """
    A service class that simplifies writing data to different destinations (S3, Postgres, Oracle).
    Mirroring the approach of ReaderService.
    """

    def __init__(self, db_type: str, config_file: str, environment: str, **runtime_kwargs):
        """
        Creates a writer using the environment-based YAML config (plus any overrides).
        """
        logger.info(f"[WriterService] Initializing for db_type={db_type}, environment={environment}")
        self.db_type = db_type
        self.config_file = config_file
        self.environment = environment
        self.runtime_kwargs = runtime_kwargs

        # Create the underlying writer
        factory = WriterFactory(db_type, config_file, environment, **runtime_kwargs)
        self._writer: DBWriter = factory.get_writer()

    def write_dataframe(self, df: pd.DataFrame, **kwargs) -> None:
        """
        Write a DataFrame to the configured destination.
        Additional options can be passed as kwargs (e.g. file_pattern='...', if_exists='...', etc.)
        """
        if df.empty:
            logger.warning("[WriterService] Empty DataFrame received. Skipping write.")
            return

        try:
            logger.info(f"[WriterService] Writing DataFrame with shape {df.shape}")
            self._writer.write_as_dataframe(df, **kwargs)
            logger.info("[WriterService] Write successful.")
        except Exception as e:
            logger.error(f"[WriterService] Write failed: {str(e)}")
            raise

    def close(self) -> None:
        """
        Close the underlying writer (connection, etc.)
        """
        if hasattr(self, '_writer') and self._writer:
            try:
                self._writer.close()
                logger.debug("[WriterService] Writer closed.")
            except Exception as e:
                logger.error(f"[WriterService] Error closing writer: {str(e)}")
