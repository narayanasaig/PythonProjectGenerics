# services/reader_service.py
import os
from pathlib import Path

from python_project_generics.utils.property_factory import PropertyFactory
from python_project_generics.readers.connection_factory import ConnectionFactory
from python_project_generics.logging_config import get_logger

class ReaderService:
    def __init__(
        self,
        environment: str,
        source: str,
        bucket: str = None,
        prefix: str = None,
        file_pattern: str = None,
        query: str = None,
        params: list = None
    ):
        """
        A facade to:
          - load environment config from db_config.yml
          - override db_type with 'source'
          - create a connection (S3Reader or DBReader)
          - remember query or s3 file details for reading
        """
        self.logger = get_logger(__name__)
        self.environment = environment
        self.source = source.lower()
        self.bucket = bucket
        self.prefix = prefix
        self.file_pattern = file_pattern
        self.query = query
        self.params = params or []

        # (1) Load environment config from db_config.yml
        base_path = Path(os.path.dirname(__file__)).parent.resolve()
        config_file = os.path.join(base_path, "config", "db_config.yml")
        self.prop_factory = PropertyFactory(config_file, self.environment)

        # (2) Force db_type to what user passed in, if you want that approach
        self.prop_factory._config["db_type"] = self.source

        # (3) Create the underlying reader (S3Reader or DBReader)
        #     We'll pass in additional info so it can handle S3 or DB logic
        self.logger.info(f"[ReaderService] Creating reader for source={self.source}")
        self.connection_factory = ConnectionFactory(self.prop_factory,
                                                    bucket=self.bucket,
                                                    prefix=self.prefix,
                                                    file_pattern=self.file_pattern,
                                                    query=self.query,
                                                    params=self.params)
        self.db_reader = self.connection_factory.get_connection()

    def read_as_dataframe(self):
        """
        A single call to read the data. Under the hood, the actual logic
        for S3 vs. DB is in the db_reader (S3Reader, PostgresReader, etc.)
        """
        self.logger.debug("[ReaderService] read_as_dataframe() called.")
        df = self.db_reader.fetch_as_dataframe()
        return df

    def close(self):
        """
        Clean up any open connections.
        """
        self.db_reader.close()
        self.logger.debug("[ReaderService] Closed db_reader.")
