# services/reader_service.py
import os
from pathlib import Path
from typing import Optional, List, Any, Dict, Union, Tuple

from readers.connection_factory import ConnectionFactory
from python_project_generics.logging_config import get_logger


class ReaderService:
    """
    A service class that simplifies working with different data sources.

    This class serves as a facade for the ConnectionFactory and various reader implementations,
    providing a unified interface for reading data from different sources.
    """

    def __init__(
            self,
            environment: str,
            source: str,
            bucket: Optional[str] = None,
            prefix: Optional[str] = None,
            file_pattern: Optional[str] = None,
            query: Optional[str] = None,
            params: Optional[List[Any]] = None
    ):
        """
        Initialize a ReaderService for reading data from various sources.

        Parameters:
        -----------
        environment : str
            Environment name (e.g., 'DEV', 'PROD')
        source : str
            Data source type ('s3', 'postgres', 'oracle')
        bucket : str, optional
            S3 bucket name (for S3 source)
        prefix : str, optional
            S3 prefix/folder path (for S3 source)
        file_pattern : str, optional
            File pattern to match in S3 (for S3 source)
        query : str, optional
            SQL query for database sources or full S3 path
        params : list, optional
            Query parameters for SQL or format indicator for S3
        """
        self.logger = get_logger(__name__)
        self.logger.info(f"[ReaderService] Initializing with source={source}, environment={environment}")

        self.environment = environment
        self.source = source.lower()
        self.bucket = bucket
        self.prefix = prefix
        self.file_pattern = file_pattern
        self.query = query
        self.params = params or []

        # Validate source type
        self._validate_source_type()

        # Validate parameters based on source type
        self._validate_parameters()

        # Find config file path relative to current file
        base_path = Path(os.path.dirname(__file__)).parent.resolve()
        config_file = os.path.join(base_path, "config", "db_config.yml")

        # Map 'query' parameter to 'sql' for compatibility with readers
        runtime_kwargs = {}
        if self.query:
            runtime_kwargs['sql'] = self.query

        # Add source-specific parameters
        if self.source == 's3':
            if self.bucket:
                runtime_kwargs['bucket'] = self.bucket
            if self.prefix:
                runtime_kwargs['prefix'] = self.prefix
            if self.file_pattern:
                runtime_kwargs['file_pattern'] = self.file_pattern

        # Convert and prepare params
        if self.params:
            runtime_kwargs['params'] = tuple(self._convert_params(self.params))

        # Create the underlying reader (S3Reader or DBReader)
        self.logger.info(f"[ReaderService] Creating reader for source={self.source}")
        try:
            self.connection_factory = ConnectionFactory(
                db_type=self.source,
                config_file=config_file,
                environment=self.environment,
                **runtime_kwargs
            )

            self.db_reader = self.connection_factory.get_connection()
            self.logger.debug(f"[ReaderService] Created {type(self.db_reader).__name__}")
        except Exception as e:
            self.logger.error(f"[ReaderService] Failed to create reader: {str(e)}")
            raise

    def _validate_source_type(self):
        """Validate that the source type is supported"""
        valid_sources = {'postgres', 'oracle', 's3'}
        if self.source not in valid_sources:
            raise ValueError(f"Unsupported source type: {self.source}. "
                             f"Must be one of: {', '.join(valid_sources)}")

    def _validate_parameters(self):
        """Validate that required parameters are provided based on source type"""
        if self.source == 's3':
            # For S3, either bucket or query must be provided
            if not self.bucket and not self.query:
                raise ValueError("For S3 source, either bucket or full query (S3 path) must be provided")
        else:
            # For database sources, query should be provided
            if not self.query:
                self.logger.warning(f"[ReaderService] No query provided for {self.source} source. "
                                    "Will need to be provided in fetch_as_dataframe call or in config.")

    def _convert_params(self, params_list: List[Any]) -> List[Any]:
        """
        Convert string parameters to appropriate types (int, float, bool).
        Only converts string parameters; leaves other types unchanged.

        Parameters:
        -----------
        params_list : List[Any]
            List of parameters that might need conversion

        Returns:
        --------
        List[Any]
            List of converted parameters
        """
        converted = []

        for param in params_list:
            # Skip conversion for non-string types
            if not isinstance(param, str):
                converted.append(param)
                continue

            # Try to convert to int
            try:
                converted.append(int(param))
                continue
            except ValueError:
                pass

            # Try to convert to float
            try:
                converted.append(float(param))
                continue
            except ValueError:
                pass

            # Check for boolean values
            if param.lower() in ('true', 'yes', 'y', '1'):
                converted.append(True)
            elif param.lower() in ('false', 'no', 'n', '0'):
                converted.append(False)
            else:
                # Keep as string
                converted.append(param)

        self.logger.debug(f"[ReaderService] Converted params: {converted}")
        return converted

    def read_as_dataframe(self, query: Optional[str] = None, params: Optional[List[Any]] = None):
        """
        Read data from the configured source and return as a DataFrame.

        Parameters:
        -----------
        query : str, optional
            SQL query or S3 path to override the one provided at initialization
        params : list, optional
            Query parameters or S3 format to override those provided at initialization

        Returns:
        --------
        pandas.DataFrame
            DataFrame containing the data
        """
        self.logger.info("[ReaderService] Reading data as DataFrame")

        # Use provided query/params or instance attributes
        final_query = query if query is not None else self.query

        # Convert params if needed
        if params is not None:
            final_params = tuple(self._convert_params(params))
        elif self.params:
            final_params = tuple(self.params)
        else:
            final_params = None

        try:
            # Pass query and params to the reader's fetch_as_dataframe method
            df = self.db_reader.fetch_as_dataframe(final_query, final_params)

            self.logger.info(f"[ReaderService] Successfully read DataFrame with shape {df.shape}")
            return df
        except Exception as e:
            self.logger.error(f"[ReaderService] Error reading data: {str(e)}")
            raise

    def close(self):
        """
        Close the underlying reader connection.
        """
        if hasattr(self, 'db_reader') and self.db_reader:
            try:
                self.db_reader.close()
                self.logger.debug("[ReaderService] Closed db_reader connection")
            except Exception as e:
                self.logger.error(f"[ReaderService] Error closing connection: {str(e)}")