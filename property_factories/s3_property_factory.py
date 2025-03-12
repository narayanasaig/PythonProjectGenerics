# property_factories/s3_property_factory.py

from typing import Any, Dict
from property_factories.base_property_factory import BasePropertyFactory
from logging_config import get_logger

logger = get_logger(__name__)


class S3PropertyFactory(BasePropertyFactory):
    """
    Property factory for S3 data sources.

    Loads environment-based config from YAML files and filters
    to include only keys relevant to S3 operations.
    """

    # Valid keys for S3 configuration
    VALID_KEYS = {
        "db_type",  # Type identifier (always "s3")
        "bucket",  # S3 bucket name
        "prefix",  # Path prefix within bucket
        "file_pattern",  # File pattern to match
        "region_name",  # AWS region
        "aws_profile",  # AWS profile name
        "format",  # Default file format (csv, parquet, etc.)
        "sql",  # Can be used to store a full S3 URI
        "params"  # Can store format parameters
    }

    def __init__(self, config_file: str, environment: str) -> None:
        """
        Initialize S3PropertyFactory by loading config from YAML.

        Parameters:
        -----------
        config_file : str
            Path to the YAML config file
        environment : str
            Environment to load ("DEV", "PROD", etc.)
        """
        # Call parent class to load and filter config
        super().__init__(config_file, environment)

        # Log the final configuration
        logger.info(f"[S3PropertyFactory] Loaded environment '{environment}' with the following S3 config:")
        for key, val in self._config.items():
            # Don't log sensitive information if present
            if key in ("aws_secret_access_key", "password", "secret"):
                logger.info(f"    {key} = ******")
            else:
                logger.info(f"    {key} = {val}")

        # Ensure db_type is set correctly
        if "db_type" not in self._config:
            self._config["db_type"] = "s3"
        elif self._config["db_type"] != "s3":
            logger.warning(f"[S3PropertyFactory] Overriding db_type from '{self._config['db_type']}' to 's3'")
            self._config["db_type"] = "s3"

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter the config to include only keys relevant to S3.

        Parameters:
        -----------
        env_block : dict
            Raw config dictionary from YAML

        Returns:
        --------
        dict
            Filtered config with only S3-relevant keys
        """
        logger.debug("[S3PropertyFactory] Filtering config for S3.")

        # Keep only the keys relevant to S3
        filtered = {k: v for k, v in env_block.items() if k in self.VALID_KEYS}

        # Check for required keys
        if "bucket" not in filtered and "sql" not in filtered:
            logger.warning("[S3PropertyFactory] Neither 'bucket' nor 'sql' found in config")

        return filtered