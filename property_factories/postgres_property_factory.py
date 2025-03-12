# property_factories/postgres_property_factory.py

from typing import Any, Dict
from logging_config import get_logger
from .base_property_factory import BasePropertyFactory

logger = get_logger(__name__)


class PostgresPropertyFactory(BasePropertyFactory):
    """
    A specialized property factory for PostgreSQL data sources.
    Loads environment-based configuration from YAML files and filters
    to include only keys relevant to PostgreSQL operations.
    """

    # Valid keys for PostgreSQL configuration
    VALID_KEYS = {
        "db_type",
        "host",
        "port",
        "user",
        "password",
        "database",
        "pooling",
        "sql",
        "params",
        # Add new optional IAM keys:
        "iam_auth",
        "region",
        "sslrootcert",
        "sslmode",
        "token_ttl"
    }

    def __init__(self, config_file: str, environment: str) -> None:
        """
        Initialize PostgresPropertyFactory by loading config from YAML.

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
        logger.info(
            f"[PostgresPropertyFactory] Loaded environment '{environment}' with the following PostgreSQL config:")
        for key, val in self._config.items():
            # Don't log password
            if key == "password":
                logger.info(f"    {key} = ******")
            else:
                logger.info(f"    {key} = {val}")

        # Ensure db_type is set correctly
        if "db_type" not in self._config:
            self._config["db_type"] = "postgres"
        elif self._config["db_type"] != "postgres":
            logger.warning(
                f"[PostgresPropertyFactory] Overriding db_type from '{self._config['db_type']}' to 'postgres'")
            self._config["db_type"] = "postgres"

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter the config to include only keys relevant to PostgreSQL.

        Parameters:
        -----------
        env_block : dict
            Raw config dictionary from YAML

        Returns:
        --------
        dict
            Filtered config with only PostgreSQL-relevant keys
        """
        logger.debug("[PostgresPropertyFactory] Filtering config for PostgreSQL.")

        # Keep only the keys relevant to PostgreSQL
        filtered = {k: v for k, v in env_block.items() if k in self.VALID_KEYS}

        # Check for required keys
        required_keys = {"host", "port", "user", "password", "database"}
        missing_keys = required_keys - set(filtered.keys())

        if missing_keys:
            logger.warning(f"[PostgresPropertyFactory] Missing recommended keys: {missing_keys}")

        return filtered