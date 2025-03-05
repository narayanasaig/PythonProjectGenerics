# property_factories/oracle_property_factory.py

from typing import Any, Dict
from python_project_generics.property_factories.base_property_factory import BasePropertyFactory
from python_project_generics.logging_config import get_logger

logger = get_logger(__name__)

class OraclePropertyFactory(BasePropertyFactory):
    """
    A specialized property factory for Oracle data sources.
    It loads environment-based config from db_config.yml,
    prunes or validates only the keys relevant to OracleDBReader.
    """

    # Adjust these keys to match your OracleDBReader's expected fields.
    # (We've included optional "sql" or "params" if you sometimes pass them in the config.)
    VALID_KEYS = {
        "db_type",
        "dsn",
        "user",
        "password",
        "pooling",
        "sql",
        "params"
    }

    def __init__(self, config_file: str, environment: str) -> None:
        # (1) base class load + filter config
        super().__init__(config_file, environment)

        # (2) Log the final config for easy debugging
        logger.info(f"[OraclePropertyFactory] Loaded environment '{environment}' with the following Oracle config:")
        for key, val in self._config.items():
            logger.info(f"    {key} = {val}")

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Keep only the keys relevant to Oracle.
        """
        logger.debug("[OraclePropertyFactory] Filtering config for Oracle.")
        filtered = {k: v for k, v in env_block.items() if k in self.VALID_KEYS}
        return filtered
