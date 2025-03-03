# utils/property_factory.py

import logging
from typing import Any, Dict
import yaml

from python_project_generics.logging_config  import get_logger


logger = get_logger(__name__)

class PropertyFactory:
    """
    Reads environment-specific config from db_config.yml.
    The environment name is determined externally (e.g., from env_selector.json).
    """

    def __init__(self, config_file: str, environment: str) -> None:
        logger.info(f"[PropertyFactory] Loading '{config_file}' for environment '{environment}'")
        with open(config_file, 'r') as f:
            self._full_config = yaml.safe_load(f)

        if environment not in self._full_config:
            raise ValueError(f"Environment '{environment}' not found in {config_file}.")

        self._config = self._full_config[environment]

    def get_property(self, key: str, default=None) -> Any:
        value = self._config.get(key, default)
        logger.debug(f"[PropertyFactory] get_property('{key}') -> {value}")
        return value

    def get_all(self) -> Dict[str, Any]:
        return dict(self._config)
