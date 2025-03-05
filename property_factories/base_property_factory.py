# property_factories/base_property_factory.py

import yaml
from typing import Any, Dict
from python_project_generics.logging_config import get_logger

logger = get_logger(__name__)

class BasePropertyFactory:
    """
    A generic base class for loading environment-based configurations from a YAML file.
    Child classes (e.g., PostgresPropertyFactory, S3PropertyFactory) can override
    'filter_config()' to prune or validate fields specific to their data source.
    """

    def __init__(self, config_file: str, environment: str) -> None:
        """
        :param config_file: Path to the YAML config (e.g. db_config.yml).
        :param environment: The environment name to load (e.g. 'DEV', 'PROD').
        """
        logger.debug(f"[BasePropertyFactory] Loading config from '{config_file}' for env '{environment}'")
        with open(config_file, "r") as f:
            all_envs = yaml.safe_load(f)

        if environment not in all_envs:
            raise ValueError(f"Environment '{environment}' not found in {config_file}.")

        # The raw block for the chosen environment
        env_block = all_envs[environment]

        # Give child classes a chance to prune/transform
        self._config = self.filter_config(env_block)
        logger.debug(f"[BasePropertyFactory] Final config for env='{environment}': {self._config}")

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Child classes can override this method to remove or validate keys.
        By default, it just returns 'env_block' as is.
        """
        return dict(env_block)

    def get_all(self) -> Dict[str, Any]:
        """
        Returns a copy of the final config dictionary for the environment.
        """
        return dict(self._config)

    def get_property(self, key: str, default=None) -> Any:
        """
        Retrieve a specific property from the loaded config, or return 'default' if missing.
        """
        return self._config.get(key, default)
