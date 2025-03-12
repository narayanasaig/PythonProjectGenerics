# writers/writer_factory.py
from typing import Dict, Any
from logging_config import get_logger
from .base_writer import DBWriter
from .s3_writer import WranglerS3Writer
from .postgres_writer import PostgresDBWriter
from .oracle_writer import OracleDBWriter
from property_factories.postgres_property_factory import PostgresPropertyFactory
from property_factories.s3_property_factory import S3PropertyFactory
from property_factories.oracle_property_factory import OraclePropertyFactory

logger = get_logger(__name__)

DB_WRITER_REGISTRY = {
    "s3": WranglerS3Writer,
    "postgres": PostgresDBWriter,
    "oracle": OracleDBWriter
}

# Dispatch the correct property factory
PROPERTY_FACTORY_REGISTRY = {
    "postgres": PostgresPropertyFactory,
    "s3": S3PropertyFactory,
    "oracle": OraclePropertyFactory
}

class WriterFactory:
    """
    Creates appropriate writer instances based on configuration.
    """

    def __init__(self, db_type: str, config_file: str, environment: str, **runtime_kwargs):
        """
        :param db_type: "s3", "postgres", or "oracle"
        :param config_file: YAML config file path
        :param environment: e.g. "DEV", "PROD"
        :param runtime_kwargs: Additional overrides for the writer.
        """
        self.db_type = db_type.lower()
        self.config_file = config_file
        self.environment = environment
        self.runtime_kwargs = runtime_kwargs or {}

    def get_writer(self) -> DBWriter:
        """
        1) Use a property factory (if desired) to load environment config
        2) Merge with runtime kwargs
        3) Instantiate the writer
        """

        if self.db_type not in DB_WRITER_REGISTRY:
            raise ValueError(f"Unsupported writer db_type: {self.db_type}")

        # 2) Load environment config
        prop_factory_cls = PROPERTY_FACTORY_REGISTRY.get(self.db_type)
        if not prop_factory_cls:
            raise ValueError(f"No property factory for db_type={self.db_type}")
        try:
            prop_factory = prop_factory_cls(self.config_file, self.environment)
            env_config = prop_factory.get_all()
        except Exception as e:
            logger.error(f"[WriterFactory] Error loading config: {e}")
            raise

        # Merge environment config with runtime overrides
        final_config = {**env_config, **self.runtime_kwargs}
        final_config.pop("db_type", None)  # not needed by the writer class

        def _validate_oracle_mode(mode):
            assert mode, "[WriterFactory] For Oracle, you must specify 'mode' in config or runtime kwargs."
            assert mode in ("insert", "update"), f"[WriterFactory] Invalid mode='{mode}' for Oracle. Must be 'insert' or 'update'."

        try:
            self.db_type == "oracle" and _validate_oracle_mode(final_config.get("mode", None))
        except AssertionError as e:
            raise ValueError(str(e))

        # 3) Instantiate the correct writer class
        writer_cls = DB_WRITER_REGISTRY[self.db_type]
        try:
            writer_obj = writer_cls(**final_config)
            logger.info(f"[WriterFactory] Created {writer_cls.__name__} for db_type='{self.db_type}'")
            return writer_obj
        except TypeError as e:
            logger.error(f"[WriterFactory] Failed to create {writer_cls.__name__}: {str(e)}")
            raise
