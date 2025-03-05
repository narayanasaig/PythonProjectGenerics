# readers/connection_factory.py

from typing import TypeVar, Generic, Dict, Any
from python_project_generics.logging_config import get_logger

from .base_reader import DBReader
from .postgres_reader import PostgresDBReader
from .oracle_reader import OracleDBReader
from .s3_reader import WranglerS3Reader
from property_factories.postgres_property_factory import PostgresPropertyFactory
from property_factories.oracle_property_factory import OraclePropertyFactory
from property_factories.s3_property_factory import S3PropertyFactory

logger = get_logger(__name__)

# A dictionary for Reader
DB_REGISTRY = {
    "postgres": PostgresDBReader,
    "oracle": OracleDBReader,
    "s3": WranglerS3Reader
}

# A dictionary for each propertyFactory
PROPERTY_FACTORY_REGISTRY = {
    "postgres": PostgresPropertyFactory,
    "oracle": OraclePropertyFactory,
    "s3": S3PropertyFactory
}

T = TypeVar("T", bound=DBReader)


class ConnectionFactory(Generic[T]):
    """
    Creates appropriate database or S3 reader instances based on configuration.

    Uses two registries:
      1) PROPERTY_FACTORY_REGISTRY: picks which specialized property factory to use
      2) DB_REGISTRY: picks which DB reader class to instantiate

    Merges environment-based config from YAML with runtime kwargs.
    """

    def __init__(self, db_type: str, config_file: str, environment: str, **runtime_kwargs) -> None:
        """
        Initialize a ConnectionFactory for creating reader instances.

        Parameters:
        -----------
        db_type : str
            Type of data source ("postgres", "oracle", "s3")
        config_file : str
            Path to YAML config file (e.g., "db_config.yml")
        environment : str
            Environment to use from the config ("DEV", "PROD", etc.)
        runtime_kwargs : dict
            Additional parameters to override or supplement config
        """
        self.db_type = db_type.lower()
        self.config_file = config_file
        self.environment = environment
        self.runtime_kwargs = runtime_kwargs or {}

        logger.debug(f"[ConnectionFactory] Initialized with db_type='{db_type}', "
                     f"environment='{environment}', runtime_kwargs={self.runtime_kwargs}")

        # Validate db_type is supported
        if self.db_type not in DB_REGISTRY:
            raise ValueError(f"Unsupported db_type: {self.db_type}. "
                             f"Must be one of: {', '.join(DB_REGISTRY.keys())}")

    def get_connection(self) -> T:
        """
        Create and return a DB or S3 reader instance for the configured source.

        Process:
        1) Get the appropriate PropertyFactory for the db_type
        2) Load environment config from YAML file
        3) Merge with runtime kwargs, with runtime taking precedence
        4) Instantiate the correct reader class

        Returns:
            T: Instance of a reader class (PostgresDBReader, OracleDBReader, or WranglerS3Reader)
        """
        # 1) Get the property factory class for this db_type
        factory_cls = PROPERTY_FACTORY_REGISTRY.get(self.db_type)
        if not factory_cls:
            raise ValueError(f"Unsupported db_type (no property factory): {self.db_type}")

        # 2) Instantiate the property factory to load config from file
        try:
            prop_factory = factory_cls(self.config_file, self.environment)
            logger.debug(f"[ConnectionFactory] Loaded config for {self.db_type} from {self.config_file}")
        except Exception as e:
            logger.error(f"[ConnectionFactory] Failed to load config: {str(e)}")
            raise RuntimeError(f"Error loading config for {self.db_type}: {str(e)}") from e

        # 3) Merge environment config with runtime kwargs
        config_dict = prop_factory.get_all()
        final_kwargs = {**config_dict, **self.runtime_kwargs}

        # Remove db_type from kwargs since it's not needed by readers
        final_kwargs.pop("db_type", None)

        logger.debug(f"[ConnectionFactory] Final configuration: {final_kwargs}")

        # 4) Get the reader class and instantiate it
        reader_cls = DB_REGISTRY.get(self.db_type)
        if not reader_cls:
            raise ValueError(f"Unsupported db_type (no reader class): {self.db_type}")

        try:
            # Create the reader instance with the merged configuration
            connection_obj: T = reader_cls(**final_kwargs)  # type: ignore
            logger.info(f"[ConnectionFactory] Created {reader_cls.__name__} for db_type='{self.db_type}'")
            return connection_obj
        except TypeError as e:
            # Provide better error messages for missing parameters
            logger.error(f"[ConnectionFactory] Failed to create {reader_cls.__name__}: {str(e)}")

            # Check for missing required parameters
            import inspect
            sig = inspect.signature(reader_cls.__init__)
            required_params = [p.name for p in sig.parameters.values()
                               if p.default == inspect.Parameter.empty and p.name != 'self']

            missing = [p for p in required_params if p not in final_kwargs]
            if missing:
                raise TypeError(f"Missing required parameters for {reader_cls.__name__}: {', '.join(missing)}") from e
            raise