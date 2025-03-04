# readers/connection_factory.py

from typing import TypeVar, Generic
from python_project_generics.logging_config import get_logger
from .base_reader import DBReader, TConn
from .postgres_reader import PostgresDBReader
from .oracle_reader import OracleDBReader
from .s3_reader import S3Reader  # hypothetical
from python_project_generics.utils.property_factory import PropertyFactory

logger = get_logger(__name__)

DB_REGISTRY = {
    "postgres": PostgresDBReader,
    "oracle": OracleDBReader,
    "s3": S3Reader
}

T = TypeVar("T", bound=DBReader)

class ConnectionFactory(Generic[T]):
    """
    Uses a registry (db_type -> DBReader class) to avoid if/elif
    and merges environment properties with runtime kwargs.
    """

    def __init__(self, property_factory: PropertyFactory, **runtime_kwargs) -> None:
        self.property_factory = property_factory
        self.runtime_kwargs = runtime_kwargs
        logger.debug("[ConnectionFactory] Initialized with PropertyFactory + runtime_kwargs.")

    def get_connection(self) -> T:
        db_type = self.property_factory.get_property("db_type", "").lower()
        logger.info(f"[ConnectionFactory] Creating DBReader for db_type='{db_type}'")

        reader_cls = DB_REGISTRY.get(db_type)
        if not reader_cls:
            raise ValueError(f"Unsupported db_type: {db_type}")

        # (1) Pull DB config from the YAML
        env_config = self.property_factory.get_all()
        env_config.pop("db_type", None)

        # (2) Combining environment config with any runtime kwargs
        final_kwargs = {**env_config, **self.runtime_kwargs}

        logger.debug(f"[ConnectionFactory] Instantiating {reader_cls.__name__} with {final_kwargs}")
        connection_obj: T = reader_cls(**final_kwargs)  # type: ignore
        return connection_obj
