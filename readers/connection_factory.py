# readers/connection_factory.py

from typing import TypeVar, Generic
from python_project_generics.logging_config  import get_logger
from .base_reader import DBReader, TConn
from .postgres_reader import PostgresDBReader
from .oracle_reader import OracleDBReader
from python_project_generics.utils.property_factory import PropertyFactory



logger = get_logger(__name__)



DB_REGISTRY = {
    "postgres": PostgresDBReader,
    "oracle": OracleDBReader
}

T = TypeVar("T", bound=DBReader)

class ConnectionFactory(Generic[T]):
    """
    Uses a registry (db_type -> DBReader class) to avoid if/elif.
    """

    def __init__(self, property_factory: PropertyFactory) -> None:
        self.property_factory = property_factory
        logger.debug("[ConnectionFactory] Initialized with PropertyFactory.")

    def get_connection(self) -> T:
        db_type = self.property_factory.get_property("db_type", "").lower()
        logger.info(f"[ConnectionFactory] Creating DBReader for db_type='{db_type}'")

        reader_cls = DB_REGISTRY.get(db_type)
        if not reader_cls:
            raise ValueError(f"Unsupported db_type: {db_type}")

        kwargs = self.property_factory.get_all()
        kwargs.pop("db_type", None)
        logger.debug(f"[ConnectionFactory] Instantiating {reader_cls.__name__} with {kwargs}")
        connection_obj: T = reader_cls(**kwargs)  # type: ignore
        return connection_obj
