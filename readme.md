# Data Access Framework

A flexible and extensible framework for reading data from various sources (PostgreSQL, Oracle, S3) with a unified interface.

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Data Flow Diagram](#data-flow-diagram)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage Examples](#usage-examples)
- [Component Reference](#component-reference)
  - [Core Components](#core-components)
  - [Property Factories](#property-factories)
  - [Reader Implementations](#reader-implementations)
  - [Service Layer](#service-layer)
- [Error Handling](#error-handling)
- [Development Guidelines](#development-guidelines)
- [Extending the Framework](#extending-the-framework)

## Architecture Overview

This framework follows a layered architecture to provide a consistent interface for reading data from multiple sources:

1. **Protocol Layer**: Defines the common interface for all data readers
2. **Reader Layer**: Contains concrete implementations for different data sources
3. **PropertyFactory Layer**: Handles configuration loading for different environments
4. **Factory Layer**: Creates appropriate readers with the right configuration
5. **Service Layer**: Provides a simplified API for client code
6. **CLI Layer**: Command-line interface for direct usage

This design allows for:
- **Consistency**: All data sources are accessed through the same interface
- **Extensibility**: Adding new data sources requires minimal changes
- **Configurability**: Environment-specific configuration without code changes
- **Separation of concerns**: Each layer has a clear responsibility

## Data Flow Diagram

```
┌─────────────┐     ┌─────────────┐     ┌─────────────────┐     ┌────────────────┐
│             │     │             │     │                 │     │                │
│  CLI Input  │────▶│ ReaderService│────▶│ConnectionFactory│────▶│PropertyFactory │
│             │     │             │     │                 │     │                │
└─────────────┘     └─────────────┘     └─────────────────┘     └────────────────┘
                           │                     │                      │
                           │                     │                      │
                           │                     │                      │
                           │                     ▼                      │
                           │              ┌────────────┐                │
                           │              │            │                │
                           │              │ DBRegistry │                │
                           │              │            │                │
                           │              └────────────┘                │
                           │                     │                      │
                           │                     │                      │
                           │                     ▼                      ▼
                           │              ┌────────────────────────────────┐
                           │              │                                │
                           └─────────────▶│        Reader Instance         │
                                          │  (PostgreSQL, Oracle, or S3)   │
                                          │                                │
                                          └────────────────────────────────┘
                                                         │
                                                         │
                                                         ▼
                                          ┌────────────────────────────────┐
                                          │                                │
                                          │         pandas DataFrame       │
                                          │                                │
                                          └────────────────────────────────┘
```

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/your-org/data-access-framework.git
   cd data-access-framework
   ```

2. Install required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

Requirements include:
- pandas
- psycopg2 (for PostgreSQL)
- cx_Oracle (for Oracle)
- boto3 and awswrangler (for S3)
- PyYAML (for configuration)

## Configuration

Create a `config` directory with your database configuration file:

```yaml
# config/db_config.yml
DEV:
  postgres:
    db_type: postgres
    host: database.host.dev
    port: 5432
    user: dev_user
    password: dev_password
    database: dev_db
    pooling:
      minconn: 1
      maxconn: 5

  oracle:
    db_type: oracle
    dsn: oracle.host.dev/service
    user: dev_user
    password: dev_password
    pooling:
      min: 1
      max: 5
      increment: 1

  s3:
    db_type: s3
    bucket: dev-bucket
    region_name: us-east-1

PROD:
  postgres:
    db_type: postgres
    host: database.host.prod
    port: 5432
    user: prod_user
    password: prod_password
    database: prod_db
    pooling:
      minconn: 5
      maxconn: 20

  oracle:
    db_type: oracle
    dsn: oracle.host.prod/service
    user: prod_user
    password: prod_password
    pooling:
      min: 5
      max: 20
      increment: 5

  s3:
    db_type: s3
    bucket: prod-bucket
    region_name: us-east-1
```

## Usage Examples

### Command Line Interface

```bash
# Read from PostgreSQL
python main.py --environment DEV --source postgres --query "SELECT * FROM table WHERE id = %s" --params 123

# Read from Oracle
python main.py --environment PROD --source oracle --query "SELECT * FROM table WHERE id = :1" --params 123

# Read from S3 with bucket/prefix/pattern
python main.py --environment DEV --source s3 --bucket my-bucket --prefix data/2023 --file_pattern report.csv

# Read from S3 with direct path
python main.py --environment DEV --source s3 --query "s3://my-bucket/data/2023/report.csv"
```

### Python API

```python
from services.reader_service import ReaderService

# Read from PostgreSQL
postgres_service = ReaderService(
    environment="DEV",
    source="postgres",
    query="SELECT * FROM table WHERE region = %s",
    params=["EMEA"]
)
postgres_df = postgres_service.read_as_dataframe()

# Read from Oracle
oracle_service = ReaderService(
    environment="PROD", 
    source="oracle",
    query="SELECT * FROM table WHERE region = :1",
    params=["EMEA"]
)
oracle_df = oracle_service.read_as_dataframe()

# Read from S3 with bucket/prefix/pattern
s3_service_1 = ReaderService(
    environment="DEV",
    source="s3",
    bucket="my-bucket",
    prefix="data/2023",
    file_pattern="report.csv"
)
s3_df_1 = s3_service_1.read_as_dataframe()

# Read from S3 with direct URI
s3_service_2 = ReaderService(
    environment="DEV",
    source="s3",
    query="s3://my-bucket/data/2023/report.csv"
)
s3_df_2 = s3_service_2.read_as_dataframe()
```

## Component Reference

### Core Components

#### DBReader Protocol

```python
# readers/base_reader.py

class DBReader(Protocol[TConn], Generic[TConn]):
    """
    A generic interface for reading data from various sources.
    
    Type Parameters:
    ----------------
    TConn: 
        The type of connection object used by the reader.
    """
    
    def connect(self) -> TConn:
        """
        Establish a connection to the data source.
        
        Returns:
            The connection object specific to the data source.
        """
        ...

    def execute_query(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Execute a query on the data source.
        
        Parameters:
            query: The query to execute. If None, uses a pre-configured query.
            params: Parameters for the query. If None, uses pre-configured parameters.
            
        Returns:
            List of result rows. Empty list for non-query operations or no results.
        """
        ...

    def fetch_as_dataframe(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Execute a query and return results as a pandas DataFrame.
        
        Parameters:
            query: The query to execute. If None, uses a pre-configured query.
            params: Parameters for the query. If None, uses pre-configured parameters.
            
        Returns:
            DataFrame containing the query results or file contents.
        """
        ...

    def close(self) -> None:
        """
        Close the connection and release resources.
        """
        ...
```

#### BasePropertyFactory

```python
# property_factories/base_property_factory.py

class BasePropertyFactory:
    """
    A generic base class for loading environment-based configurations.
    
    This class loads configuration from a YAML file for a specific environment.
    Child classes can override filter_config() to customize which properties are included.
    """

    def __init__(self, config_file: str, environment: str) -> None:
        """
        Initialize the property factory.
        
        Parameters:
            config_file: Path to the YAML config file.
            environment: The environment to load (e.g., 'DEV', 'PROD').
        
        Raises:
            ValueError: If the environment is not found in the config file.
        """
        ...

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter the raw configuration block for the environment.
        
        Child classes can override this to include only relevant properties.
        
        Parameters:
            env_block: The raw configuration dictionary for the environment.
            
        Returns:
            Filtered configuration dictionary.
        """
        ...

    def get_all(self) -> Dict[str, Any]:
        """
        Get the complete configuration dictionary.
        
        Returns:
            A copy of the complete configuration.
        """
        ...

    def get_property(self, key: str, default=None) -> Any:
        """
        Get a specific property from the configuration.
        
        Parameters:
            key: The property key to retrieve.
            default: Value to return if key is not found.
            
        Returns:
            The property value or default if not found.
        """
        ...
```

#### ConnectionFactory

```python
# readers/connection_factory.py

class ConnectionFactory(Generic[T]):
    """
    Factory for creating reader instances based on configuration.
    
    Type Parameters:
    ----------------
    T: 
        The type of reader to create (must implement DBReader protocol).
    """

    def __init__(self, db_type: str, config_file: str, environment: str, **runtime_kwargs) -> None:
        """
        Initialize the connection factory.
        
        Parameters:
            db_type: The type of database ("postgres", "oracle", "s3").
            config_file: Path to the YAML config file.
            environment: The environment to load (e.g., 'DEV', 'PROD').
            runtime_kwargs: Additional parameters to override config values.
        
        Raises:
            ValueError: If db_type is not supported.
        """
        ...

    def get_connection(self) -> T:
        """
        Create and return a reader instance.
        
        Process:
        1. Get the appropriate PropertyFactory for the db_type
        2. Load environment config from YAML file
        3. Merge with runtime kwargs, with runtime taking precedence
        4. Instantiate the correct reader class
        
        Returns:
            An instance of a reader class (PostgresDBReader, OracleDBReader, or WranglerS3Reader).
            
        Raises:
            ValueError: If db_type has no matching property factory or reader class.
            TypeError: If required parameters are missing.
        """
        ...
```

### Property Factories

#### PostgresPropertyFactory

```python
# property_factories/postgres_property_factory.py

class PostgresPropertyFactory(BasePropertyFactory):
    """
    PropertyFactory for PostgreSQL configuration.
    
    Filters configuration to include only PostgreSQL-relevant properties.
    """

    # Valid keys for PostgreSQL configuration
    VALID_KEYS = {
        "db_type", "host", "port", "user", "password", "database", 
        "pooling", "sql", "params"
    }

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter the config to include only keys relevant to PostgreSQL.
        
        Parameters:
            env_block: The raw configuration dictionary.
            
        Returns:
            Filtered configuration with only PostgreSQL-relevant keys.
        """
        ...
```

#### OraclePropertyFactory

```python
# property_factories/oracle_property_factory.py

class OraclePropertyFactory(BasePropertyFactory):
    """
    PropertyFactory for Oracle configuration.
    
    Filters configuration to include only Oracle-relevant properties.
    """

    # Valid keys for Oracle configuration
    VALID_KEYS = {
        "db_type", "dsn", "user", "password", "pooling", "sql", "params"
    }

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter the config to include only keys relevant to Oracle.
        
        Parameters:
            env_block: The raw configuration dictionary.
            
        Returns:
            Filtered configuration with only Oracle-relevant keys.
        """
        ...
```

#### S3PropertyFactory

```python
# property_factories/s3_property_factory.py

class S3PropertyFactory(BasePropertyFactory):
    """
    PropertyFactory for S3 configuration.
    
    Filters configuration to include only S3-relevant properties.
    """

    # Valid keys for S3 configuration
    VALID_KEYS = {
        "db_type", "bucket", "prefix", "file_pattern", "region_name",
        "aws_profile", "format", "sql", "params"
    }

    def filter_config(self, env_block: Dict[str, Any]) -> Dict[str, Any]:
        """
        Filter the config to include only keys relevant to S3.
        
        Parameters:
            env_block: The raw configuration dictionary.
            
        Returns:
            Filtered configuration with only S3-relevant keys.
        """
        ...
```

### Reader Implementations

#### PostgresDBReader

```python
# readers/postgres_reader.py

@dataclass
class PostgresDBReader(DBReader[psycopg2.extensions.connection]):
    """
    DBReader implementation for PostgreSQL databases.
    
    Uses psycopg2 with connection pooling to read from PostgreSQL databases.
    
    Attributes:
        host: Database host address.
        port: Database port number.
        user: Database username.
        password: Database password.
        database: Database name.
        pooling: Connection pooling settings (dict with minconn, maxconn).
        sql: Optional SQL query provided at initialization.
        params: Optional parameters for the SQL query.
    """

    host: str
    port: int
    user: str
    password: str
    database: str
    pooling: Dict[str, Any]
    sql: Optional[str] = None
    params: Optional[Tuple[Any, ...]] = None
    extras: Dict[str, Any] = field(default_factory=dict)

    _connection_pool: Optional[pool.SimpleConnectionPool] = field(default=None, init=False)

    def connect(self) -> psycopg2.extensions.connection:
        """Get a connection from the pool, initializing it if needed."""
        ...

    def execute_query(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """Execute a SQL query and return rows."""
        ...

    def fetch_as_dataframe(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        ...

    def close(self) -> None:
        """Close all connections in the pool."""
        ...
```

#### OracleDBReader

```python
# readers/oracle_reader.py

@dataclass
class OracleDBReader(DBReader[cx_Oracle.Connection]):
    """
    DBReader implementation for Oracle databases.
    
    Uses cx_Oracle with session pooling to read from Oracle databases.
    
    Attributes:
        dsn: Oracle connection string.
        user: Database username.
        password: Database password.
        pooling: Session pooling settings (dict with min, max, increment).
        sql: Optional SQL query provided at initialization.
        params: Optional parameters for the SQL query.
    """

    dsn: str
    user: str
    password: str
    pooling: Dict[str, Any]
    sql: Optional[str] = None
    params: Optional[List[Any]] = None

    _session_pool: Optional[cx_Oracle.SessionPool] = field(default=None, init=False)

    def connect(self) -> cx_Oracle.Connection:
        """Get a connection from the session pool, initializing it if needed."""
        ...

    def execute_query(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """Execute a SQL query and return rows."""
        ...

    def fetch_as_dataframe(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """Execute a SQL query and return results as a DataFrame."""
        ...

    def close(self) -> None:
        """Close the session pool."""
        ...
```

#### WranglerS3Reader

```python
# readers/s3_reader.py

@dataclass
class WranglerS3Reader(DBReader[BaseClient]):
    """
    DBReader implementation for S3 data sources.
    
    Uses boto3 and awswrangler to read files from S3 buckets.
    
    Attributes:
        bucket: S3 bucket name.
        prefix: Optional prefix/folder path within the bucket.
        file_pattern: Optional file pattern to match within the prefix.
        region_name: AWS region for the S3 bucket.
        sql: Optional full S3 URI (s3://bucket/path/file).
        params: Optional parameters (first element can specify file format).
    """

    bucket: str
    prefix: Optional[str] = None
    file_pattern: Optional[str] = None
    region_name: Optional[str] = None
    sql: Optional[str] = None
    params: Optional[List[Any]] = None
    session_kwargs: dict = field(default_factory=dict)

    _s3_client: Optional[BaseClient] = field(default=None, init=False)

    def connect(self) -> BaseClient:
        """Create or return a boto3 S3 client."""
        ...

    def _build_s3_path(self) -> str:
        """
        Build a complete S3 path from components or use sql parameter.
        
        Returns:
            Full S3 URI (s3://bucket/prefix/file_pattern).
        """
        ...

    def execute_query(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> List[Tuple[Any, ...]]:
        """
        Stub implementation for protocol compatibility.
        Returns empty list as S3 is not used for row-oriented queries.
        """
        ...

    def fetch_as_dataframe(
        self,
        query: Optional[str] = None,
        params: Optional[Tuple[Any, ...]] = None
    ) -> pd.DataFrame:
        """
        Read a file from S3 as a DataFrame.
        
        S3 path is determined from:
        1. query parameter if provided
        2. sql attribute if it's a valid S3 URI
        3. bucket/prefix/file_pattern components
        
        File format is determined from:
        1. First element of params if it's a string
        2. File extension in the S3 path
        3. Default to 'csv' if unable to determine
        """
        ...

    def close(self) -> None:
        """Close the S3 client, releasing underlying HTTP connections."""
        ...
```

### Service Layer

#### ReaderService

```python
# services/reader_service.py

class ReaderService:
    """
    High-level service for reading data from various sources.
    
    This service provides a simplified interface for reading data from
    different sources (PostgreSQL, Oracle, S3) with a consistent API.
    
    Attributes:
        environment: Environment name (e.g., 'DEV', 'PROD').
        source: Data source type ('s3', 'postgres', 'oracle').
        bucket: S3 bucket name (for S3 source).
        prefix: S3 prefix/folder path (for S3 source).
        file_pattern: File pattern to match in S3 (for S3 source).
        query: SQL query for database sources or full S3 path.
        params: Query parameters for SQL or format indicator for S3.
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
        Initialize the ReaderService for a specific source and environment.
        
        Parameters:
            environment: Environment name (e.g., 'DEV', 'PROD').
            source: Data source type ('s3', 'postgres', 'oracle').
            bucket: S3 bucket name (for S3 source).
            prefix: S3 prefix/folder path (for S3 source).
            file_pattern: File pattern to match in S3 (for S3 source).
            query: SQL query for database sources or full S3 path.
            params: Query parameters for SQL or format indicator for S3.
            
        Raises:
            ValueError: If source type is not supported or required parameters are missing.
        """
        ...

    def _convert_params(self, params_list: List[Any]) -> List[Any]:
        """
        Convert string parameters to appropriate types (int, float, bool).
        
        Parameters:
            params_list: List of parameters that might need conversion.
            
        Returns:
            List of converted parameters.
        """
        ...

    def read_as_dataframe(self, query: Optional[str] = None, params: Optional[List[Any]] = None):
        """
        Read data from the configured source and return as a DataFrame.
        
        Parameters:
            query: SQL query or S3 path to override the one provided at initialization.
            params: Query parameters or S3 format to override those provided at initialization.
            
        Returns:
            DataFrame containing the data.
            
        Raises:
            Exception: If there is an error reading the data.
        """
        ...

    def close(self):
        """
        Close the underlying reader connection.
        
        This should be called when finished with the service to release resources.
        """
        ...
```

## Error Handling

The framework provides several layers of error handling:

1. **Parameter Validation**: Each component validates its parameters and provides helpful error messages.
2. **Source-Type Validation**: ReaderService validates that the source type is supported.
3. **Required Parameter Validation**: Components check for required parameters based on source type.
4. **Configuration Validation**: PropertyFactories validate that the environment exists in the config.
5. **Connection Error Handling**: Readers handle connection errors and resource cleanup.
6. **Query Error Handling**: Readers catch and report query execution errors.
7. **Logging**: Comprehensive logging throughout the stack for debugging.

Common error cases:

| Error | Description | Resolution |
|-------|-------------|------------|
| `ValueError: Unsupported source type` | The source type is not recognized | Use one of: 'postgres', 'oracle', 's3' |
| `ValueError: Environment not found` | The environment is not in the config | Check your config file and environment name |
| `ValueError: No SQL query provided` | Missing query for database source | Provide a query parameter |
| `ValueError: S3 bucket is required` | Missing bucket for S3 source | Provide a bucket parameter or full S3 URI |
| `ConnectionError` | Error connecting to the data source | Check connection parameters and network |
| `TypeError: Missing required parameters` | Missing required init parameters | Check reader constructor parameters |

## Development Guidelines

When extending or modifying the framework, follow these guidelines:

1. **Protocol Adherence**: All reader implementations must fully implement the DBReader protocol.
2. **Parameter Handling**: Methods should accept optional parameters and fall back to instance attributes.
3. **Error Handling**: Use appropriate exception handling and provide helpful error messages.
4. **Logging**: Include detailed logging for debugging and monitoring.
5. **Resource Cleanup**: Ensure connections and resources are properly closed.
6. **Type Hints**: Use proper type hints for better IDE support and code validation.
7. **Testing**: Add tests for new features and edge cases.

## Extending the Framework

### Adding a New Reader Type

To add a new reader type (e.g., MongoDB):

1. Create a new reader implementation in the `readers` package:
   ```python
   # readers/mongo_reader.py
   @dataclass
   class MongoDBReader(DBReader[pymongo.MongoClient]):
       """DBReader implementation for MongoDB."""
       # ...implement required methods
   ```

2. Create a new property factory in the `property_factories` package:
   ```python
   # property_factories/mongo_property_factory.py
   class MongoPropertyFactory(BasePropertyFactory):
       """PropertyFactory for MongoDB configuration."""
       # ...implement required methods
   ```

3. Register the new reader and property factory in `connection_factory.py`:
   ```python
   DB_REGISTRY = {
       "postgres": PostgresDBReader,
       "oracle": OracleDBReader,
       "s3": WranglerS3Reader,
       "mongodb": MongoDBReader  # Add new reader
   }

   PROPERTY_FACTORY_REGISTRY = {
       "postgres": PostgresPropertyFactory,
       "oracle": OraclePropertyFactory,
       "s3": S3PropertyFactory,
       "mongodb": MongoPropertyFactory  # Add new property factory
   }
   ```

4. Add configuration to `db_config.yml`:
   ```yaml
   DEV:
     mongodb:
       db_type: mongodb
       connection_string: mongodb://localhost:27017/
       database: dev_db
       # ...other MongoDB config
   ```

5. Update documentation and tests to include the new reader type.

The framework's modular design makes it easy to add new data sources while maintaining a consistent interface for client code.


