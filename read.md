├── readers
│   ├── base_reader.py
│   ├── s3_reader.py
│   ├── postgres_reader.py
│   ├── oracle_reader.py
│   └── connection_factory.py
├── property_factories
│   ├── base_property_factory.py
│   ├── s3_property_factory.py
│   ├── postgres_property_factory.py
│   ├── oracle_property_factory.py
│   └── query_loader.py
├── services
│   └── reader_service.py
├── python_project_generics
│   └── logging_config.py
├── config
│   └── db_config.yml
├── main.py
└── README.md


flowchart LR
    A[User/CLI Inputs] --> B[ReaderService.__init__]
    B --> C[ConnectionFactory]
    C --> D{PropertyFactories<br>(S3/Postgres/Oracle)}
    D -->|Load & Filter Config| C
    C -->|Create| E[DBReader (S3Reader / PostgresReader / OracleReader)]
    B -->|Holds reference| E
    B --> F[ReaderService.read_as_dataframe()]
    F --> E
    E --> F
    F --> G[Return DataFrame]


User runs main.py (CLI) or invokes ReaderService in code.
ReaderService initializes, validates parameters, and calls the ConnectionFactory.
ConnectionFactory uses a corresponding PropertyFactory to load the environment config from YAML, merges it with runtime parameters, and creates the appropriate DBReader (S3, Postgres, or Oracle).
ReaderService exposes a method read_as_dataframe() to fetch data.
The DBReader implementation actually connects and retrieves data, returning a pandas DataFrame.


Files and Explanations

1. readers Package
a) base_reader.py

What: Defines a DBReader protocol (an interface) for consistent methods: connect, execute_query, fetch_as_dataframe, close.
Why: Ensures all concrete readers (S3, Postgres, Oracle) share the same API, so the rest of the system can interact uniformly.
b) s3_reader.py (WranglerS3Reader)

What: A class for reading data from Amazon S3 using AWS Wrangler.
Key Methods:
connect(): Creates or returns a cached S3 client.
fetch_as_dataframe(query, params): Reads CSV/Parquet/Excel/JSON/ORC from S3 into a pandas DataFrame.
Why: Enables easy S3 file reads (single or multiple files) in a uniform “DBReader”-like interface.
c) postgres_reader.py (PostgresDBReader)

What: A class for reading data from a PostgreSQL database using psycopg2 and connection pooling.
Key Methods:
connect(): Gets a pooled Postgres connection.
execute_query(query, params): Executes a SQL query, returns rows.
fetch_as_dataframe(query, params): Returns a pandas DataFrame of query results.
Why: Encapsulates all Postgres-specific logic (pooling, connection details, commits/rollbacks) behind a single API.
d) oracle_reader.py (OracleDBReader)

What: A class for reading data from an Oracle database using cx_Oracle and session pooling.
Key Methods:
connect(): Acquires a session from the Oracle session pool.
execute_query(query, params): Executes a SQL query, returns rows.
fetch_as_dataframe(query, params): Returns a pandas DataFrame.
Why: Same rationale as the Postgres reader, but for Oracle specifics (session pooling, Oracle bind variables, etc.).
e) connection_factory.py (ConnectionFactory)

What: A factory that, given a db_type and a config file + environment, loads the correct property factory and then creates the corresponding reader (S3 or DB).
Why: Centralizes logic for deciding which reader class to instantiate. This keeps user code simple: provide a db_type (s3, postgres, or oracle), plus environment config, and it returns a ready-to-use reader.
2. property_factories Package
a) base_property_factory.py (BasePropertyFactory)

What: Loads a YAML config file, selects the block for a chosen environment, and passes it to filter_config().
Why: Provides a framework for environment-based configurations. Child classes override filter_config() to keep only relevant keys or do further validation.
b) s3_property_factory.py (S3PropertyFactory)

What: Specialized for S3. Filters out only S3-relevant keys (like bucket, prefix, region_name).
Why: Ensures your S3 config is correctly shaped (e.g., doesn’t accidentally include DB credentials). Also sets db_type = "s3" if not already.
c) postgres_property_factory.py (PostgresPropertyFactory)

What: Specialized for Postgres. Keeps keys like host, port, user, password, database, pooling, etc.
Why: Validates or warns about missing required fields for Postgres. Logs configuration details while masking sensitive info.
d) oracle_property_factory.py (OraclePropertyFactory)

What: Specialized for Oracle. Keeps keys like dsn, user, password, pooling.
Why: Same pattern as above, but for Oracle connections.
e) query_loader.py (QueryLoader)

What: Loads named SQL queries from a JSON file. A typical entry might look like:
{
  "employee_select": {
    "sql": "SELECT * FROM employees WHERE dept = %s",
    "params": [10]
  }
}
Why: Useful if you want to store pre-written queries in a separate file. You can fetch them by key and get both the SQL and parameter list.
3. services Package
a) reader_service.py (ReaderService)

What: A high-level facade that (1) takes in parameters like environment, source, bucket, prefix, query, etc., (2) validates them, (3) sets up a ConnectionFactory, and (4) exposes a simple read_as_dataframe() method to return data as a pandas DataFrame.
Key Functions:
__init__:
Validates the source type (S3, Postgres, or Oracle).
Ensures required parameters are present.
Constructs runtime keyword arguments for ConnectionFactory.
Creates the actual db_reader using the factory.
read_as_dataframe(query, params):
Optionally override the initial query/params.
Delegates to the db_reader.fetch_as_dataframe() method.
close(): Closes the underlying connection (S3 client or DB pool).
_convert_params(): Converts CLI string params to int, float, bool if possible.
_validate_source_type() & _validate_parameters(): Ensure correct usage based on the data source.
Why: Instead of dealing with raw factories and readers, end users (or CLI scripts) can just create a ReaderService, call read_as_dataframe(), and get their data.
4. python_project_generics Package
a) logging_config.py

What: Sets up a centralized logging configuration with a specific format:
%(asctime)s [%(funcName)s] %(levelname)s: %(message)s
Why: Provides a consistent logging setup across the entire codebase, so all logs use the same style. Exposes:
setup_logging(level): Configures the root logger.
get_logger(name): Returns a logger with the given name.
5. config Directory
a) db_config.yml

What: A YAML file that organizes environment-specific configuration, for example:
DEV:
  db_type: "postgres"
  host: "..."
  port: 5432
  user: "..."
  password: "..."
  database: "..."
  pooling:
    minconn: 1
    maxconn: 5

DEV_S3:
  db_type: "s3"
  region: "us-east-1"
Why: This allows you to switch between DEV, PROD, or other environments without changing code.
6. main.py (CLI Script)
What: A command-line interface that uses argparse to accept user inputs, instantiate a ReaderService, call read_as_dataframe(), and log output or errors.
Key Steps:
Parse CLI arguments like --environment DEV --source s3 --bucket my-bucket --query s3://....
Construct a ReaderService with those arguments.
Call read_as_dataframe().
Print the result (DataFrame shape, head, or “empty” if no rows).
Close the service.
Why: Provides a convenient entry point for users/admins to quickly fetch data from different environments or sources.
Usage Instructions

Install Dependencies
Python 3.8+ recommended.
Install required libraries:
pip install -r requirements.txt
Typical libraries needed:
boto3, awswrangler, psycopg2, cx_Oracle, PyYAML, pandas.
Set Up Oracle Instant Client (if using Oracle)
Ensure cx_Oracle can find your Oracle Instant Client libraries.
Configure Logging (Optional)
By default, it logs at DEBUG level. You can modify setup_logging() or set environment variables to configure the log level.
Edit db_config.yml
Add your own credentials, bucket names, or environment blocks.
Run From CLI
Example for S3:
python main.py --environment DEV_S3 --source s3 --bucket my-bucket --prefix myfolder --file_pattern data.csv
Example for Postgres:
python main.py --environment DEV --source postgres --query "SELECT * FROM my_table" --params 42
The script logs to stdout and prints the resulting DataFrame info.
FAQ / Notes

Can I pass queries at runtime?
Yes, either supply them via --query in CLI or call service.read_as_dataframe(query="...").
What if I have more data sources?
Add a new reader class implementing DBReader, a property factory, and update DB_REGISTRY and PROPERTY_FACTORY_REGISTRY in connection_factory.py.
Where do I store big queries?
Use the query_loader.py approach to keep them in a JSON file, retrieve them by key, and pass them to ReaderService.
How do I close the connection properly?
Either call service.close() or rely on finally: blocks in your code. You could also turn ReaderService into a context manager (with ReaderService(...) as service:) for automatic cleanup.
Contributing

Fork or clone the repository.
Make your feature branch.
Add tests if needed, particularly for new data sources.
Submit a pull request.
License

This project is provided under [Your License of Choice], see LICENSE file for details.


