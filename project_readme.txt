my_project/
├── config/
│   ├── db_config.yml       # Environment-specific DB configs
│   ├── env_selector.json   # Tiny file specifying which environment to use
│   └── queries.json        # SQL queries (and optional parameters)
├── readers/
│   ├── base_reader.py      # Protocol/generic interface
│   ├── postgres_reader.py  # Concrete Postgres/Aurora implementation
│   ├── oracle_reader.py    # Concrete Oracle implementation
│   └── connection_factory.py
├── utils/
│   ├── property_factory.py # Reads YAML for the environment chosen
│   └── query_loader.py     # Loads SQL statements (and params) from JSON
└── main.py                 # Entry point
