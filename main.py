# main.py

import logging
import sys
import json
import os
from pathlib import Path
from utils.property_factory import PropertyFactory
from readers.connection_factory import ConnectionFactory
from utils.query_loader import QueryLoader
from .logging_config import get_logger




main_path = Path(os.path.dirname(__file__)).resolve()
config_file= os.path.join(main_path, "config","env_selector.json")
db_file= os.path.join(main_path, "config","db_config.yml")
query_file=os.path.join(main_path, "config","queries.json")

def main():
#    logging.basicConfig(
#        level=logging.DEBUG,  # or INFO in production
#        format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
#        stream=sys.stdout
#   )
    logger = get_logger(__name__)
    logger.info("[Main] Starting application...")

    # 1) Load the environment from env_selector.json
    with open(config_file, "r") as f:
        env_data = json.load(f)
    environment = env_data.get("environment", "DEV")
    logger.info(f"[Main] Environment chosen: {environment}")

    # 2) Create a PropertyFactory from db_config.yml
    prop_factory = PropertyFactory(db_file, environment)

    # 3) Create a DBReader using the ConnectionFactory
    cf = ConnectionFactory(prop_factory)
    db_reader = cf.get_connection()

    # 4) Load queries from queries.json
    ql = QueryLoader(query_file)

    # 5) For demonstration, pick one query key
    query_key = "employee_select_postgres"  # or "employee_select_oracle"
    sql, params = ql.get_sql_and_params(query_key)
    logger.info(f"[Main] Executing query key='{query_key}' with params={params}")

    # 6) Execute (rows + DataFrame)
    rows = db_reader.execute_query(sql, params)
    logger.info(f"[Main] Rows returned: {rows}")

    df = db_reader.fetch_as_dataframe(sql, params)
    logger.info(f"[Main] DataFrame shape: {df.shape}")

    # 7) Cleanup
    db_reader.close()
    logger.info("[Main] Finished.")


if __name__ == "__main__":
    main()
