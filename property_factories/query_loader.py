# property_factories/query_loader.py

import logging
import json
from typing import Dict, Any, Tuple, List

from logging_config import get_logger


logger = get_logger(__name__)

class QueryLoader:
    """
    Loads SQL queries (and optional parameters) from queries.json, e.g.:
      {
        "employee_select": {
          "sql": "SELECT ... WHERE dept_id = %s",
          "params": [10]
        },
        ...
      }
    """

    def __init__(self, query_file: str) -> None:
        logger.info(f"[QueryLoader] Loading queries from '{query_file}'")
        with open(query_file, 'r') as f:
            self._queries: Dict[str, Dict[str, Any]] = json.load(f)

    def get_sql_and_params(self, key: str) -> Tuple[str, Tuple[Any, ...]]:
        if key not in self._queries:
            raise KeyError(f"Query '{key}' not found.")
        q_data = self._queries[key]
        sql = q_data.get("sql")
        if not sql:
            raise ValueError(f"Query '{key}' has no 'sql' field.")
        param_list = q_data.get("params", [])
        if not isinstance(param_list, list):
            raise ValueError(f"Query '{key}' 'params' must be a list.")
        return sql, tuple(param_list)
