import os
import sys
from pathlib import Path
import logging
#sys.path.append(os.path.dirname(os.path.abspath(__file__)))
#Path(os.path.dirname(__file__)).resolve()
init_path=Path(os.path.join(os.path.dirname(__file__))).resolve()
sys.path.append(str(init_path))


from logging_config import (setup_logging,get_logger)


logger = setup_logging(
    level=logging.INFO
    #log_file="python_data_aggregator.log"  # Optional: remove if file logging is not needed
)
logger.info("Initialized python_data_aggregator package.")

__all__ = [
    "setup_logging",
    "get_logger"
]