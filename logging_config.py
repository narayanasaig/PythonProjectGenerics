# logging_config.py
import logging
import sys
from typing import Optional

# The exact format you specified
LOG_FORMAT = "%(asctime)s [%(funcName)s] %(levelname)s: %(message)s"
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S%z"


def setup_logging(level=logging.DEBUG) -> logging.Logger:
    """
    Sets up centralized logging configuration.

    Args:
        level: Logging level (default: INFO)

    Returns:
        logging.Logger: Configured logger instance
    """
    # Get the root logger
    logger = logging.getLogger()
    logger.setLevel(level)

    # Clear any existing handlers
    logger.handlers.clear()

    # Add console handler with the specified format
    handler = logging.StreamHandler()
    formatter = logging.Formatter(fmt=LOG_FORMAT, datefmt=DATE_FORMAT)
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def get_logger(name: Optional[str] = None) -> logging.Logger:
    """
    Gets a logger instance.

    Args:
        name: Optional logger name

    Returns:
        logging.Logger: Logger instance
    """
    return logging.getLogger(name)