# main.py
import sys
import argparse
from typing import List, Any
from services.reader_service import ReaderService
from python_project_generics.logging_config import get_logger
import gc

logger = get_logger(__name__)


# Removed convert_params function - moved to ReaderService

def main():
    """
    Main entry point for the data reader CLI application.

    Parses command line arguments, creates a ReaderService,
    and reads data as a DataFrame.
    """
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Read data from various sources')
    parser.add_argument("--environment", required=True, help="Environment (e.g., DEV, PROD)")
    parser.add_argument("--source", required=True, help="Source type (s3, postgres, oracle)")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--prefix", help="S3 prefix")
    parser.add_argument("--file_pattern", help="File pattern for S3 files")
    parser.add_argument("--query", help="SQL query or S3 URI")
    parser.add_argument("--params", nargs="*", default=[], help="Query parameters or S3 format")
    args = parser.parse_args()

    # Convert source to lowercase for consistency
    source = args.source.lower()


    try:
        # Create the ReaderService with all the inputs
        logger.info(f"[Main] Creating ReaderService with source={source}, environment={args.environment}")
        service = ReaderService(
            environment=args.environment,
            source=source,
            bucket=args.bucket,
            prefix=args.prefix,
            file_pattern=args.file_pattern,
            query=args.query,
            params=args.params
        )

        # Read data as DataFrame
        logger.info("[Main] Reading data as DataFrame")
        df = service.read_as_dataframe()

        # Log results
        if df.empty:
            logger.info("[Main] DataFrame is empty")
        else:
            logger.info(f"[Main] DataFrame shape: {df.shape}")
            logger.info(f"[Main] DataFrame head:\n{df.head()}")

    except Exception as e:
        logger.error(f"[Main] Error: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        logger.debug("[Main] Closing service")
        service.close()
        gc.collect()

if __name__ == "__main__":
    main()