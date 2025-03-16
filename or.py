# main.py
import sys
import argparse
import gc
from typing import List, Any
import pandas as pd

from services.reader_service import ReaderService
from services.writer_service import WriterService
from logging_config import get_logger

logger = get_logger(__name__)


def main():
    """
    Main entry point that can:
      1) Read from a source (Oracle, S3, Postgres) via ReaderService
      2) Optionally write to Oracle in 'insert' or 'update' mode via WriterService
         (by providing --mode and --table arguments).
    """

    # 1. Parse command line arguments
    parser = argparse.ArgumentParser(description="Read (and optionally write) data from various sources")

    # Required for ReaderService
    parser.add_argument("--environment", required=True, help="Environment (e.g., DEV, PROD)")
    parser.add_argument("--source", required=True, help="Source type (s3, postgres, oracle)")

    # ReaderService optional args for S3 or DB
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--prefix", help="S3 prefix")
    parser.add_argument("--file_pattern", help="File pattern for S3 files")
    parser.add_argument("--query", help="SQL query or S3 URI")
    parser.add_argument("--params", nargs="*", default=[], help="Query parameters or S3 format")

    # Arguments for WriterService / Oracle
    parser.add_argument("--mode", choices=["insert", "update"], help="Mode for Oracle Writer (insert/update)")
    parser.add_argument("--table", help="Oracle table name for writing")
    parser.add_argument("--pk_cols", nargs="*", default=[], help="Primary key columns if doing update")

    args = parser.parse_args()

    source = args.source.lower()

    # 2. Create the ReaderService and read a DataFrame
    logger.info(f"[Main] Creating ReaderService with source={source}, environment={args.environment}")
    reader_svc = ReaderService(
        environment=args.environment,
        source=source,
        bucket=args.bucket,
        prefix=args.prefix,
        file_pattern=args.file_pattern,
        query=args.query,
        params=args.params
    )

    df = pd.DataFrame()  # default empty
    try:
        logger.info("[Main] Reading data as DataFrame")
        df = reader_svc.read_as_dataframe()
        if df.empty:
            logger.info("[Main] DataFrame from reader is empty.")
        else:
            logger.info(f"[Main] DataFrame shape: {df.shape}")
            logger.info(f"[Main] DataFrame head:\n{df.head()}")
    except Exception as e:
        logger.error(f"[Main] Error reading data: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        # We close the reader once we're done
        logger.debug("[Main] Closing ReaderService")
        reader_svc.close()

    # 3. Optionally write to Oracle using WriterService
    #    Only proceed if user specified --mode and --table
    if args.mode and args.table and source == "oracle":
        # The WriterService needs a mode, table, and environment
        # The writer_factory does a fail-fast check on mode.
        try:
            logger.info(f"[Main] Creating WriterService for Oracle with mode={args.mode} and table={args.table}")
            writer_svc = WriterService(
                db_type="oracle",
                config_file="db_config.yml",    # Adjust path if needed
                environment=args.environment,
                mode=args.mode,
                table=args.table
            )

            # Let's do a simple scenario:
            # If the mode is 'insert', we just insert the df we read (assuming columns match).
            # If the mode is 'update', we pass pk_cols from CLI to the writer.
            # For a real scenario, you might build a new df to insert or update.

            if df.empty:
                logger.warning("[Main] There's no data in df to write. We skip the writing step.")
            else:
                write_kwargs = {"mode": args.mode}
                if args.mode == "update":
                    # For update, we require pk_cols
                    if not args.pk_cols:
                        logger.error("[Main] --pk_cols is required for update mode. Aborting write.")
                    else:
                        write_kwargs["pk_cols"] = args.pk_cols

                logger.info(f"[Main] Writing DataFrame in {args.mode} mode with table={args.table}")
                writer_svc.write_dataframe(df, **write_kwargs)

        except Exception as e:
            logger.error(f"[Main] Error writing data: {str(e)}", exc_info=True)
            sys.exit(1)
        finally:
            logger.debug("[Main] Closing WriterService")
            writer_svc.close()
    else:
        # No writing
        if args.mode and source == "oracle" and not args.table:
            logger.warning("[Main] 'mode' was specified but no '--table' given; skipping writing.")
        elif source == "oracle":
            logger.info("[Main] No write operation requested. Provide --mode and --table to write.")

    gc.collect()
    logger.info("[Main] Done.")


if __name__ == "__main__":
    main()
