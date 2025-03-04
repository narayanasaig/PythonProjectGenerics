# main.py
import sys
import argparse
from services.reader_service import ReaderService

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--environment", required=True, help="e.g. DEV, PROD")
    parser.add_argument("--source", required=True, help="s3 or postgres or oracle")
    parser.add_argument("--bucket", help="S3 bucket name")
    parser.add_argument("--prefix", help="S3 prefix")
    parser.add_argument("--file_pattern", help="File pattern for S3 files")
    parser.add_argument("--query", help="SQL query or partial query")
    parser.add_argument("--params", nargs="*", default=[], help="Query parameters")
    args = parser.parse_args()

    # Create the ReaderService with all the inputs
    service = ReaderService(
        environment=args.environment,
        source=args.source.lower(),
        bucket=args.bucket,
        prefix=args.prefix,
        file_pattern=args.file_pattern,
        query=args.query,
        params=args.params
    )

    try:
        # Call a single method to read data, returning a DataFrame
        df = service.read_as_dataframe()
        print(df.head())  # Or do something else with the data
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        # Ensure resources are closed
        service.close()

if __name__ == "__main__":
    main()
