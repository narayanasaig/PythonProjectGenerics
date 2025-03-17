import sys
import argparse
import pandas as pd

from services.reader_service import ReaderService
from services.writer_service import WriterService
from python_project_generics.logging_config import get_logger

logger = get_logger(__name__)


def main():
    """
    Demonstration of:
      1) Reading from table1 with a 'region' filter.
         - Then updating last_changed_at to current date/time, comments to ''.
      2) Reading from table2 with an 'id' filter.
         - Updating status=2 for those rows (id is PK).
         - Copying those updated rows into table2 with a *new* primary key
           fetched from your custom sequence logic (via 'sequence_name').

    The code uses two queries passed in arguments:
      --query1  for table1
      --query2  for table2
    Optionally, you can pass more, but we'll focus on these two.
    """

    parser = argparse.ArgumentParser(description="Example program reading & updating Oracle data.")
    parser.add_argument("--environment", required=True, help="Environment in db_config.yml (e.g. DEV_ORACLE)")
    parser.add_argument("--query1", required=True, help="SQL query for table1 or the region filter scenario")
    parser.add_argument("--query2", required=True, help="SQL query for table2 or the PK filter scenario")

    # Extra arguments for the 'region' or 'id' filters
    parser.add_argument("--region", help="Region filter for table1 updates", default=None)
    parser.add_argument("--id", help="PK filter for table2 updates", default=None)

    # Possibly more arguments if you want to set new status, etc.
    parser.add_argument("--new_status", type=int, default=2, help="Status to set in table2")

    args = parser.parse_args()

    # Step 1: Use ReaderService to read from table1
    logger.info("[Main] Creating ReaderService for query1 on table1.")
    read_svc_1 = ReaderService(
        environment=args.environment,
        source="oracle",
        query=args.query1
    )

    try:
        logger.info(f"[Main] Reading data for query1: {args.query1}")
        df1 = read_svc_1.read_as_dataframe()
        logger.info(f"[Main] df1 shape: {df1.shape}")
    except Exception as e:
        logger.error(f"[Main] Error reading table1 data: {e}", exc_info=True)
        sys.exit(1)
    finally:
        read_svc_1.close()

    if df1.empty:
        logger.warning("[Main] No rows returned from query1; skipping table1 update logic.")
    else:
        # 1a) Filter by region (non-PK). Let's assume 'region' is a column in df1.
        if args.region:
            df1_filtered = df1[df1['REGION'] == args.region]
        else:
            df1_filtered = df1.copy()

        if df1_filtered.empty:
            logger.warning("[Main] No rows match the given region in df1.")
        else:
            # 1b) Set last_changed_at to current date/time, comments=''
            # We do it in the DataFrame. Then we'll use writer to do a chunk-based update.
            # But region is not PK, so we might not be able to do a direct PK-based update_data approach.
            # Alternatively, we can do a custom direct SQL update. Let's show a direct approach:

            df1_filtered['LAST_CHANGED_AT'] = pd.Timestamp.now()  # or some approach to store
            df1_filtered['COMMENTS'] = ''

            # But region is not PK => we can't do a standard 'where pk=...' approach.
            # Option 1: We do a row-by-row update using custom SQL based on region or unique keys.
            # Option 2: We do a direct single statement approach:
            #   UPDATE table1 SET last_changed_at=SYSDATE, comments='' WHERE region=?
            # We'll do Option 2 with WriterService custom logic.

            # We'll build a direct SQL approach in the writer, or do it inline here.

            # For demonstration, let's do a single statement:
            update_sql_1 = f"""
                UPDATE table1
                SET last_changed_at = SYSDATE,
                    comments       = ''
                WHERE region = :region
            """

            # We'll use the writer to execute custom SQL (assuming we can).
            # If your writer doesn't have a 'direct_execute' method, we show how to do it inline:

            wr_svc_1 = WriterService(
                db_type="oracle",
                config_file="db_config.yml",
                environment=args.environment,
                mode="update",     # just so the fail-fast on 'mode' is satisfied, but we'll do custom logic
                table="table1"     # not strictly used in the custom SQL
            )

            try:
                logger.info(f"[Main] Updating table1 last_changed_at/comments for region={args.region}")
                # We'll do direct inline logic (since region is not PK).
                conn = wr_svc_1._writer.connect()  # or wr_svc_1.get_connection() if you made that method
                cur = conn.cursor()
                cur.execute(update_sql_1, region=args.region)
                conn.commit()
                logger.info("[Main] table1 update for region done.")
            except Exception as e:
                logger.error(f"[Main] Error updating table1 by region: {e}", exc_info=True)
            finally:
                wr_svc_1._writer.close()  # or wr_svc_1.close()

    # Step 2: Use ReaderService to read from table2 with "id" filter
    logger.info("[Main] Creating ReaderService for query2 on table2.")
    read_svc_2 = ReaderService(
        environment=args.environment,
        source="oracle",
        query=args.query2
    )

    try:
        logger.info(f"[Main] Reading data for query2: {args.query2}")
        df2 = read_svc_2.read_as_dataframe()
        logger.info(f"[Main] df2 shape: {df2.shape}")
    except Exception as e:
        logger.error(f"[Main] Error reading table2 data: {e}", exc_info=True)
        sys.exit(1)
    finally:
        read_svc_2.close()

    if df2.empty:
        logger.warning("[Main] No rows returned from query2; skipping table2 update and copy.")
        sys.exit(0)

    # 2a) Update table2 => set status=2 => ID is PK
    # We'll do a typical pk-based update approach:
    df2['STATUS'] = args.new_status  # e.g. 2

    # We'll call WriterService with 'mode=update' and pk_cols=['ID'] (assuming 'ID' is the PK).
    # If your PK column is literally called 'ID', or 'ID' is the one from df2
    if 'ID' not in df2.columns:
        logger.error("[Main] The DataFrame from table2 doesn't have 'ID' column. Can't do PK-based update.")
    else:
        wr_svc_2 = WriterService(
            db_type="oracle",
            config_file="db_config.yml",
            environment=args.environment,
            mode="update",
            table="table2"
        )

        try:
            logger.info("[Main] Updating table2 => set status=2 for the rows from query2.")
            wr_svc_2.write_dataframe(
                df2,
                mode="update",
                pk_cols=["ID"]  # the PK
            )
            logger.info("[Main] table2 updated successfully.")
        except Exception as e:
            logger.error(f"[Main] Error updating table2: {e}", exc_info=True)
        finally:
            wr_svc_2.close()

    # 2b) Copy those updated rows => insert new record with new PK from sequence
    # We'll take df2 (which we just updated). We want to insert them back into table2
    # but with a new ID from your custom sequence approach.
    # We'll remove or revert 'ID' so the writer can fill it from e.g. sequence_name='TABLE2'
    # or we rename it if you prefer. Let's do a new df:
    df2_new = df2.copy()
    # Reset ID so the writer does a sequence fill:
    df2_new['ID'] = None  # so _fill_df_pk_with_sequence can fill it
    # Possibly you want to reset 'STATUS' again or keep it as 2. Up to your logic.

    # Now do an insert with sequence_name='TABLE2' if your "Sequences" table has ENTITYNAME='TABLE2'
    wr_svc_3 = WriterService(
        db_type="oracle",
        config_file="db_config.yml",
        environment=args.environment,
        mode="insert",
        table="table2"
    )

    try:
        logger.info("[Main] Inserting new rows in table2 with new ID from custom sequence.")
        wr_svc_3.write_dataframe(
            df2_new,
            mode="insert",
            pk_col="ID",
            sequence_name="TABLE2"  # Matches your SEQUENCES.ENTITYNAME if that's your config
        )
        logger.info("[Main] Insert with new ID complete.")
    except Exception as e:
        logger.error(f"[Main] Error inserting new rows into table2: {e}", exc_info=True)
    finally:
        wr_svc_3.close()

    logger.info("[Main] All steps completed successfully.")


if __name__ == "__main__":
    main()
