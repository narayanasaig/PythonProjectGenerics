import sys
import argparse
import pandas as pd

from services.reader_service import ReaderService
from services.writer_service import WriterService
from python_project_generics.logging_config import get_logger

logger = get_logger(__name__)

def main():
    """
    Demonstration of using ROWID to update rows by region (non-PK).
      1) We read table1 (including ROWID).
      2) Filter rows by region in Python.
      3) Update last_changed_at, comments in the DataFrame.
      4) Use writer in 'update' mode with pk_cols=['ROWID'].

    Then we do the same PK-based approach for table2 with ID, plus copy to new rows.

    We accept:
      --query1: a SELECT statement for table1 that includes ROWID.
                e.g. "SELECT t.*, t.ROWID AS row_id FROM table1 t WHERE region='...' "
      --query2: a SELECT statement for table2 that includes ID.
      --region : a region to filter in the DataFrame
      --id     : a PK filter if desired (not used in this snippet, but available)
    """

    parser = argparse.ArgumentParser(description="Example program for ROWID-based or PK-based updates in Oracle.")
    parser.add_argument("--environment", required=True, help="Environment in db_config.yml (e.g. DEV_ORACLE)")
    parser.add_argument("--query1", required=True, help="SQL query for table1 that MUST include ROWID as row_id")
    parser.add_argument("--query2", required=True, help="SQL query for table2 that returns an 'ID' PK")

    # Extra arguments for the 'region' or 'id' filters
    parser.add_argument("--region", help="Region filter for table1 updates", default=None)
    parser.add_argument("--id", help="PK filter for table2 updates", default=None)

    # Possibly more arguments if you want to set new status, etc.
    parser.add_argument("--new_status", type=int, default=2, help="Status to set in table2")

    args = parser.parse_args()

    ####################################################
    # 1) READ table1 (including ROWID) via query1
    ####################################################
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
        logger.warning("[Main] No rows returned from query1; skipping table1 ROWID-based update.")
    else:
        ####################################################
        # 1a) Filter the DataFrame by region if provided
        ####################################################
        if args.region and "REGION" in df1.columns:
            df1_filtered = df1[df1["REGION"] == args.region]
        else:
            df1_filtered = df1.copy()

        if df1_filtered.empty:
            logger.warning("[Main] No rows match the given region in df1.")
        else:
            ####################################################
            # 1b) Update columns in the DataFrame
            #     We'll set last_changed_at, comments, etc.
            ####################################################
            df1_filtered["LAST_CHANGED_AT"] = pd.Timestamp.now()
            df1_filtered["COMMENTS"] = ""

            # Ensure we have a ROWID column
            if "ROW_ID" not in df1_filtered.columns:
                logger.error("[Main] df1 does not contain ROW_ID column. "
                             "Please ensure your query1 includes 'ROWID AS row_id' or 'AS ROW_ID'.")
            else:
                # We'll rename the ROW_ID column to a name that matches how we pass pk_cols
                # i.e. if 'ROW_ID' is in df, we can rename it to 'ROWID' or keep it as 'ROW_ID'.
                # The writer's update_data() uses pk_cols = ["ROW_ID"] or ["ROWID"].
                df1_filtered.rename(columns={"ROW_ID": "ROWID"}, inplace=True)  # unify naming

                ####################################################
                # 1c) Use the writer in 'update' mode, pk_cols=['ROWID']
                ####################################################
                wr_svc_1 = WriterService(
                    db_type="oracle",
                    config_file="db_config.yml",
                    environment=args.environment,
                    mode="update",
                    table="table1"
                )

                try:
                    logger.info("[Main] Updating table1 rows using ROWID as PK.")
                    wr_svc_1.write_dataframe(
                        df1_filtered,
                        mode="update",
                        pk_cols=["ROWID"]  # We treat ROWID as a pseudo-PK
                    )
                    logger.info("[Main] ROWID-based update on table1 done.")
                except Exception as e:
                    logger.error(f"[Main] Error updating table1 with ROWID: {e}", exc_info=True)
                finally:
                    wr_svc_1.close()

    ####################################################
    # 2) READ table2 (with ID) via query2
    ####################################################
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
        logger.warning("[Main] No rows returned from query2; skipping table2 PK-based update/copy.")
        sys.exit(0)

    ####################################################
    # 2a) PK-based update on table2 => set status
    ####################################################
    if "ID" not in df2.columns:
        logger.error("[Main] df2 has no ID column. Cannot do PK-based update_data().")
    else:
        df2["STATUS"] = args.new_status
        wr_svc_2 = WriterService(
            db_type="oracle",
            config_file="db_config.yml",
            environment=args.environment,
            mode="update",
            table="table2"
        )
        try:
            logger.info("[Main] Updating table2 => set status=? for the rows from query2.")
            wr_svc_2.write_dataframe(
                df2,
                mode="update",
                pk_cols=["ID"]
            )
            logger.info("[Main] PK-based update on table2 done.")
        except Exception as e:
            logger.error(f"[Main] Error updating table2: {e}", exc_info=True)
        finally:
            wr_svc_2.close()

    ####################################################
    # 2b) Copy updated rows => insert new record with new PK from sequence
    ####################################################
    df2_new = df2.copy()
    df2_new["ID"] = None
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
            sequence_name="TABLE2"
        )
        logger.info("[Main] Insert with new ID complete.")
    except Exception as e:
        logger.error(f"[Main] Error inserting new rows into table2: {e}", exc_info=True)
    finally:
        wr_svc_3.close()

    logger.info("[Main] All steps completed successfully.")


if __name__ == "__main__":
    main()
