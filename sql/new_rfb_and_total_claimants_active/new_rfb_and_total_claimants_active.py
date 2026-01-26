from datetime import datetime
from datamart_analytics.models.custom_models import DatamartTable
from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.logger import logger
from datamart_analytics.tools.datamart_utils import (
    create_and_parse_datamart_table_args,
    create_target_credentials,
)


def run_new_rfb_and_total_claimants_active(datamart_table: DatamartTable):
    """
    Run the new_rfb_and_total_claimants_active report.
    
    This report creates:
    1. service_type_by_vendor VIEW (reusable by other reports)
    2. new_rfb_and_total_claimants_active_detail TABLE
    3. new_rfb_and_total_claimants_active_summary TABLE
    
    Args:
        datamart_table: DatamartTable configuration with required parameters
    
    Raises:
        Exception: If report execution fails
    """
    execution_start = datetime.now()

    try:
        snowflake_credentials = create_target_credentials(datamart_table)

        with SnowparkConnector(snowflake_credentials) as connector:
            # Set session variables for SQL files
            logger.info("Setting session variables...")
            connector.set_session_variables_from_datamart_table(datamart_table)
            
            # Step 1: Create reusable view (session variables handled by Snowflake)
            logger.info("Step 1/3: Creating service_type_by_vendor view...")
            service_view_df = connector.execute_query_from_file(
                file_name="service_type_by_vendor_base.sql",
                datamart_table=datamart_table,
                lazy=True,
                folder_name="service_type_by_vendor",
            )
            connector.save_as_view(service_view_df, "service_type_by_vendor")
            logger.info("Successfully created service_type_by_vendor view")

            # Step 2: Create detail table
            logger.info("Step 2/3: Creating detail table...")
            detail_df = connector.execute_query_from_file(
                file_name="new_rfb_and_total_claimants_active_detail.sql",
                datamart_table=datamart_table,
                lazy=True,
                folder_name="new_rfb_and_total_claimants_active",
            )
            connector.save_as_table(detail_df, "new_rfb_and_total_claimants_active_detail")
            logger.info("Successfully created new_rfb_and_total_claimants_active_detail table")

            # Step 3: Create summary table
            logger.info("Step 3/3: Creating summary table...")
            summary_df = connector.execute_query_from_file(
                file_name="new_rfb_and_total_claimants_active_summary.sql",
                datamart_table=datamart_table,
                lazy=True,
                folder_name="new_rfb_and_total_claimants_active",
            )
            connector.save_as_table(summary_df, "new_rfb_and_total_claimants_active_summary")
            logger.info("Successfully created new_rfb_and_total_claimants_active_summary table")

            # Calculate execution time
            execution_end = datetime.now()
            duration = (execution_end - execution_start).total_seconds()
            
            logger.info("Report generation completed successfully")
            logger.info(f"Total execution time: {duration:.2f} seconds")


    except Exception as e:
        duration = (datetime.now() - execution_start).total_seconds()
        logger.error(f"Report failed after {duration:.2f}s: {e}", exc_info=True)
        raise





if __name__ == "__main__":
    # Use centralized argument parsing utility
    datamart_table = create_and_parse_datamart_table_args("new_rfb_and_total_claimants_active")
    run_new_rfb_and_total_claimants_active(datamart_table)
