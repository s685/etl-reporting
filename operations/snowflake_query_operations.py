from snowflake.snowpark.session import Session
from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.definitions.custom_definitions import (
    DatamartFrameworkTable,
)
from datamart_analytics.logger import logger


def create_execution_log_table(
    snowpark_connector_target: SnowparkConnector,
    target_database: str,
    target_schema: str,
) -> None:
    """
    Create execution log and control tables in the target Snowflake database.
    This function ensures that the required execution log and control tables are created
    in the target database and schema if they do not already exist.

    Parameters:
        snowpark_connector_target: SnowparkConnector
            The Snowpark connector object for connecting to the target Snowflake environment.
        target_database: str
            The name of the target database in Snowflake.
        target_schema: str
            The name of the target schema in Snowflake.
    Returns:
        None
    """
    try:
        with snowpark_connector_target as connector:
            target_session: Session = connector.session

            # Set the database and schema context
            logger.info(
                f"Using target database: {target_database}, schema: {target_schema}"
            )
            target_session.use_database(target_database)
            target_session.use_schema(target_schema)

            # Define the table creation SQL for execution log table
            execution_log_table_name = (
                DatamartFrameworkTable.MERGE_EXECUTION_LOG_TABLE.value
            )
            execution_log_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {target_database}.{target_schema}.{execution_log_table_name} (
                ID NUMBER AUTOINCREMENT PRIMARY KEY,
                TABLE_NAME STRING NOT NULL,
                MERGE_STATUS STRING NOT NULL,
                ROWS_INSERTED NUMBER,
                ROWS_UPDATED NUMBER,
                ROWS_DELETED NUMBER,
                EXECUTION_TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            # Execute the SQL to create the execution log table
            logger.info(
                f"Creating execution log table: {execution_log_table_name}"
            )
            target_session.sql(execution_log_table_sql).collect()
            logger.info(
                f"Execution log table '{execution_log_table_name}' created successfully."
            )
    except Exception as e:
        logger.error(
            f"Failed to create execution or control tables in Snowflake. Error: {e}"
        )
        raise
