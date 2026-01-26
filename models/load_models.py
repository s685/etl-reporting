from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.logger import logger
from datamart_analytics.models.custom_models import DatamartTable_integrated


def create_execution_log_table(
    snowpark_connector: SnowparkConnector,
    datamart_table: DatamartTable_integrated,
) -> None:
    """
    Create execution log and control tables in the target Snowflake database.
    This function ensures that the required execution log and control tables are created
    in the target database and schema if they do not already exist.

    Parameters:
        snowpark_connector: SnowparkConnector
            The Snowpark connector object for connecting to the target Snowflake environment.
        datamart_table: DatamartTable_integrated
            The datamart table configuration containing target database and schema information.
    Returns:
        None
    """
    try:
        # Define table names
        execution_table_name = "DATAMART_EXECUTION_LOG_TABLE"

        # Check if the execution table exists
        execution_table_exists = snowpark_connector.execute_query(
            query=f"SHOW TABLES LIKE '{execution_table_name}' IN SCHEMA {datamart_table.target_schema}",
            lazy=False,
        )

        # Define the table creation SQL for DATAMART_EXECUTION_LOG_TABLE
        execution_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {datamart_table.target_database}.{datamart_table.target_schema}.{execution_table_name} (
            ID STRING(255) NOT NULL PRIMARY KEY,
            CARRIER_NAME STRING(255) NOT NULL,
            DATABASE_NAME STRING(255) NOT NULL,
            FOLDER_NAME STRING(255) NOT NULL,
            --SCHEMA_NAME STRING(255) NOT NULL,
            TABLE_OR_SCRIPT_NAME STRING(255) NOT NULL,
            INSERTED_ROW_COUNT NUMBER DEFAULT 0,
            UPDATED_ROW_COUNT NUMBER DEFAULT 0,
            START_TIMESTAMP STRING NOT NULL,
            END_TIMESTAMP STRING NOT NULL,
            QUERY_DURATION STRING,
            GENERAL_ERROR_MESSAGE STRING,
            STATUS STRING
        )
        """

        if execution_table_exists and len(execution_table_exists) > 0:
            logger.info(
                f"Table '{execution_table_name}' already exists in schema '{datamart_table.target_schema}'"
            )
        else:
            logger.info("Creating execution log table")
            snowpark_connector.execute_query(
                execution_table_sql, lazy=False
            )
            logger.info(
                f"Execution log table '{execution_table_name}' created successfully in schema '{datamart_table.target_schema}'"
            )

    except Exception as e:
        logger.error(f"Failed to create execution log table in Snowflake. Error: {e}")
        raise


def create_execution_metadata_table(
    snowpark_connector: SnowparkConnector,
    datamart_table: DatamartTable_integrated,
) -> None:
    """
    Ensures that the required DATAMART_EXECUTION_METADATA_TABLE table is created in the target database and schema if it does not already exist.

    Parameters:
        snowpark_connector: SnowparkConnector
            The Snowpark connector object for connecting to the target Snowflake environment.
        datamart_table: DatamartTable_integrated
            The datamart table configuration containing target database and schema information.
    Returns:
        None
    """
    try:
        # Define table name
        metadata_table_name = "DATAMART_EXECUTION_METADATA_TABLE"

        # Check if the table exists
        metadata_table_exists = snowpark_connector.execute_query(
            query=f"SHOW TABLES LIKE '{metadata_table_name}' IN SCHEMA {datamart_table.target_schema}",
            lazy=False,
        )

        if metadata_table_exists and len(metadata_table_exists) > 0:
            logger.info(
                f"Table '{metadata_table_name}' already exists in schema '{datamart_table.target_schema}'"
            )
        else:
            # Define the table creation SQL
            metadata_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {datamart_table.target_database}.{datamart_table.target_schema}.{metadata_table_name} (
                PROCESS_NAME VARCHAR(255) NOT NULL,
                CARRIER_NAME VARCHAR(255) NOT NULL,
                LAST_LOAD_TIMESTAMP TIMESTAMP NOT NULL
            )
            """
            snowpark_connector.execute_query(metadata_table_sql, lazy=False)
            logger.info(
                f"Table '{metadata_table_name}' created successfully in schema '{datamart_table.target_schema}'"
            )

    except Exception as e:
        logger.error(
            f"Failed to ensure '{metadata_table_name}' table exists: {e}"
        )
        raise
