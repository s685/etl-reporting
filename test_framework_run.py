import argparse
import os

from datamart_analytics.custom_exceptions.test_framework_exceptions import (
    CompareSQLToSnowflakeException,
    CSVFileNotFoundException,
    LoadTestException,
    NoRowsValidatedException,
    UnhandledFrameworkException,
)
from datamart_analytics.definitions.custom_definitions import (
    SourceTargetDatabaseConnectionType,
)
from datamart_analytics.logger import logger
from datamart_analytics.tools.test_framework_helper import (
    analyze_and_log_results,
    compare_source_sql_to_target_snowflake_data,
    get_snowflake_credentials,
    get_sql_server_credentials,
    load_test_case_cross_reference_table,
    log_validated_row,
    map_query_and_validate_test_case_data,
)



def load_test(
    source_database_name: str,
    target_database_name: str,
    database_warehouse: str,
    table_schema_name: str,
    specification_csv_path: str,
    source_target_database_connection_type: str,
    sql_server_host: str | None,
    sql_server_port: int,
    sql_to_snowflake_query_file_path: str | None
) -> None:
    """
    Load test cases and validate rows based on specifications.
    """

    try:
        snowflake_credentials = get_snowflake_credentials(database_warehouse, source_database_name, table_schema_name)
        if source_target_database_connection_type == SourceTargetDatabaseConnectionType.SQLSERVER_TO_SNOWFLAKE:
            if not sql_server_host or not sql_to_snowflake_query_file_path:
                raise LoadTestException("SQL Server host and query file path must be provided for SQLSERVER_TO_SNOWFLAKE_DATA_TESTING.")

            sql_server_credentials = get_sql_server_credentials(sql_server_host, source_database_name, sql_server_port)
            compare_source_sql_to_target_snowflake_data(sql_server_credentials, snowflake_credentials, source_database_name, target_database_name, sql_to_snowflake_query_file_path)

        elif source_target_database_connection_type == SourceTargetDatabaseConnectionType.SNOWFLAKE_TO_SNOWFLAKE:
            if not specification_csv_path or not os.path.exists(specification_csv_path):
                raise CSVFileNotFoundException(
                    f"Specification CSV file not found: {specification_csv_path or '(not provided)'}"
                )

            df = load_test_case_cross_reference_table(specification_csv_path)
            if df.empty:
                raise NoRowsValidatedException("No rows found in the test specification DataFrame.")

            validated_rows = map_query_and_validate_test_case_data(df, source_database_name, target_database_name, snowflake_credentials)
            if not validated_rows:
                raise NoRowsValidatedException("No rows were validated in the test.")

            result_list = [log_validated_row(row, snowflake_credentials) for row in validated_rows]
            analyze_and_log_results(result_list)
        else:
            raise LoadTestException(
                f"Unsupported source_target_database_connection_type: {source_target_database_connection_type}. "
                "Use SQLSERVER_TO_SNOWFLAKE or SNOWFLAKE_TO_SNOWFLAKE."
            )
    except CompareSQLToSnowflakeException:
        raise
    except Exception as e:
        logger.error(f"Error in load_test: {e}", exc_info=True)
        raise LoadTestException(f"Error in load_test: {e}")



if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description="Run the test framework with Snowflake connection parameters."
        )
        parser.add_argument("--source_database_name", type=str, required=True, help="Snowflake source database name.")
        parser.add_argument("--target_database_name", type=str, required=True, help="Snowflake target database name.")
        parser.add_argument("--database_warehouse", type=str, required=True, help="Snowflake warehouse name.")
        parser.add_argument("--table_schema_name", type=str, required=True, help="Snowflake schema name.")
        parser.add_argument("--specification_csv_path", type=str, help="Path to the test specification CSV file.")
        parser.add_argument("--source_target_database_connection_type", type=str, default=SourceTargetDatabaseConnectionType.SNOWFLAKE_TO_SNOWFLAKE, help="Source to target connection type.")
        parser.add_argument("--sql_server_host", type=str, help="SQL Server host name.")
        parser.add_argument("--sql_server_port", type=int, default=1433, help="SQL Server port number.")
        parser.add_argument("--sql_to_snowflake_query_file_path", type=str, help="Path to the SQL to Snowflake query file.")
        args = parser.parse_args()
        load_test(args.source_database_name, args.target_database_name, args.database_warehouse,
                args.table_schema_name, args.specification_csv_path, args.source_target_database_connection_type,
                args.sql_server_host, args.sql_server_port, args.sql_to_snowflake_query_file_path)
    except CompareSQLToSnowflakeException as e:
        logger.error(f"SQL-to-Snowflake test case(s) failed: {e}")
        raise
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        raise UnhandledFrameworkException(f"Unhandled exception: {e}")