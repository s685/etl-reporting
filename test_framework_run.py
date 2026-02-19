import argparse
import os

from custom_exceptions.test_framework_exceptions import (
    CSVFileNotFoundException,
    LoadTestException,
    NoRowsValidatedException,
    UnhandledFrameworkException,
)
from datamart_analytics.logger import logger
from definitions.custom_definitions import SourceTargetDatabaseConnectionType
from tools.test_framework_helper import (
    analyze_and_log_results,
    compare_source_sql_to_target_snowflake_data,
    get_snowflake_credentials,
    get_sql_server_credentials,
    load_test_case_cross_reference_table,
    log_validated_row,
    map_query_and_validate_test_case_data,
)


def load_test(
    database_warehouse: str,
    source_database_name: str,
    target_database_name: str,
    table_schema_name: str,
    specification_csv_path: str,
    source_target_database_connection_type: str,
    sql_server_host: str | None = None,
    sql_server_port: int | None = None,
    sql_to_snowflake_query_file_path: str | None = None,
) -> None:
    """
    Load test cases and validate rows based on specifications.
    """
    try:
        if not os.path.exists(specification_csv_path):
            raise CSVFileNotFoundException(
                f"Specification CSV file not found: {specification_csv_path}"
            )

        df = load_test_case_cross_reference_table(specification_csv_path)
        if df.empty:
            raise NoRowsValidatedException(
                "No rows found in the test specification DataFrame."
            )

        if (
            source_target_database_connection_type
            == SourceTargetDatabaseConnectionType.SNOWFLAKE_TO_SNOWFLAKE.value
        ):
            snowflake_credentials = get_snowflake_credentials(
                database_warehouse, source_database_name, table_schema_name
            )

            validated_rows = map_query_and_validate_test_case_data(
                df, source_database_name, target_database_name, snowflake_credentials
            )
            if not validated_rows:
                raise NoRowsValidatedException("No rows were validated in the test.")

            result_list = []
            for row in validated_rows:
                result_list.append(log_validated_row(row, snowflake_credentials))

            analyze_and_log_results(result_list)
            return

        if (
            source_target_database_connection_type
            == SourceTargetDatabaseConnectionType.SQLSERVER_TO_SNOWFLAKE.value
        ):
            if not sql_server_host or not sql_server_port or not sql_to_snowflake_query_file_path:
                raise LoadTestException(
                    "sql_server_host, sql_server_port, and sql_to_snowflake_query_file_path are required for SQL Server tests."
                )

            sql_server_credentials = get_sql_server_credentials(
                sql_server_host, source_database_name, sql_server_port
            )
            snowflake_credentials = get_snowflake_credentials(
                database_warehouse, source_database_name, table_schema_name
            )

            compare_source_sql_to_target_snowflake_data(
                sql_server_credentials,
                snowflake_credentials,
                source_database_name,
                target_database_name,
                sql_to_snowflake_query_file_path,
            )
            return

        raise LoadTestException(
            f"Unsupported source_target_database_connection_type: {source_target_database_connection_type}"
        )

    except Exception as e:
        logger.error(f"An error occurred in load_test: {e}", exc_info=True)
        raise LoadTestException(f"An error occurred in load_test: {e}") from e


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser(
            description="Run the test framework with Snowflake connection parameters."
        )
        parser.add_argument("--source_database_name", type=str, required=True)
        parser.add_argument("--target_database_name", type=str, required=True)
        parser.add_argument("--database_warehouse", type=str, required=True)
        parser.add_argument("--table_schema_name", type=str, required=True)
        parser.add_argument("--specification_csv_path", type=str, required=True)
        parser.add_argument(
            "--source_target_database_connection_type",
            type=str,
            required=True,
            help="Source to target connection type",
        )
        parser.add_argument("--sql_server_host", type=str, required=False)
        parser.add_argument("--sql_server_port", type=int, required=False)
        parser.add_argument("--sql_to_snowflake_query_file_path", type=str, required=False)

        args = parser.parse_args()

        load_test(
            args.database_warehouse,
            args.source_database_name,
            args.target_database_name,
            args.table_schema_name,
            args.specification_csv_path,
            args.source_target_database_connection_type,
            args.sql_server_host,
            args.sql_server_port,
            args.sql_to_snowflake_query_file_path,
        )

    except Exception as e:
        logger.error(f"An unhandled exception occurred: {e}", exc_info=True)
        raise UnhandledFrameworkException(f"An unhandled exception occurred: {e}") from e
