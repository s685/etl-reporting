import argparse
import os
from datamart_analytics.custom_exceptions.test_framework_exceptions import (
    CSVFileNotFoundException,
    LoadTestException,
    NoRowsValidatedException,
    UnhandledFrameworkException,
)
from datamart_analytics.environment import environment_configuration
from datamart_analytics.logger import logger
from datamart_analytics.models.custom_models import SnowflakeCredentials
from datamart_analytics.tools.test_framework_helper import (
    analyze_and_log_results,
    load_test_case_cross_reference_table,
    log_validated_row,
    map_query_and_validate_test_case_data,
)


def load_test(
    database_warehouse,
    source_database_name,
    target_database_name,
    table_schema_name,
    specification_csv_path,
):
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

        snowflake_credentials = SnowflakeCredentials(
            user=environment_configuration.snowflake_user_target,
            password=environment_configuration.snowflake_password_target,
            account=environment_configuration.snowflake_account,
            role=environment_configuration.snowflake_role_target,
            authenticator=environment_configuration.snowflake_authenticator,
            private_key_file=environment_configuration.snowflake_private_key_file,
            private_key_password=environment_configuration.snowflake_private_key_password,
            warehouse=database_warehouse,
            database=source_database_name,
            table_schema=table_schema_name,
        )

        validated_rows = map_query_and_validate_test_case_data(
            df, source_database_name, target_database_name
        )

        if not validated_rows:
            raise NoRowsValidatedException("No rows were validated in the test.")

        result_list = []
        for row in validated_rows:
            result_list.append(log_validated_row(row, snowflake_credentials))

        analyze_and_log_results(result_list)

    except Exception as e:
        logger.error(f"An error occurred in load_test: {e}")
        raise LoadTestException(f"An error occurred in load_test: {e}")


if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument(
            "--source_database_name", type=str, required=True
        )
        parser.add_argument(
            "--target_database_name", type=str, required=True
        )
        parser.add_argument(
            "--database_warehouse", type=str, required=True
        )
        parser.add_argument(
            "--table_schema_name", type=str, required=True
        )
        parser.add_argument(
            "--specification_csv_path", type=str, required=True
        )

        args = parser.parse_args()

        load_test(
            args.database_warehouse,
            args.source_database_name,
            args.target_database_name,
            args.table_schema_name,
            args.specification_csv_path,
        )

    except Exception as e:
        logger.error(f"An unhandled exception occurred: {e}")
        raise UnhandledFrameworkException(f"An unhandled exception occurred: {e}")
