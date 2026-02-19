import ast
import json
import re
from typing import Any

import numpy as np
import pandas as pd
from pydantic import ValidationError
from sqlalchemy import create_engine

from custom_exceptions.test_framework_exceptions import (
    CompareSQLToSnowflakeException,
    NoTestResultsException,
    OneOrMoreTestCasesFailedException,
    SQLFileNotFoundException,
    SQLTemplateNotFoundException,
    TestCaseParseException,
    TestCaseValidationException,
)
from definitions.custom_definitions import SQLServerODBCVersion, TestCaseType
from models.test_framework_models import TestCaseMetadata

from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.environment import environment_configuration
from datamart_analytics.logger import logger
from datamart_analytics.models.custom_models import (
    SnowflakeCredentials,
    SQLServerCredentials,
)


def load_test_case_cross_reference_table(csv_path: str) -> pd.DataFrame:
    """
    Loads the test specifications from a CSV file, fills the last 4 columns with sample values
    if they are missing, and returns only rows where is_enabled is True (bool).
    """
    df = pd.read_csv(csv_path, keep_default_na=False)
    df = df[df["is_enabled"] == True]  # type: ignore
    df = df.reset_index(drop=True)
    return df


def get_snowflake_credentials(
    database_warehouse: str,
    source_database_name: str,
    table_schema_name: str,
) -> SnowflakeCredentials:
    """
    Returns a SnowflakeCredentials object using environment configuration and provided parameters.
    """
    return SnowflakeCredentials(
        user=environment_configuration.snowflake_user_target,
        password=environment_configuration.snowflake_password_target,
        account=environment_configuration.snowflake_account,
        warehouse=database_warehouse,
        database=source_database_name,
        table_schema=table_schema_name,
        role=environment_configuration.snowflake_role_target,
        authenticator=environment_configuration.snowflake_authenticator,
        private_key_file=environment_configuration.snowflake_private_key_file,
        private_key_password=environment_configuration.snowflake_private_key_password,
    )


def get_sql_server_credentials(
    sql_server_host: str,
    source_database_name: str,
    sql_server_port: int,
) -> SQLServerCredentials:
    """
    Returns a SQLServerCredentials object using environment configuration and provided parameters.
    Supports SSO (Windows Authentication) if user/password are not set.
    """
    if (
        environment_configuration.sql_server_user is None
        and environment_configuration.sql_server_password is None
    ):
        return SQLServerCredentials(
            server=sql_server_host,
            database=source_database_name,
            port=sql_server_port,
            user=None,
            password=None,
            driver=SQLServerODBCVersion.MS_ODBC_DRIVER_18.value,
        )
    return SQLServerCredentials(
        server=sql_server_host,
        database=source_database_name,
        port=sql_server_port,
        user=environment_configuration.sql_server_user,
        password=environment_configuration.sql_server_password,
        driver=SQLServerODBCVersion.MS_ODBC_DRIVER_18.value,
    )


def get_set_statements(is_set: bool, set_params: str) -> list[str] | None:
    """
    Get the list of SET statements if is_set is True, using key-value pairs from set_params.
    """
    if isinstance(is_set, str):
        is_set_bool = is_set.strip().lower() == "true"
    else:
        is_set_bool = bool(is_set)

    if (
        not is_set_bool
        or set_params is None
        or (isinstance(set_params, str) and set_params.strip().lower() == "none")
    ):
        return None

    try:
        if isinstance(set_params, str):
            params_dict = ast.literal_eval(set_params)
        elif isinstance(set_params, dict):
            params_dict = set_params
        else:
            return None

        if not isinstance(params_dict, dict):
            return None

        return create_set_statements(params_dict)
    except Exception as e:
        raise TestCaseValidationException(
            f"Failed to create SET statements from set_params: {set_params}. Error: {e}"
        ) from e


def parse_set_parameters(set_params_str: str) -> dict:
    """
    Converts a string representation of a dict (with single quotes) to a Python dictionary.
    Returns an empty dict if input is empty, 'None', or invalid.
    """
    if not set_params_str:
        return {}
    if isinstance(set_params_str, dict):
        return set_params_str
    if isinstance(set_params_str, str) and set_params_str.strip().lower() == "none":
        return {}
    try:
        return ast.literal_eval(set_params_str)
    except Exception:
        return {}


def create_set_statements(params_dict: dict[str, str]) -> list[str]:
    """
    Creates a list of SQL SET statements from a dictionary of parameters.
    """
    return [f"SET {param} = '{value}';" for param, value in params_dict.items()]


def parse_sql_file(path: str) -> dict[str, str]:
    """
    Parses the SQL file and extracts the blocks of @NAME and @QUERY pairs.
    """
    with open(path) as file:
        sql_text = file.read()

    test_case_pattern = r"-- START_TEST(.*?)-- END_TEST"
    test_case_blocks = re.findall(test_case_pattern, sql_text, re.DOTALL)
    name_query_mapping: dict[str, str] = {}

    for test_case_block in test_case_blocks:
        try:
            match = re.search(r"--\s*@NAME\s*:\s*(.+)", test_case_block)
            if not match:
                raise TestCaseParseException(
                    f"Missing @NAME in block:\n{test_case_block}"
                )
            name = match.group(1).strip()

            query_lines = re.split(
                r"--\s*@QUERY\s*:\s*\n", test_case_block, flags=re.DOTALL
            )
            if len(query_lines) < 2:
                raise TestCaseParseException(
                    f"Missing @QUERY in block:\n{test_case_block}"
                )
            query = query_lines[1].strip()

            name_query_mapping[name] = query
        except (AttributeError, ValidationError) as e:
            raise TestCaseParseException(
                f"Error parsing test case: {e}\nBlock: \n{test_case_block}\n"
            ) from e

    return name_query_mapping


def build_final_rendered_sql_query(
    final_row_dict: dict,
    snowflake_credentials: SnowflakeCredentials,
) -> str:
    """
    Builds the final SQL query by filling placeholders in mapped_sql_query using values from final_row_dict.
    Handles column_name and not_null_condition logic as required for NOT_NULL_CHECK.
    """
    if final_row_dict["test_case_type"] == TestCaseType.DATA_TESTING:
        with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
            if (
                final_row_dict.get("target_database_name")
                and final_row_dict.get("target_schema_name")
                and final_row_dict.get("target_table_name")
            ):
                df = sq.session.table(
                    f"{final_row_dict['target_database_name']}.{final_row_dict['target_schema_name']}.{final_row_dict['target_table_name']}"
                )
            else:
                df = sq.session.table(
                    f"{final_row_dict['source_database_name']}.{final_row_dict['source_schema_name']}.{final_row_dict['source_table_name']}"
                )

            columns = [field.name for field in df.schema.fields]
            if "carrier_name" in columns:
                final_row_dict["carrier_name_condition"] = (
                    f"carrier_name = '{final_row_dict['carrier_name'].strip()}'"
                )
            elif "carrier_name_dim_id" in columns:
                final_row_dict["carrier_name_condition"] = (
                    f"carrier_name_dim_id = MD5('{final_row_dict['carrier_name'].strip()}')"
                )
            else:
                final_row_dict["carrier_name_condition"] = "1=1"

    final_row_dict = create_column_conditions_for_final_rendered_query(final_row_dict)
    template = final_row_dict.get("mapped_sql_query", "")

    def replacer(match):
        key = match.group(1)
        return str(final_row_dict.get(key, match.group(0)))

    filled_query = re.sub(r"\{([A-Za-z0-9_]+)\}", replacer, template)
    return filled_query


def build_and_map_sql_query_to_row(
    row_dict: dict,
    sql_template: dict,
    snowflake_credentials: SnowflakeCredentials,
) -> dict:
    """
    Maps the SQL query from sql_template to the row_dict based on test_case_name.
    Updates row_dict['mapped_sql_query'] and then builds the final SQL query.
    """
    test_case_name = row_dict.get("test_case_name")
    if not test_case_name:
        raise ValueError("test_case_name is missing in row_dict")

    sql_query = sql_template.get(test_case_name)
    if not sql_query:
        raise ValueError(
            f"No SQL template found for test_case_name: {test_case_name}"
        )

    row_dict["mapped_sql_query"] = sql_query
    row_dict["final_rendered_sql_query"] = build_final_rendered_sql_query(
        row_dict, snowflake_credentials
    )
    return row_dict


def map_query_and_validate_test_case_data(
    df: pd.DataFrame,
    source_database_name: str,
    target_database_name: str,
    snowflake_credentials: SnowflakeCredentials,
) -> list[dict]:
    """
    For each row in the DataFrame, map the corresponding SQL query from the SQL template file
    (using test_case_name as the key), add it to the row as 'mapped_sql_query', and validate
    the row using the TestCaseMetadata pydantic schema.
    """
    validated_rows: list[dict] = []

    for idx, row in df.iterrows():
        sql_file_path = row.get("query_file_path")
        if not sql_file_path:
            raise SQLFileNotFoundException(f"No query_file_path found for row {idx}")

        sql_templates = parse_sql_file(sql_file_path)
        if not sql_templates:
            raise SQLTemplateNotFoundException(
                f"No SQL templates found in file: {sql_file_path}"
            )

        row_dict = row.to_dict()
        row_dict["source_database_name"] = source_database_name
        row_dict["target_database_name"] = target_database_name

        final_row_dict = build_and_map_sql_query_to_row(
            row_dict, sql_templates, snowflake_credentials
        )
        final_row_dict["set_params"] = parse_set_parameters(
            final_row_dict.get("set_params", "")
        )

        try:
            validated_params: TestCaseMetadata = TestCaseMetadata(**final_row_dict)
            validated_rows.append(validated_params.model_dump())
        except (KeyError, ValidationError) as e:
            raise TestCaseValidationException(
                f"Validation failed for row {idx}: {e}"
            ) from e
        except Exception as e:
            raise TestCaseValidationException(f"Error processing row {idx}: {e}") from e

    return validated_rows


def format_column(col: str) -> str:
    return f'"{col}"' if '"' in col else col


def create_column_conditions_for_final_rendered_query(row_dict: dict) -> dict:
    """
    Enriches the input dictionary with SQL condition strings based on the specified test case type.
    """
    source_columns_values = row_dict.get("source_column_name", "").strip()
    target_columns_values = row_dict.get("target_column_name", "").strip()

    if source_columns_values:
        columns = [col.strip() for col in source_columns_values.split(",")]
        row_dict["source_column_name"] = source_columns_values
    elif target_columns_values:
        columns = [col.strip() for col in target_columns_values.split(",")]
        row_dict["target_column_name"] = target_columns_values
    else:
        columns = []

    test_case_upper = row_dict.get("test_case_name", "").upper()

    if test_case_upper in ("UNIQUE_CHECK", "COMBINATION_COLUMN_UNIQUE_CHECK"):
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join(
                [format_column(col.strip()) for col in columns]
            )
    elif test_case_upper == "ORPHAN_CHECK":
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join(
                [format_column(col.strip()) for col in columns]
            )
    elif test_case_upper == "NOT_NULL_CHECK":
        if columns:
            row_dict["not_null_condition"] = " AND ".join(
                [f"{format_column(col)} IS NULL" for col in columns]
            )
        else:
            row_dict["not_null_condition"] = "-1=1"
    elif test_case_upper == "DEFAULT_VALUE_CHECK":
        default_values = row_dict.get("default_values", "")
        conditions = []
        if default_values:
            for pair in default_values.split(","):
                if "=" in pair:
                    col, val = pair.split("=", 1)
                    col = format_column(col.strip())
                    val = val.strip()
                    if not (val.startswith("'") and val.endswith("'")) and not val.replace(".", "", 1).isdigit():
                        val = f"'{val}'"
                    conditions.append(f"{col} = {val}")
        if conditions:
            row_dict["default_value_condition"] = " AND ".join(conditions)
        else:
            row_dict["default_value_condition"] = " AND ".join(
                [f"{format_column(col)} IS NULL" for col in columns] if columns else ["-1=1"]
            )
    elif test_case_upper == "NEGATIVE_VALUE_CHECK":
        numeric_columns = [format_column(col) for col in columns]
        if not numeric_columns:
            raise TestCaseValidationException(
                f"NEGATIVE_VALUE_CHECK requires at least one numeric column. Got: {columns}"
            )
        negative_value_condition = " AND ".join([f"{col} < 0" for col in numeric_columns])
        row_dict["negative_value_condition"] = negative_value_condition
    elif test_case_upper == "ZERO_VALUE_CHECK":
        numeric_columns = [format_column(col) for col in columns]
        if not numeric_columns:
            raise TestCaseValidationException(
                f"ZERO_VALUE_CHECK requires at least one numeric column. Got: {columns}"
            )
        zero_value_condition = " AND ".join([f"{col} = 0" for col in numeric_columns])
        row_dict["zero_value_condition"] = zero_value_condition

    return row_dict


def log_validated_row(
    validated_row: dict[str, Any],
    snowflake_credentials: SnowflakeCredentials,
) -> tuple[str, str, str, int] | None:
    """
    Logs the test result in JSON format with table, columns, and test_results.
    The entire JSON is enclosed in a table-like ASCII box.
    Returns a tuple: (table_name, test_case_name, status, failure_count) or None if an error occurs.
    """
    result_json: dict[str, Any] = {"test_results": []}

    if validated_row["test_case_type"] == TestCaseType.DATA_TESTING:
        if validated_row.get("source_database_name") and validated_row.get("source_schema_name") and validated_row.get("source_table_name"):
            table_name = f"{validated_row['source_database_name']}.{validated_row['source_schema_name']}.{validated_row['source_table_name']}"
        elif validated_row.get("target_database_name") and validated_row.get("target_schema_name") and validated_row.get("target_table_name"):
            table_name = f"{validated_row['target_database_name']}.{validated_row['target_schema_name']}.{validated_row['target_table_name']}"
        else:
            table_name = validated_row.get("fact_table_name", "UNKNOWN")
    else:
        table_name = validated_row.get("fact_table_name", "UNKNOWN")

    source_column_str = str(validated_row.get("source_column_name", "")).strip()
    target_column_str = str(validated_row.get("target_column_name", "")).strip()
    if source_column_str:
        columns = [col.strip() for col in source_column_str.split(",")]
    elif target_column_str:
        columns = [col.strip() for col in target_column_str.split(",")]
    else:
        columns = []

    result_json["TABLE_NAME"] = table_name
    result_json["COLUMNS"] = columns
    result_json["TEST_RESULTS"] = []

    try:
        query = validated_row.get("final_rendered_sql_query")
        if not query:
            raise Exception("No rendered SQL query found in validated_row.")

        with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
            if validated_row.get("is_set"):
                set_statements = get_set_statements(validated_row.get("is_set"), validated_row.get("set_params"))
                if set_statements:
                    all_statements = set_statements + [query]
                    result_df = sq.execute_multiple_statements(all_statements, lazy=True)
                else:
                    result_df = sq.execute_query(query, lazy=True)
            else:
                result_df = sq.execute_query(query, lazy=True)

            if result_df is not None and hasattr(result_df, "collect"):
                result_df = result_df.collect()
            else:
                raise Exception("Query execution did not return a valid result.")

    except Exception as e:
        test_result = {
            "TEST_CASE_NAME": validated_row.get("test_case_name"),
            "STATUS": "ERROR",
            "FAILURE_COUNT": None,
            "DETAILS": str(e),
        }
        result_json["TEST_RESULTS"].append(test_result)
        json_str = json.dumps(result_json, indent=2, default=str)
        box_width = max(len(line) for line in json_str.splitlines()) + 4
        table_box = f"\n{'=' * box_width}\n{json_str}\n{'=' * box_width}\n"
        logger.info(table_box)
        return None

    failure_count = 0
    details: list[dict] = []
    status = "PASS"

    if result_df:
        dict_list = [row.as_dict() if hasattr(row, "as_dict") else dict(row) for row in result_df]
        df = pd.DataFrame(dict_list)
        if not df.empty:
            failure_count = int(df.iloc[0].get("err_count", len(df))) if "err_count" in df.columns else len(df)
            status = "FAIL" if failure_count > 0 else "PASS"
            details = (df.head(10)).to_dict(orient="records")

    test_result = {
        "TEST_CASE_NAME": validated_row.get("test_case_name"),
        "STATUS": status,
        "FAILURE_COUNT": failure_count,
        "DETAILS": details,
    }
    result_json["TEST_RESULTS"].append(test_result)

    json_str = json.dumps(result_json, indent=2, default=str)
    box_width = max(len(line) for line in json_str.splitlines()) + 4
    table_box = f"\n{'=' * box_width}\n{json_str}\n{'=' * box_width}\n"
    logger.info(table_box)

    return (table_name, validated_row.get("test_case_name"), status, failure_count)


def analyze_and_log_results(result_list: list) -> None:
    """
    Analyze the result list, log each result, and raise exception if any test case failed.
    """
    if not result_list:
        raise NoTestResultsException("No test results to analyze.")

    failed_rows = [
        row for row in result_list if row is not None and len(row) > 3 and str(row[2]).upper() == "FAIL"
    ]

    if not failed_rows:
        return

    headers = ["TABLE_NAME", "TEST_CASE_NAME", "STATUS", "COUNT"]
    col_widths = [len(h) for h in headers]
    for row in failed_rows:
        for i in range(4):
            col_widths[i] = max(col_widths[i], len(str(row[i])))

    sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
    header_row = "|" + "|".join([f" {headers[i].ljust(col_widths[i])} " for i in range(4)]) + "|"
    table_lines = [sep, header_row, sep]
    for row in failed_rows:
        row_line = "|" + "|".join([f" {str(row[i]).ljust(col_widths[i])} " for i in range(4)]) + "|"
        table_lines.append(row_line)
    table_lines.append(sep)

    table_str = "\n".join(table_lines)
    logger.info("\nFailed Test Cases Summary:\n" + table_str)
    raise OneOrMoreTestCasesFailedException("One or more test cases failed. See logs for details.")


def extract_sql_to_snowflake_test_case_queries(
    sql_to_snowflake_query_file_path: str,
) -> list[dict[str, str | None]]:
    with open(sql_to_snowflake_query_file_path) as f:
        content = f.read()

    test_blocks = re.findall(r"-- START_TEST(.*?)-- END_TEST", content, re.DOTALL)
    results: list[dict[str, str | None]] = []
    for block in test_blocks:
        test_name = re.search(r"-- @TEST_NAME\\s*:(.*?)\\n", block)
        sql_server_query = re.search(
            r"-- @START_SQL_SERVER_QUERY:(.*?)-- @END_SQL_SERVER_QUERY",
            block,
            re.DOTALL,
        )
        snowflake_query = re.search(
            r"-- @START_SNOWFLAKE_QUERY:(.*?)-- @END_SNOWFLAKE_QUERY",
            block,
            re.DOTALL,
        )
        results.append(
            {
                "TEST_NAME": test_name.group(1).strip() if test_name else None,
                "SQL_SERVER_QUERY": sql_server_query.group(1).strip() if sql_server_query else None,
                "SNOWFLAKE_QUERY": snowflake_query.group(1).strip() if snowflake_query else None,
            }
        )
    return results


def get_sql_to_snowflake_final_rendered_query(template: str, params: dict) -> str:
    """
    Renders a SQL query by replacing placeholders in the template with values from params.
    Placeholders should be in the format {key}.
    """

    def replacer(match):
        key = match.group(1)
        return str(params.get(key, match.group(0)))

    return re.sub(r"\{([A-Za-z0-9_]+)\}", replacer, template)


def execute_sql_server_query(credentials: SQLServerCredentials, query: str) -> pd.DataFrame:
    """
    Connects to SQL Server using SQLAlchemy, executes the query, and returns the result as a DataFrame.
    """
    driver = credentials.driver.value if hasattr(credentials.driver, "value") else credentials.driver
    if credentials.user and credentials.password:
        connection_url = f"mssql+pyodbc://{credentials.user}:{credentials.password}@{credentials.server},{credentials.port}/{credentials.database}?driver={driver}&TrustServerCertificate=yes"
    else:
        connection_url = f"mssql+pyodbc://@{credentials.server},{credentials.port}/{credentials.database}?driver={driver}&TrustServerCertificate=yes"

    engine = create_engine(connection_url)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def compare_sql_server_and_snowflake_result_df(
    sql_server_result_df: pd.DataFrame,
    snowflake_result_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compares two DataFrames after normalizing column names to uppercase and aligning columns.
    Returns a DataFrame of rows in SQL Server but not in Snowflake.
    Raises Exception if columns do not match after normalization.
    """
    sql_server_result_df = sql_server_result_df.copy()
    snowflake_result_df = snowflake_result_df.copy()

    sql_server_result_df.columns = [col.strip().upper() for col in sql_server_result_df.columns]
    snowflake_result_df.columns = [col.strip().upper() for col in snowflake_result_df.columns]

    if set(sql_server_result_df.columns) != set(snowflake_result_df.columns):
        raise Exception(
            f"Column mismatch after normalization. SQL Server: {list(sql_server_result_df.columns)}, Snowflake: {list(snowflake_result_df.columns)}"
        )

    snowflake_result_df = snowflake_result_df.reindex(columns=list(sql_server_result_df.columns))

    sql_server_result_df = sql_server_result_df.replace({np.nan: "NULL", None: "NULL"})
    snowflake_result_df = snowflake_result_df.replace({np.nan: "NULL", None: "NULL"})

    for col in sql_server_result_df.columns:
        if pd.api.types.is_object_dtype(sql_server_result_df[col]):
            sql_server_result_df[col] = sql_server_result_df[col].astype(str).str.strip()
            snowflake_result_df[col] = snowflake_result_df[col].astype(str).str.strip()
        elif pd.api.types.is_float_dtype(sql_server_result_df[col]) or pd.api.types.is_float_dtype(snowflake_result_df[col]):
            sql_server_result_df[col] = pd.to_numeric(sql_server_result_df[col], errors="coerce").round(6)
            snowflake_result_df[col] = pd.to_numeric(snowflake_result_df[col], errors="coerce").round(6)
        elif pd.api.types.is_integer_dtype(sql_server_result_df[col]) or pd.api.types.is_integer_dtype(snowflake_result_df[col]):
            sql_server_result_df[col] = pd.to_numeric(sql_server_result_df[col], errors="coerce")
            snowflake_result_df[col] = pd.to_numeric(snowflake_result_df[col], errors="coerce")

    sql_server_result_df = sql_server_result_df.sort_values(by=list(sql_server_result_df.columns)).reset_index(drop=True)
    snowflake_result_df = snowflake_result_df.sort_values(by=list(snowflake_result_df.columns)).reset_index(drop=True)

    df_sql_minus_snowflake = pd.DataFrame(
        list(set(map(tuple, sql_server_result_df.values)) - set(map(tuple, snowflake_result_df.values))),
        columns=sql_server_result_df.columns,
    )
    return df_sql_minus_snowflake


def log_sql_server_vs_snowflake_comparison(test_case: dict[str, Any], df_sql_minus_snowflake: pd.DataFrame) -> None:
    """
    Logs a summary and up to 10 rows of df_sql_minus_snowflake in a boxed format for a given test case dict.
    """
    test_case_result = "PASS" if len(df_sql_minus_snowflake) == 0 else "FAIL"

    if not df_sql_minus_snowflake.empty:
        preview_df = df_sql_minus_snowflake.head(10)
        headers = list(preview_df.columns)
        col_widths = [len(h) for h in headers]
        for row in preview_df.itertuples(index=False):
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))

        sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
        header_row = "|" + "|".join([f" {headers[i].ljust(col_widths[i])} " for i in range(len(headers))]) + "|"
        table_lines = [sep, header_row, sep]
        for row in preview_df.itertuples(index=False):
            row_line = "|" + "|".join([f" {str(row[i]).ljust(col_widths[i])} " for i in range(len(headers))]) + "|"
            table_lines.append(row_line)
        table_lines.append(sep)
        table_str = "\n".join(table_lines)
    else:
        table_str = "(no unmatched rows)"

    summary_lines = [
        f"TEST_CASE_NAME: {test_case.get('TEST_NAME')}",
        f"SOURCE_DATABASE_NAME: {test_case.get('source_database_name')}",
        f"TARGET_DATABASE_NAME: {test_case.get('target_database_name')}",
        f"RESULT: {test_case_result}",
        f"FEATURE_COUNT: {len(df_sql_minus_snowflake)}",
        "ROWS IN SQL SERVER RESULT NOT IN SNOWFLAKE RESULT:",
    ]

    all_lines = summary_lines + [table_str]
    max_width = max(len(line) for line in all_lines)
    border = "*" * (max_width + 4)
    box = [border] + [f"* {line.ljust(max_width)} *" for line in all_lines] + [border]
    logger.info("\n" + "\n".join(box))


def compare_source_sql_to_target_snowflake_data(
    sql_server_credentials: SQLServerCredentials,
    snowflake_credentials: SnowflakeCredentials,
    source_database_name: str,
    target_database_name: str,
    sql_to_snowflake_query_file_path: str,
) -> None:
    """
    Compares data between SQL Server and Snowflake based on the provided query file.
    """
    try:
        sql_to_snowflake_test_cases = extract_sql_to_snowflake_test_case_queries(sql_to_snowflake_query_file_path)
        if not sql_to_snowflake_test_cases:
            raise SQLTemplateNotFoundException(f"No SQL templates found in file: {sql_to_snowflake_query_file_path}")

        for test_case in sql_to_snowflake_test_cases:
            test_case["test_case_type"] = TestCaseType.SQLSERVER_TO_SNOWFLAKE_DATA_TESTING
            test_case["source_database_name"] = source_database_name
            test_case["target_database_name"] = target_database_name
            test_case["sql_to_snowflake_query_file_path"] = sql_to_snowflake_query_file_path

            test_case["final_rendered_sql_server_query"] = get_sql_to_snowflake_final_rendered_query(
                test_case.get("SQL_SERVER_QUERY") or "", test_case
            )
            test_case["final_rendered_snowflake_query"] = get_sql_to_snowflake_final_rendered_query(
                test_case.get("SNOWFLAKE_QUERY") or "", test_case
            )

            if not test_case["final_rendered_sql_server_query"]:
                raise Exception("No rendered SQL Server query found in test case.")
            sql_server_result_df = execute_sql_server_query(sql_server_credentials, test_case["final_rendered_sql_server_query"])

            if not test_case["final_rendered_snowflake_query"]:
                raise Exception("No rendered Snowflake SQL query found in test case.")

            with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
                snowflake_query_result = sq.execute_query(test_case["final_rendered_snowflake_query"], lazy=True)
                if snowflake_query_result is None:
                    raise Exception("Query execution did not return a valid result.")
                if hasattr(snowflake_query_result, "collect"):
                    snowflake_result_df = pd.DataFrame([row.as_dict() for row in snowflake_query_result.collect()])
                elif isinstance(snowflake_query_result, list):
                    snowflake_result_df = pd.DataFrame([row.as_dict() if hasattr(row, "as_dict") else dict(row) for row in snowflake_query_result])
                else:
                    raise Exception("Unexpected result type from SnowparkConnector.execute_query.")

            df_sql_minus_snowflake = compare_sql_server_and_snowflake_result_df(sql_server_result_df, snowflake_result_df)
            log_sql_server_vs_snowflake_comparison(test_case, df_sql_minus_snowflake)

    except Exception as e:
        raise CompareSQLToSnowflakeException(f"Error in comparing sql query and snowflake: {e}") from e

import ast
import json
import re
from typing import Any

import numpy as np
import pandas as pd
from pydantic import ValidationError
from sqlalchemy import create_engine

from custom_exceptions.test_framework_exceptions import (
    CompareSQLToSnowflakeException,
    NoTestResultsException,
    OneOrMoreTestCasesFailedException,
    SQLFileNotFoundException,
    SQLTemplateNotFoundException,
    TestCaseParseException,
    TestCaseValidationException,
)
from definitions.custom_definitions import SQLServerODBCVersion, TestCaseType
from models.test_framework_models import TestCaseMetadata

from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.environment import environment_configuration
from datamart_analytics.logger import logger
from datamart_analytics.models.custom_models import (
    SnowflakeCredentials,
    SQLServerCredentials,
)


def load_test_case_cross_reference_table(csv_path: str) -> pd.DataFrame:
    """
    Loads the test specifications from a CSV file, fills the last 4 columns with sample values
    if they are missing, and returns only rows where is_enabled is True (bool).
    """
    df = pd.read_csv(csv_path, keep_default_na=False)
    df = df[df["is_enabled"] == True]  # type: ignore
    df = df.reset_index(drop=True)
    return df


def get_snowflake_credentials(
    database_warehouse: str,
    source_database_name: str,
    table_schema_name: str,
) -> SnowflakeCredentials:
    """
    Returns a SnowflakeCredentials object using environment configuration and provided parameters.
    """
    return SnowflakeCredentials(
        user=environment_configuration.snowflake_user_target,
        password=environment_configuration.snowflake_password_target,
        account=environment_configuration.snowflake_account,
        warehouse=database_warehouse,
        database=source_database_name,
        table_schema=table_schema_name,
        role=environment_configuration.snowflake_role_target,
        authenticator=environment_configuration.snowflake_authenticator,
        private_key_file=environment_configuration.snowflake_private_key_file,
        private_key_password=environment_configuration.snowflake_private_key_password,
    )


def get_sql_server_credentials(
    sql_server_host: str,
    source_database_name: str,
    sql_server_port: int,
) -> SQLServerCredentials:
    """
    Returns a SQLServerCredentials object using environment configuration and provided parameters.
    Supports SSO (Windows Authentication) if user/password are not set.
    """
    if (
        environment_configuration.sql_server_user is None
        and environment_configuration.sql_server_password is None
    ):
        return SQLServerCredentials(
            server=sql_server_host,
            database=source_database_name,
            port=sql_server_port,
            user=None,
            password=None,
            driver=SQLServerODBCVersion.MS_ODBC_DRIVER_18.value,
        )
    return SQLServerCredentials(
        server=sql_server_host,
        database=source_database_name,
        port=sql_server_port,
        user=environment_configuration.sql_server_user,
        password=environment_configuration.sql_server_password,
        driver=SQLServerODBCVersion.MS_ODBC_DRIVER_18.value,
    )


def get_set_statements(is_set: bool, set_params: str) -> list[str] | None:
    """
    Get the list of SET statements if is_set is True, using key-value pairs from set_params.
    """
    if isinstance(is_set, str):
        is_set_bool = is_set.strip().lower() == "true"
    else:
        is_set_bool = bool(is_set)

    if (
        not is_set_bool
        or set_params is None
        or (isinstance(set_params, str) and set_params.strip().lower() == "none")
    ):
        return None

    try:
        if isinstance(set_params, str):
            params_dict = ast.literal_eval(set_params)
        elif isinstance(set_params, dict):
            params_dict = set_params
        else:
            return None

        if not isinstance(params_dict, dict):
            return None

        return create_set_statements(params_dict)
    except Exception as e:
        raise TestCaseValidationException(
            f"Failed to create SET statements from set_params: {set_params}. Error: {e}"
        ) from e


def parse_set_parameters(set_params_str: str) -> dict:
    """
    Converts a string representation of a dict (with single quotes) to a Python dictionary.
    Returns an empty dict if input is empty, 'None', or invalid.
    """
    if not set_params_str:
        return {}
    if isinstance(set_params_str, dict):
        return set_params_str
    if isinstance(set_params_str, str) and set_params_str.strip().lower() == "none":
        return {}
    try:
        return ast.literal_eval(set_params_str)
    except Exception:
        return {}


def create_set_statements(params_dict: dict[str, str]) -> list[str]:
    """
    Creates a list of SQL SET statements from a dictionary of parameters.
    """
    return [f"SET {param} = '{value}';" for param, value in params_dict.items()]


def parse_sql_file(path: str) -> dict[str, str]:
    """
    Parses the SQL file and extracts the blocks of @NAME and @QUERY pairs.
    """
    with open(path) as file:
        sql_text = file.read()

    test_case_pattern = r"-- START_TEST(.*?)-- END_TEST"
    test_case_blocks = re.findall(test_case_pattern, sql_text, re.DOTALL)
    name_query_mapping: dict[str, str] = {}

    for test_case_block in test_case_blocks:
        try:
            match = re.search(r"--\\s*@NAME\\s*:\\s*(.+)", test_case_block)
            if not match:
                raise TestCaseParseException(
                    f"Missing @NAME in block:\\n{test_case_block}"
                )
            name = match.group(1).strip()
            query_lines = re.split(
                r"--\\s*@QUERY\\s*:\\s*\\n", test_case_block, flags=re.DOTALL
            )
            if len(query_lines) < 2:
                raise TestCaseParseException(
                    f"Missing @QUERY in block:\\n{test_case_block}"
                )
            query = query_lines[1].strip()
            name_query_mapping[name] = query
        except (AttributeError, ValidationError) as e:
            raise TestCaseParseException(
                f"Error parsing test case: {e}\\nBlock: \\n{test_case_block}\\n"
            ) from e

    return name_query_mapping


def build_final_rendered_sql_query(
    final_row_dict: dict,
    snowflake_credentials: SnowflakeCredentials,
) -> str:
    """
    Builds the final SQL query by filling placeholders in mapped_sql_query using values from final_row_dict.
    Handles column_name and not_null_condition logic as required for NOT_NULL_CHECK.
    """
    if final_row_dict["test_case_type"] == TestCaseType.DATA_TESTING:
        with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
            if (
                final_row_dict.get("target_database_name")
                and final_row_dict.get("target_schema_name")
                and final_row_dict.get("target_table_name")
            ):
                df = sq.session.table(
                    f"{final_row_dict['target_database_name']}.{final_row_dict['target_schema_name']}.{final_row_dict['target_table_name']}"
                )
            else:
                df = sq.session.table(
                    f"{final_row_dict['source_database_name']}.{final_row_dict['source_schema_name']}.{final_row_dict['source_table_name']}"
                )

            columns = [field.name for field in df.schema.fields]
            if "carrier_name" in columns:
                final_row_dict["carrier_name_condition"] = (
                    f"carrier_name = '{final_row_dict['carrier_name'].strip()}'"
                )
            elif "carrier_name_dim_id" in columns:
                final_row_dict["carrier_name_condition"] = (
                    f"carrier_name_dim_id = MD5('{final_row_dict['carrier_name'].strip()}')"
                )
            else:
                final_row_dict["carrier_name_condition"] = "1=1"

    final_row_dict = create_column_conditions_for_final_rendered_query(final_row_dict)
    template = final_row_dict.get("mapped_sql_query", "")

    def replacer(match):
        key = match.group(1)
        return str(final_row_dict.get(key, match.group(0)))

    return re.sub(r"\\{([A-Za-z0-9_]+)\\}", replacer, template)


def build_and_map_sql_query_to_row(
    row_dict: dict,
    sql_template: dict,
    snowflake_credentials: SnowflakeCredentials,
) -> dict:
    """
    Maps the SQL query from sql_template to the row_dict based on test_case_name.
    Updates row_dict['mapped_sql_query'] and then builds the final SQL query.
    """
    test_case_name = row_dict.get("test_case_name")
    if not test_case_name:
        raise ValueError("test_case_name is missing in row_dict")

    sql_query = sql_template.get(test_case_name)
    if not sql_query:
        raise ValueError(
            f"No SQL template found for test_case_name: {test_case_name}"
        )

    row_dict["mapped_sql_query"] = sql_query
    row_dict["final_rendered_sql_query"] = build_final_rendered_sql_query(
        row_dict, snowflake_credentials
    )
    return row_dict


def map_query_and_validate_test_case_data(
    df: pd.DataFrame,
    source_database_name: str,
    target_database_name: str,
    snowflake_credentials: SnowflakeCredentials,
) -> list[dict]:
    """
    For each row in the DataFrame, map the corresponding SQL query from the SQL template file
    (using test_case_name as the key), add it to the row as 'mapped_sql_query', and validate
    the row using the TestCaseMetadata pydantic schema.
    """
    validated_rows: list[dict] = []

    for idx, row in df.iterrows():
        sql_file_path = row.get("query_file_path")
        if not sql_file_path:
            raise SQLFileNotFoundException(f"No query_file_path found for row {idx}")

        sql_templates = parse_sql_file(sql_file_path)
        if not sql_templates:
            raise SQLTemplateNotFoundException(
                f"No SQL templates found in file: {sql_file_path}"
            )

        row_dict = row.to_dict()
        row_dict["source_database_name"] = source_database_name
        row_dict["target_database_name"] = target_database_name
        row_dict = {str(k).strip(): (str(v).strip() if isinstance(v, str) else v) for k, v in row_dict.items()}

        final_row_dict = build_and_map_sql_query_to_row(
            row_dict, sql_templates, snowflake_credentials
        )
        final_row_dict["set_params"] = parse_set_parameters(
            final_row_dict.get("set_params", "")
        )

        try:
            validated_params: TestCaseMetadata = TestCaseMetadata(**final_row_dict)
            validated_rows.append(validated_params.model_dump())
        except (KeyError, ValidationError) as e:
            raise TestCaseValidationException(
                f"Validation failed for row {idx}: {e}"
            ) from e
        except Exception as e:
            raise TestCaseValidationException(f"Error processing row {idx}: {e}") from e

    return validated_rows


def format_column(col: str) -> str:
    return f'"{col}"' if '"' in col else col


def create_column_conditions_for_final_rendered_query(row_dict: dict) -> dict:
    """
    Enriches the input dictionary with SQL condition strings based on the specified test case type.
    """
    source_columns_values = row_dict.get("source_column_name", "").strip()
    target_columns_values = row_dict.get("target_column_name", "").strip()

    if source_columns_values:
        columns = [col.strip() for col in source_columns_values.split(",")]
        row_dict["source_column_name"] = source_columns_values
    elif target_columns_values:
        columns = [col.strip() for col in target_columns_values.split(",")]
        row_dict["target_column_name"] = target_columns_values
    else:
        columns = []

    test_case_upper = row_dict.get("test_case_name", "").upper()

    if test_case_upper in ("UNIQUE_CHECK", "COMBINATION_COLUMN_UNIQUE_CHECK"):
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join(
                [format_column(col.strip()) for col in columns]
            )
    elif test_case_upper == "ORPHAN_CHECK":
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join(
                [format_column(col.strip()) for col in columns]
            )
    elif test_case_upper == "NOT_NULL_CHECK":
        if columns:
            row_dict["not_null_condition"] = " OR ".join(
                [f"{format_column(col)} IS NULL" for col in columns]
            )
        else:
            row_dict["not_null_condition"] = "-1=1"
    elif test_case_upper == "DEFAULT_VALUE_CHECK":
        default_values = row_dict.get("default_values", "")
        if default_values and columns:
            default_value_conditions = []
            default_list = [val.strip() for val in str(default_values).split(",")]
            for i, col in enumerate(columns):
                default_val = default_list[i] if i < len(default_list) else default_list[0]
                default_value_conditions.append(
                    f"{format_column(col)} = '{default_val}'"
                )
            row_dict["default_value_condition"] = " OR ".join(default_value_conditions)
        else:
            row_dict["default_value_condition"] = ""
    elif test_case_upper == "NEGATIVE_VALUE_CHECK":
        numeric_columns = [
            format_column(col) for col in columns if col not in ["carrier_name", "carrier_name_dim_id"]
        ]
        if not numeric_columns:
            raise TestCaseValidationException(
                f"NEGATIVE_VALUE_CHECK requires at least one numeric column. Got: {columns}"
            )
        row_dict["negative_value_condition"] = " AND ".join(
            [f"{col} < 0" for col in numeric_columns]
        )
    elif test_case_upper == "ZERO_VALUE_CHECK":
        numeric_columns = [
            format_column(col) for col in columns if col not in ["carrier_name", "carrier_name_dim_id"]
        ]
        if not numeric_columns:
            raise TestCaseValidationException(
                f"ZERO_VALUE_CHECK requires at least one numeric column. Got: {columns}"
            )
        row_dict["zero_value_condition"] = " AND ".join(
            [f"{col} = 0" for col in numeric_columns]
        )

    return row_dict


def log_validated_row(
    validated_row: dict[str, Any],
    snowflake_credentials: SnowflakeCredentials,
) -> tuple[str, str, str, int] | None:
    """
    Logs the test result in JSON format with table, columns, and test_results.
    The entire JSON is enclosed in a table-like ASCII box.
    Returns a tuple: (table_name, test_case_name, status, failure_count) or None if an error occurs.
    """
    result_json: dict[str, Any] = {"test_results": []}
    block_sep = "\n" + "=" * 100

    if validated_row["test_case_type"] == TestCaseType.DATA_TESTING:
        if (
            validated_row.get("target_database_name")
            and validated_row.get("target_schema_name")
            and validated_row.get("target_table_name")
        ):
            table_name = f"{validated_row['target_database_name']}.{validated_row['target_schema_name']}.{validated_row['target_table_name']}"
        elif (
            validated_row.get("source_database_name")
            and validated_row.get("source_schema_name")
            and validated_row.get("source_table_name")
        ):
            table_name = f"{validated_row['source_database_name']}.{validated_row['source_schema_name']}.{validated_row['source_table_name']}"
        else:
            table_name = validated_row.get("fact_table_name", "UNKNOWN")
    else:
        table_name = validated_row.get("fact_table_name", "UNKNOWN")

    source_column_str = validated_row.get("source_column_name", "").strip()
    target_column_str = validated_row.get("target_column_name", "").strip()
    if source_column_str:
        columns = [col.strip() for col in source_column_str.split(",")]
    elif target_column_str:
        columns = [col.strip() for col in target_column_str.split(",")]
    else:
        columns = []

    result_json["table_name"] = table_name
    result_json["columns"] = columns
    result_json["TEST_RESULTS"] = []

    try:
        query = validated_row.get("final_rendered_sql_query")
        if not query:
            raise Exception("No rendered SQL query found in validated_row.")

        with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
            if validated_row.get("is_set"):
                set_statements = get_set_statements(
                    validated_row.get("is_set"),
                    validated_row.get("set_params"),
                )
                if set_statements:
                    all_statements = set_statements + [query]
                    result_df = sq.execute_multiple_statements(all_statements, lazy=True)
                else:
                    result_df = sq.execute_query(query, lazy=True)
            else:
                result_df = sq.execute_query(query, lazy=True)

            if result_df is not None and hasattr(result_df, "collect"):
                result_df = result_df.collect()
            else:
                raise Exception("Query execution did not return a valid result.")

    except Exception as e:
        test_result = {
            "TEST_CASE_NAME": validated_row.get("test_case_name"),
            "STATUS": "ERROR",
            "FAILURE_COUNT": None,
            "DETAILS": str(e),
        }
        result_json["TEST_RESULTS"].append(test_result)

        json_str = json.dumps(result_json, indent=2, default=str)
        box_width = max(len(line) for line in json_str.splitlines()) + 4
        table_box = f"\n{'=' * box_width}\n{json_str}\n{'=' * box_width}\n"
        logger.info(table_box)
        return None

    failure_count = 0
    details: list[dict] = []
    status = "PASS"

    if result_df:
        dict_list = [row.as_dict() if hasattr(row, "as_dict") else dict(row) for row in result_df]
        df = pd.DataFrame(dict_list)
        if not df.empty:
            failure_count = int(df.iloc[0].get("err_count", len(df))) if "err_count" in df.columns else len(df)
            status = "FAIL" if failure_count > 0 else "PASS"
            details = df.head(10).to_dict(orient="records")

    test_result = {
        "TEST_CASE_NAME": validated_row.get("test_case_name"),
        "STATUS": status,
        "FAILURE_COUNT": failure_count,
        "DETAILS": details,
    }
    result_json["TEST_RESULTS"].append(test_result)

    json_str = json.dumps(result_json, indent=2, default=str)
    box_width = max(len(line) for line in json_str.splitlines()) + 4
    table_box = f"\n{'=' * box_width}\n{json_str}\n{'=' * box_width}\n{block_sep}\n"
    logger.info(table_box)

    return (table_name, validated_row.get("test_case_name"), status, failure_count)


def analyze_and_log_results(result_list: list) -> None:
    """
    Analyze the result list, log each result, and raise exception if any test case failed.
    """
    if not result_list:
        raise NoTestResultsException("No test results to analyze.")

    failed_rows = [
        row
        for row in result_list
        if row is not None and len(row) >= 3 and str(row[2]).upper() == "FAIL"
    ]
    if not failed_rows:
        return

    headers = ["TABLE_NAME", "TEST_CASE_NAME", "STATUS", "COUNT"]
    col_widths = [len(h) for h in headers]
    for row in failed_rows:
        for i, val in enumerate(row[:4]):
            col_widths[i] = max(col_widths[i], len(str(val)))

    sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
    header_row = "|" + "|".join([f" {headers[i].ljust(col_widths[i])} " for i in range(4)]) + "|"
    table_lines = [sep, header_row, sep]

    for row in failed_rows:
        row_line = "|" + "|".join(
            [f" {str(row[i] if i < len(row) else '').ljust(col_widths[i])} " for i in range(4)]
        ) + "|"
        table_lines.append(row_line)

    table_lines.append(sep)
    table_str = "\n".join(table_lines)
    logger.info("\nFailed Test Cases Summary:\n" + table_str)

    raise OneOrMoreTestCasesFailedException(
        "One or more test cases failed. See logs for details."
    )


def extract_sql_to_snowflake_test_case_queries(
    sql_to_snowflake_query_file_path: str,
) -> list[dict[str, str | None]]:
    with open(sql_to_snowflake_query_file_path) as f:
        content = f.read()

    test_blocks = re.findall(r"-- START_TEST(.*?)-- END_TEST", content, re.DOTALL)
    results: list[dict[str, str | None]] = []
    for block in test_blocks:
        test_name = re.search(r"-- @TEST_NAME\\s*:(.*?)\\n", block)
        sql_server_query = re.search(
            r"-- @START_SQL_SERVER_QUERY:(.*?)-- @END_SQL_SERVER_QUERY",
            block,
            re.DOTALL,
        )
        snowflake_query = re.search(
            r"-- @START_SNOWFLAKE_QUERY:(.*?)-- @END_SNOWFLAKE_QUERY",
            block,
            re.DOTALL,
        )
        results.append(
            {
                "TEST_NAME": test_name.group(1).strip() if test_name else None,
                "SQL_SERVER_QUERY": sql_server_query.group(1).strip()
                if sql_server_query
                else None,
                "SNOWFLAKE_QUERY": snowflake_query.group(1).strip()
                if snowflake_query
                else None,
            }
        )
    return results


def get_sql_to_snowflake_final_rendered_query(template: str, params: dict) -> str:
    """
    Renders a SQL query by replacing placeholders in the template with values from params.
    Placeholders should be in the format {key}.
    """

    def replacer(match):
        key = match.group(1)
        return str(params.get(key, match.group(0)))

    return re.sub(r"\\{([A-Za-z0-9_]+)\\}", replacer, template)


def execute_sql_server_query(credentials: SQLServerCredentials, query: str) -> pd.DataFrame:
    """
    Connects to SQL Server using SQLAlchemy, executes the query, and returns the result as a DataFrame.
    """
    driver = (
        credentials.driver.value
        if hasattr(credentials.driver, "value")
        else credentials.driver
    )
    if credentials.user and credentials.password:
        connection_url = (
            f"mssql+pyodbc://{credentials.user}:{credentials.password}"
            f"@{credentials.server},{credentials.port}/{credentials.database}?driver={driver}&TrustServerCertificate=yes"
        )
    else:
        connection_url = (
            f"mssql+pyodbc://@{credentials.server},{credentials.port}/{credentials.database}"
            f"?driver={driver}&TrustServerCertificate=yes"
        )

    engine = create_engine(connection_url)
    with engine.connect() as conn:
        return pd.read_sql(query, conn)


def compare_sql_server_and_snowflake_result_df(
    sql_server_result_df: pd.DataFrame,
    snowflake_result_df: pd.DataFrame,
) -> pd.DataFrame:
    """
    Compares two DataFrames after normalizing column names to uppercase and aligning columns.
    Returns a DataFrame of rows in SQL Server but not in Snowflake.
    Raises Exception if columns do not match after normalization.
    """
    sql_server_result_df = sql_server_result_df.copy()
    snowflake_result_df = snowflake_result_df.copy()

    sql_server_result_df.columns = [col.strip().upper() for col in sql_server_result_df.columns]
    snowflake_result_df.columns = [col.strip().upper() for col in snowflake_result_df.columns]

    if set(sql_server_result_df.columns) != set(snowflake_result_df.columns):
        raise Exception(
            f"Column mismatch after normalization. SQL Server: {list(sql_server_result_df.columns)}, Snowflake: {list(snowflake_result_df.columns)}"
        )

    snowflake_result_df = snowflake_result_df.reindex(columns=list(sql_server_result_df.columns))

    sql_server_result_df = sql_server_result_df.replace({np.nan: "NULL", None: "NULL"})
    snowflake_result_df = snowflake_result_df.replace({np.nan: "NULL", None: "NULL"})

    for col in sql_server_result_df.columns:
        if pd.api.types.is_object_dtype(sql_server_result_df[col]):
            sql_server_result_df[col] = sql_server_result_df[col].astype(str).str.strip()
            snowflake_result_df[col] = snowflake_result_df[col].astype(str).str.strip()
        elif pd.api.types.is_float_dtype(sql_server_result_df[col]) or pd.api.types.is_float_dtype(
            snowflake_result_df[col]
        ):
            sql_server_result_df[col] = pd.to_numeric(
                sql_server_result_df[col], errors="coerce"
            ).round(6)
            snowflake_result_df[col] = pd.to_numeric(
                snowflake_result_df[col], errors="coerce"
            ).round(6)
        elif pd.api.types.is_integer_dtype(sql_server_result_df[col]) or pd.api.types.is_integer_dtype(
            snowflake_result_df[col]
        ):
            sql_server_result_df[col] = pd.to_numeric(
                sql_server_result_df[col], errors="coerce"
            )
            snowflake_result_df[col] = pd.to_numeric(
                snowflake_result_df[col], errors="coerce"
            )

    sql_server_result_df = sql_server_result_df.sort_values(by=list(sql_server_result_df.columns)).reset_index(drop=True)
    snowflake_result_df = snowflake_result_df.sort_values(by=list(snowflake_result_df.columns)).reset_index(drop=True)

    df_sql_minus_snowflake = pd.DataFrame(
        list(
            set(map(tuple, sql_server_result_df.values))
            - set(map(tuple, snowflake_result_df.values))
        ),
        columns=sql_server_result_df.columns,
    )
    return df_sql_minus_snowflake


def log_sql_server_vs_snowflake_comparison(
    test_case: dict[str, Any],
    df_sql_minus_snowflake: pd.DataFrame,
) -> None:
    """
    Logs a summary and up to 10 rows of df_sql_minus_snowflake in a boxed format for a given test case dict.
    """
    test_case_result = "PASS" if len(df_sql_minus_snowflake) == 0 else "FAIL"
    preview_df = (
        df_sql_minus_snowflake.head(10) if not df_sql_minus_snowflake.empty else df_sql_minus_snowflake
    )

    if not preview_df.empty:
        headers = list(preview_df.columns)
        col_widths = [len(h) for h in headers]
        for row in preview_df.itertuples(index=False):
            for i, val in enumerate(row):
                col_widths[i] = max(col_widths[i], len(str(val)))
        sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
        header_row = "|" + "|".join([f" {headers[i].ljust(col_widths[i])} " for i in range(len(headers))]) + "|"
        table_lines = [sep, header_row, sep]
        for row in preview_df.itertuples(index=False):
            table_lines.append(
                "|" + "|".join([f" {str(row[i]).ljust(col_widths[i])} " for i in range(len(headers))]) + "|"
            )
        table_lines.append(sep)
        table_str = "\n".join(table_lines)
    else:
        table_str = "(no unmatched rows)"

    summary_lines = [
        f"TEST_CASE_NAME: {test_case.get('TEST_NAME')}",
        f"SOURCE_DATABASE_NAME: {test_case.get('source_database_name')}",
        f"TARGET_DATABASE_NAME: {test_case.get('target_database_name')}",
        f"RESULT: {test_case_result}",
        f"FEATURE_COUNT: {len(df_sql_minus_snowflake)}",
        "ROWS IN SQL SERVER RESULT NOT IN SNOWFLAKE RESULT:",
    ]

    all_lines = summary_lines + [table_str]
    max_width = max(len(line) for line in all_lines) if all_lines else 0
    border = "*" * (max_width + 4)
    box = [border] + [f"* {line.ljust(max_width)} *" for line in all_lines] + [border]
    logger.info("\n" + "\n".join(box))


def compare_source_sql_to_target_snowflake_data(
    sql_server_credentials: SQLServerCredentials,
    snowflake_credentials: SnowflakeCredentials,
    source_database_name: str,
    target_database_name: str,
    sql_to_snowflake_query_file_path: str,
) -> None:
    """
    Compares data between SQL Server and Snowflake based on the provided query file.
    """
    try:
        sql_to_snowflake_test_cases = extract_sql_to_snowflake_test_case_queries(
            sql_to_snowflake_query_file_path
        )
        if not sql_to_snowflake_test_cases:
            raise SQLTemplateNotFoundException(
                f"No SQL templates found in file: {sql_to_snowflake_query_file_path}"
            )

        for test_case in sql_to_snowflake_test_cases:
            test_case["test_case_type"] = TestCaseType.SQLSERVER_TO_SNOWFLAKE_DATA_TESTING
            test_case["source_database_name"] = source_database_name
            test_case["target_database_name"] = target_database_name
            test_case["sql_to_snowflake_query_file_path"] = sql_to_snowflake_query_file_path

            test_case["final_rendered_sql_server_query"] = get_sql_to_snowflake_final_rendered_query(
                test_case.get("SQL_SERVER_QUERY") or "", test_case
            )
            test_case["final_rendered_snowflake_query"] = get_sql_to_snowflake_final_rendered_query(
                test_case.get("SNOWFLAKE_QUERY") or "", test_case
            )

            if not test_case.get("final_rendered_sql_server_query"):
                raise Exception("No rendered SQL Server query found in test case.")
            sql_server_result_df = execute_sql_server_query(
                sql_server_credentials, test_case["final_rendered_sql_server_query"]
            )

            if not test_case.get("final_rendered_snowflake_query"):
                raise Exception("No rendered Snowflake SQL query found in test case.")

            with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
                snowflake_query_result = sq.execute_query(
                    test_case["final_rendered_snowflake_query"], lazy=True
                )
                if snowflake_query_result is None:
                    raise Exception("Query execution did not return a valid result.")
                if hasattr(snowflake_query_result, "collect"):
                    snowflake_result_df = pd.DataFrame(
                        [row.as_dict() for row in snowflake_query_result.collect()]
                    )
                elif isinstance(snowflake_query_result, list):
                    snowflake_result_df = pd.DataFrame(
                        [
                            row.as_dict() if hasattr(row, "as_dict") else dict(row)
                            for row in snowflake_query_result
                        ]
                    )
                else:
                    raise Exception(
                        "Unexpected result type from SnowparkConnector.execute_query."
                    )

            df_sql_minus_snowflake = compare_sql_server_and_snowflake_result_df(
                sql_server_result_df, snowflake_result_df
            )
            log_sql_server_vs_snowflake_comparison(test_case, df_sql_minus_snowflake)

    except Exception as e:
        raise CompareSQLToSnowflakeException(
            f"Error in comparing sql query and snowflake: {e}"
        ) from e
import ast
import json
import re
from typing import Any
import pandas as pd
from datamart_analytics.custom_exceptions.test_framework_exceptions import (
    NoTestResultsException,
    OneOrMoreTestCasesFailedException,
    SQLFileNotFoundException,
    SQLTemplateNotFoundException,
    TestCaseParseException,
    TestCaseValidationException,
)
from datamart_analytics.definitions.custom_definitions import TestCaseType
from datamart_analytics.models.test_framework_models import TestCaseMetadata
from pydantic import ValidationError
from datamart_analytics.connector.snowpark_connector import SnowparkConnector
from datamart_analytics.logger import logger
from datamart_analytics.models.custom_models import SnowflakeCredentials


def load_test_case_cross_reference_table(csv_path: str) -> pd.DataFrame:
    """
    Loads the test specifications from a CSV file, fills the last 4 columns with sample values
    if they are missing, and returns only rows where is_enabled is True (bool).

    Args:
        csv_path (str): Path to the CSV file containing test specifications.

    Returns:
        pd.DataFrame: DataFrame containing only enabled test cases.
    """
    df = pd.read_csv(csv_path, keep_default_na=False)
    df = df[df["is_enabled"] == True]  # type: ignore
    df = df.reset_index(drop=True)
    return df


def generate_set_statements(params_dict: dict[str, str]) -> list[str]:
    """
    Generate a list of SQL SET statements from a dictionary of parameters.

    Args:
        params_dict (dict): Dictionary of parameter names and values.

    Returns:
        list[str]: List of SQL SET statements.
    """
    return [f"SET {param} = '{value}';" for param, value in params_dict.items()]


def parse_set_params(set_params_str: str) -> dict:
    """
    Parse a string representation of a dictionary into a Python dictionary.

    Args:
        set_params_str (str): String representation of a dictionary.

    Returns:
        dict: Parsed dictionary, or empty dict if parsing fails.
    """
    if not set_params_str or set_params_str.strip().lower() == "none":
        return {}

    if isinstance(set_params_str, dict):
        return set_params_str

    try:
        return ast.literal_eval(set_params_str)
    except (ValueError, SyntaxError):
        return {}


def create_set_statements(
    is_set: bool, set_params: str
) -> list[str] | None:
    """
    Executes SET statements if is_set is True, using key-value pairs from set_params.

    Args:
        is_set (str or bool): 'True' or 'False' (or bool).
        set_params (str or dict): String representation of dict, dict, or None.

    Returns:
        list[str] | None: List of SET statements or None.
    """
    # Normalize is_set to boolean
    if isinstance(is_set, str):
        is_set_bool = is_set.strip().lower() == "true"
    else:
        is_set_bool = bool(is_set)

    # Handle set_params as dict or str
    if (
        not is_set_bool
        or set_params is None
        or (isinstance(set_params, str) and set_params.strip().lower() == "none")
    ):
        return None

    try:
        if isinstance(set_params, str):
            params_dict = parse_set_params(set_params)
        else:
            params_dict = set_params

        if not isinstance(params_dict, dict):
            return None

        return generate_set_statements(params_dict)
    except Exception as e:
        raise TestCaseValidationException(
            f"Error parsing set_params: {str(e)}"
        )


def parse_sql_file(path: str) -> dict[str, str]:
    """
    Parses the SQL file and extracts the blocks of @NAME and @QUERY pairs.

    Args:
        path (str): Path to SQL file.

    Returns:
        Dict[str, str]: Returns the mapping of @NAME to the SQL query.
    """
    try:
        with open(path) as file:
            sql_text = file.read()

        # Pattern to match test case blocks
        test_case_pattern = r"-- START_TEST(.*?)-- END_TEST"
        test_case_blocks = re.findall(test_case_pattern, sql_text, re.DOTALL)

        name_query_mapping: dict[str, str] = {}

        for test_case_block in test_case_blocks:
            try:
                # Extract @NAME
                name_match = re.search(r"@NAME\s*:\s*(\w+)", test_case_block)
                if not name_match:
                    continue

                test_case_name = name_match.group(1)

                # Extract @QUERY
                query_match = re.search(
                    r"@QUERY\s*:\s*(.*?)(?=@|$)", test_case_block, re.DOTALL
                )
                if not query_match:
                    continue

                sql_query = query_match.group(1).strip()

                name_query_mapping[test_case_name] = sql_query
            except (AttributeError, ValidationError) as e:
                raise TestCaseParseException(
                    f"Error parsing test case: {e}\nBlock: \n{test_case_block}\n"
                ) from e

        return name_query_mapping
    except Exception as e:
        raise TestCaseParseException(f"Error parsing SQL file: {e}")


def build_final_rendered_sql_query(
    final_row_dict: dict, snowflake_credentials: SnowflakeCredentials
) -> str:
    """
    Builds the final SQL query by filling placeholders in mapped_sql_query using values from final_row_dict.
    Handles column_name and not_null_condition logic as required for NOT_NULL_CHECK.

    Args:
        final_row_dict (dict): The row dictionary containing all required parameters.
        snowflake_credentials (SnowflakeCredentials): Snowflake credentials for connection.

    Returns:
        str: The rendered SQL query string.
    """
    if final_row_dict["test_case_type"] == TestCaseType.DATA_TESTING:
        with SnowparkConnector(
            snowflake_credentials=snowflake_credentials
        ) as sq:
            if (
                final_row_dict.get("target_database_name")
                and final_row_dict.get("target_schema_name")
                and final_row_dict.get("target_table_name")
            ):
                df = sq.session.table(
                    f"{final_row_dict['target_database_name']}.{final_row_dict['target_schema_name']}.{final_row_dict['target_table_name']}"
                )
            else:
                df = sq.session.table(
                    f"{final_row_dict['source_database_name']}.{final_row_dict['source_schema_name']}.{final_row_dict['source_table_name']}"
                )

            columns = [field.name for field in df.schema.fields]

            if "carrier_name" in columns:
                final_row_dict["carrier_name_condition"] = (
                    f"carrier_name = '{final_row_dict['carrier_name']}'"
                )
            elif "carrier_name_dim_id" in columns:
                final_row_dict["carrier_name_condition"] = (
                    f"carrier_name_dim_id = MD5('{final_row_dict['carrier_name']}')"
                )
            else:
                final_row_dict["carrier_name_condition"] = "1=1"

    final_row_dict = create_column_conditions_for_final_rendered_query(
        final_row_dict
    )

    template = final_row_dict.get("mapped_sql_query", "")

    def replacer(match):
        key = match.group(1)
        return str(final_row_dict.get(key, match.group(0)))

    filled_query = re.sub(r"\{([A-Za-z0-9_]+)\}", replacer, template)

    return filled_query


def build_and_map_sql_query_to_row(
    row_dict: dict,
    sql_template: dict,
    snowflake_credentials: SnowflakeCredentials,
) -> dict:
    """
    Maps the SQL query from sql_template to the row_dict based on test_case_name.
    Updates row_dict['mapped_sql_query'] and then builds the final SQL query.

    Args:
        row_dict (dict): The row dictionary.
        sql_template (dict): Dictionary mapping test case names to SQL queries.
        snowflake_credentials (SnowflakeCredentials): Snowflake credentials.

    Returns:
        dict: The updated row_dict with mapped and final SQL queries.
    """
    test_case_name = row_dict.get("test_case_name")
    if not test_case_name:
        raise ValueError("test_case_name is required in row_dict")

    sql_query = sql_template.get(test_case_name)
    if not sql_query:
        raise ValueError(
            f"No SQL template found for test_case_name: {test_case_name}"
        )

    row_dict["mapped_sql_query"] = sql_query
    row_dict["final_rendered_sql_query"] = build_final_rendered_sql_query(
        row_dict, snowflake_credentials
    )

    return row_dict


def map_query_and_validate_test_case_data(
    df: pd.DataFrame,
    source_database_name: str,
    target_database_name: str,
    snowflake_credentials: SnowflakeCredentials,
) -> list[dict]:
    """
    For each row in the DataFrame, map the corresponding SQL query from the SQL template file
    (using test_case_name as the key), add it to the row as 'mapped_sql_query', and validate
    the row using the TestCaseMetadata pydantic schema.

    Args:
        df (pd.DataFrame): DataFrame containing test case data.
        source_database_name (str): Source database name.
        target_database_name (str): Target database name.
        snowflake_credentials (SnowflakeCredentials): Snowflake credentials.

    Returns:
        List[Dict]: List of validated test case dictionaries.

    Raises:
        SQLFileNotFoundException: If the SQL file path is missing.
        SQLTemplateNotFoundException: If no SQL templates are found.
        TestCaseValidationException: If validation fails.
    """
    validated_rows: list[dict] = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()

        sql_file_path = row_dict.get("query_file_path")
        if not sql_file_path:
            raise SQLFileNotFoundException(
                f"query_file_path is missing for row: {row_dict.get('test_case_name')}"
            )

        sql_templates = parse_sql_file(sql_file_path)
        if not sql_templates:
            raise SQLTemplateNotFoundException(
                f"No SQL templates found in file: {sql_file_path}"
            )

        # Set database names
        row_dict["source_database_name"] = source_database_name
        row_dict["target_database_name"] = target_database_name

        # Strip whitespace from keys and values
        row_dict = {
            k.strip(): (v.strip() if isinstance(v, str) else v)
            for k, v in row_dict.items()
        }

        final_row_dict = build_and_map_sql_query_to_row(
            row_dict, sql_templates, snowflake_credentials
        )

        # Parse set_params
        set_params_str = final_row_dict.get("set_params")
        if set_params_str:
            try:
                final_row_dict["set_params"] = parse_set_params(set_params_str)
            except Exception:
                final_row_dict["set_params"] = {}

        try:
            validated_params = TestCaseMetadata(**final_row_dict)
            validated_rows.append(validated_params.model_dump())
        except (KeyError, ValidationError) as e:
            raise TestCaseValidationException(
                f"Validation failed for row with test_case_name '{row_dict.get('test_case_name')}': {str(e)}"
            )
        except Exception as e:
            raise TestCaseValidationException(
                f"Error processing row with test_case_name '{row_dict.get('test_case_name')}': {str(e)}"
            )

    return validated_rows


def format_column(col: str) -> str:
    """
    Format a column name for SQL queries.

    Args:
        col (str): Column name.

    Returns:
        str: Formatted column name.
    """
    if '"' in col:
        return f'"{col}"'
    return col


def create_column_conditions_for_final_rendered_query(
    row_dict: dict,
) -> dict:
    """
    Enriches the input dictionary with SQL condition strings based on the specified test case type.
    This function inspects the 'test_case' field in the input dictionary and generates corresponding
    SQL condition strings for column validation. The generated conditions are added to the dictionary
    under specific keys, depending on the test case type. The function modifies the input dictionary
    in place and returns it.

    Supported test cases and their effects:
    - "NOT_NULL_CHECK": Adds a 'not_null_condition' key with a SQL condition to check for NULL values in
    - "DEFAULT_VALUE_CHECK": Adds a 'default_value_condition' key with a SQL condition to check for default values.
    - "NEGATIVE_VALUE_CHECK": Adds a 'negative_value_condition' key with a SQL condition to check for negative values.
    - "ZERO_VALUE_CHECK": Adds a 'zero_value_condition' key with a SQL condition to check for zero values.

    Args:
        row_dict (dict): A dictionary containing at least the keys 'test_case' and 'column_name'.
            May also include 'default_values' for the "DEFAULT_VALUE_CHECK" test case.

    Returns:
        dict: The input dictionary, enriched with additional SQL condition fields as appropriate.

    Raises:
        TestCaseValidationException: If a numeric check is requested but no numeric columns are found.
    """
    # Prefer source_column_name, but if missing or empty, use target_column_name for all logic
    source_columns_values = row_dict.get("source_column_name", "").strip()
    target_columns_values = row_dict.get("target_column_name", "").strip()

    if source_columns_values:
        columns_values = source_columns_values
        columns = [col.strip() for col in source_columns_values.split(",")]
        row_dict[
            "source_column_name"
        ] = source_columns_values  # keep as comma-separated string for SQL
    elif target_columns_values:
        columns_values = target_columns_values
        columns = [col.strip() for col in target_columns_values.split(",")]
        row_dict[
            "target_column_name"
        ] = target_columns_values  # keep as comma-separated string for SQL
    else:
        columns_values = ""
        columns = []

    # For backward compatibility
    source_columns = (
        [col.strip() for col in source_columns_values.split(",")]
        if source_columns_values
        else []
    )
    target_columns = (
        [col.strip() for col in target_columns_values.split(",")]
        if target_columns_values
        else []
    )

    test_case_upper = row_dict.get("test_case_name", "").upper()

    if test_case_upper == "UNIQUE_CHECK" or test_case_upper == "COMBINATION_COLUMN_UNIQUE_CHECK":
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join(
                [format_column(col.strip()) for col in columns]
            )
    elif test_case_upper == "ORPHAN_CHECK":
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join(
                [format_column(col.strip()) for col in columns]
            )
    elif test_case_upper == "NOT_NULL_CHECK":
        if columns:
            not_null_conditions = [
                f"{format_column(col)} IS NULL" for col in columns
            ]
            row_dict["not_null_condition"] = " OR ".join(not_null_conditions)
        else:
            row_dict["not_null_condition"] = ""
    elif test_case_upper == "DEFAULT_VALUE_CHECK":
        default_values = row_dict.get("default_values", "")
        if default_values and columns:
            default_value_conditions = []
            default_list = [
                val.strip() for val in str(default_values).split(",")
            ]
            for i, col in enumerate(columns):
                default_val = (
                    default_list[i] if i < len(default_list) else default_list[0]
                )
                default_value_conditions.append(
                    f"{format_column(col)} = '{default_val}'"
                )
            row_dict["default_value_condition"] = " OR ".join(
                default_value_conditions
            )
        else:
            row_dict["default_value_condition"] = ""
    elif test_case_upper == "NEGATIVE_VALUE_CHECK":
        numeric_columns = [
            col for col in columns if col not in ["carrier_name", "carrier_name_dim_id"]
        ]
        if not numeric_columns:
            raise TestCaseValidationException(
                f"NEGATIVE_VALUE_CHECK requires at least one numeric column. Got: {columns}"
            )
        negative_value_condition = " AND ".join(
            [f"{format_column(col)} < 0" for col in numeric_columns]
        )
        row_dict["negative_value_condition"] = negative_value_condition
    elif test_case_upper == "ZERO_VALUE_CHECK":
        numeric_columns = [
            col for col in columns if col not in ["carrier_name", "carrier_name_dim_id"]
        ]
        if not numeric_columns:
            raise TestCaseValidationException(
                f"ZERO_VALUE_CHECK requires at least one numeric column. Got: {columns}"
            )
        zero_value_condition = " AND ".join(
            [f"{format_column(col)} = 0" for col in numeric_columns]
        )
        row_dict["zero_value_condition"] = zero_value_condition

    return row_dict


def log_validated_row(
    validated_row: dict[str, Any], snowflake_credentials: SnowflakeCredentials
) -> tuple[str, str, str, int] | None:
    """
    Logs the test result in JSON format with table, columns, and test_results.
    The entire JSON is enclosed in a table-like ASCII box.
    Returns a tuple: (table_name, test_case_name, status, failure_count) or None if an error occurs.

    Args:
        validated_row (dict[str, Any]): The validated test case row.
        snowflake_credentials (SnowflakeCredentials): Snowflake credentials.

    Returns:
        tuple[str, str, str, int] | None: Tuple containing table_name, test_case_name, status, and failure_count.
    """
    result_json: dict[str, Any] = {"test_results": []}
    block_sep = "\n" + "=" * 100

    # Determine table name
    if validated_row["test_case_type"] == TestCaseType.DATA_TESTING:
        if (
            validated_row.get("target_database_name")
            and validated_row.get("target_schema_name")
            and validated_row.get("target_table_name")
        ):
            table_name = f"{validated_row['target_database_name']}.{validated_row['target_schema_name']}.{validated_row['target_table_name']}"
        elif (
            validated_row.get("source_database_name")
            and validated_row.get("source_schema_name")
            and validated_row.get("source_table_name")
        ):
            table_name = f"{validated_row['source_database_name']}.{validated_row['source_schema_name']}.{validated_row['source_table_name']}"
        else:
            table_name = validated_row.get("fact_table_name", "UNKNOWN")
    else:
        table_name = validated_row.get("fact_table_name", "UNKNOWN")

    # Extract columns
    source_column_str = validated_row.get("source_column_name", "").strip()
    target_column_str = validated_row.get("target_column_name", "").strip()

    if source_column_str:
        columns = [col.strip() for col in source_column_str.split(",")]
    elif target_column_str:
        columns = [col.strip() for col in target_column_str.split(",")]
    else:
        columns = []

    result_json["table_name"] = table_name
    result_json["columns"] = columns
    result_json["TEST_RESULTS"] = []

    # Execute query
    try:
        query = validated_row.get("final_rendered_sql_query")
        if not query:
            raise Exception("final_rendered_sql_query is missing")

        with SnowparkConnector(
            snowflake_credentials=snowflake_credentials
        ) as sq:
            if validated_row.get("is_set"):
                set_statements = create_set_statements(
                    validated_row.get("is_set"),
                    validated_row.get("set_params"),
                )
                if set_statements:
                    all_statements = set_statements + [query]
                    result_df = sq.execute_multiple_statements(
                        all_statements, lazy=True
                    )
                else:
                    result_df = sq.execute_query(query, lazy=True)
            else:
                result_df = sq.execute_query(query, lazy=True)

            if result_df is not None and hasattr(result_df, "collect"):
                result_df = result_df.collect()
            else:
                raise Exception("Invalid result from query execution")

    except Exception as e:
        test_result = {
            "TEST_CASE_NAME": validated_row.get("test_case_name", "UNKNOWN"),
            "STATUS": "ERROR",
            "FAILURE_COUNT": None,
            "DETAILS": str(e),
        }
        result_json["TEST_RESULTS"].append(test_result)

        json_str = json.dumps(result_json, indent=2, default=str)
        box_width = max(len(line) for line in json_str.split("\n")) + 4

        table_box = f"""
{"=" * box_width}
{json_str}
{"=" * box_width}
"""
        logger.info(table_box)
        return None

    # Determine test result based on test case type and query result
    failure_count = 0
    details: list[dict] = []
    status = "PASS"

    if result_df:
        dict_list = [row.as_dict() if hasattr(row, "as_dict") else dict(row) for row in result_df]
        df = pd.DataFrame(dict_list)

        if not df.empty:
            failure_count = (
                df.iloc[0].get("err_count", len(df))
                if "err_count" in df.columns
                else len(df)
            )
            status = "FAIL" if failure_count > 0 else "PASS"
            details = df.head(10).to_dict("records")
        else:
            failure_count = 0
            status = "PASS"
            details = []
    else:
        failure_count = 0
        status = "PASS"
        details = []

    test_result = {
        "TEST_CASE_NAME": validated_row.get("test_case_name", "UNKNOWN"),
        "STATUS": status,
        "FAILURE_COUNT": failure_count,
        "DETAILS": details,
    }

    result_json["TEST_RESULTS"].append(test_result)

    json_str = json.dumps(result_json, indent=2, default=str)
    box_width = max(len(line) for line in json_str.split("\n")) + 4

    table_box = f"""
{"=" * box_width}
{json_str}
{"=" * box_width}
"""
    logger.info(table_box)

    return (table_name, validated_row.get("test_case_name", "UNKNOWN"), status, failure_count)


def analyze_and_log_results(result_list: list) -> None:
    """
    Analyze the result list, log each result, and raise exception if any test case failed.

    Args:
        result_list (list): List of test results.

    Raises:
        NoTestResultsException: If no test results are provided.
        OneOrMoreTestCasesFailedException: If one or more test cases failed.
    """
    if not result_list:
        raise NoTestResultsException("No test results to analyze.")

    failed_rows = [
        row
        for row in result_list
        if row is not None and len(row) >= 3 and str(row[2]).upper() == "FAIL"
    ]

    if not failed_rows:
        return

    # Build table for failed tests
    headers = ["TABLE_NAME", "TEST_CASE_NAME", "STATUS", "COUNT"]
    col_widths = [len(h) for h in headers]

    for row in failed_rows:
        for i, val in enumerate(row[:4]):
            if i < len(col_widths):
                col_widths[i] = max(col_widths[i], len(str(val)))

    sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
    header_row = (
        "|"
        + "|".join(
            [
                f" {headers[i]:<{col_widths[i]}} "
                for i in range(len(headers))
            ]
        )
        + "|"
    )

    table_lines = [sep, header_row, sep]

    for row in failed_rows:
        row_line = (
            "|"
            + "|".join(
                [
                    f" {str(row[i] if i < len(row) else ''):<{col_widths[i]}} "
                    for i in range(len(headers))
                ]
            )
            + "|"
        )
        table_lines.append(row_line)

    table_lines.append(sep)
    table_str = "\n".join(table_lines)

    logger.info("\nFailed Test Cases Summary:\n" + table_str)

    raise OneOrMoreTestCasesFailedException(
        "One or more test cases failed. See logs for details."
    )
