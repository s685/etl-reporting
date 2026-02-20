import ast
import json
import re
from typing import Any

import numpy as np
import pandas as pd
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
from pydantic import ValidationError
from sqlalchemy import create_engine

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
    df = df[df["is_enabled"] == True] # type: ignore
    df = df.reset_index(drop=True)
    return df



def get_snowflake_credentials(
    database_warehouse: str,
    source_database_name: str,
    table_schema_name: str
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
    sql_server_host: str, source_database_name: str, sql_server_port: int
) -> SQLServerCredentials:
    """
    Returns a SQLServerCredentials object using environment configuration and provided parameters.
    Supports SSO (Windows Authentication) if user/password are not set.
    """
    if environment_configuration.sql_server_user is None and environment_configuration.sql_server_password is None:
        return SQLServerCredentials(
            server=sql_server_host,
            database=source_database_name,
            port=sql_server_port,
            user=None,
            password=None,
            driver=SQLServerODBCVersion.MS_ODBC_DRIVER_18.value
        )
    else:
        return SQLServerCredentials(
            server=sql_server_host,
            database=source_database_name,
            port=sql_server_port,
            user=environment_configuration.sql_server_user,
            password=environment_configuration.sql_server_password,
            driver=SQLServerODBCVersion.MS_ODBC_DRIVER_18.value
        )



def get_set_statements(is_set: bool, set_params: str) -> list[str] | None:
    """
    Get the list of SET statements if is_set is True, using key-value pairs from set_params.
    Args:
        is_set (str or bool): 'True' or 'False' (or bool).
        set_params (str or dict): String representation of dict, dict, or None.
    """
    # Normalize is_set to boolean
    if isinstance(is_set, str):
        is_set_bool = is_set.strip().lower() == "true"
    else:
        is_set_bool = bool(is_set)
    # Handle set_params as dict or str
    if not is_set_bool or set_params is None or (isinstance(set_params, str) and set_params.strip().lower() == "none"):
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

    Args:
        params_dict (dict): Dictionary of parameter names and values.

    Returns:
        list[str]: List of SQL SET statements.
    """
    return [f"SET {param} = '{value}';" for param, value in params_dict.items()]



def parse_sql_file(path: str) -> dict[str, str]:
    """
    Parses the SQL file and extracts the blocks of @NAME and @QUERY pairs.

    Args:
        path (str): Path to SQL file.

    Returns:
        Dict[str, str]: Returns the mapping of @NAME to the SQL query.
    """
    with open(path) as file:
        sql_text = file.read()

    test_case_pattern = r"-- START_TEST(.*?)-- END_TEST"
    test_case_blocks = re.findall(test_case_pattern, sql_text, re.DOTALL)

    name_query_mapping = {}

    for test_case_block in test_case_blocks:
        try:
            match = re.search(r"-- @NAME:\s*(.+)", test_case_block)
            if not match:
                raise TestCaseParseException(f"Missing @NAME in block:\n{test_case_block}")

            name = match.group(1).strip()
            query_lines = re.split(r"-- @QUERY:.*?\n", test_case_block, flags=re.DOTALL)[1]
            query = re.sub(r"--.*", "", query_lines).strip()
            name_query_mapping[name] = query
        except (AttributeError, ValidationError) as e:
            raise TestCaseParseException(
                f"Error parsing test case: {e}\nBlock:\n{test_case_block}\n"
            ) from e

    return name_query_mapping



def build_final_rendered_sql_query(final_row_dict: dict, snowflake_credentials: SnowflakeCredentials) -> str:
    """
    Builds the final SQL query by filling placeholders in 

    Returns:
        str: The rendered SQL query string.
    """
    if final_row_dict['test_case_type'] == TestCaseType.DATA_TESTING:
        with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
            if final_row_dict['target_database_name'] and final_row_dict['target_schema_name'] and final_row_dict['target_table_name']:
                df = sq.session.table(f"{final_row_dict['target_database_name']}.{final_row_dict['target_schema_name']}.{final_row_dict['target_table_name']}")
            else:
                df = sq.session.table(f"{final_row_dict['source_database_name']}.{final_row_dict['source_schema_name']}.{final_row_dict['source_table_name']}")
            columns = [field.name for field in df.schema.fields]
            if "carrier_name" in columns:
                final_row_dict['carrier_name_condition'] = f"carrier_name = '{final_row_dict['carrier_name'].strip()}'"
            elif "carrier_name_dim_id" in columns:
                final_row_dict['carrier_name_condition'] = f"carrier_name_dim_id = MD5('{final_row_dict['carrier_name'].strip()}')"
            else:
                final_row_dict['carrier_name_condition'] = "1=1"

        final_row_dict = create_column_conditions_for_final_rendered_query(final_row_dict)

    template = final_row_dict.get("mapped_sql_query", "")

    def replacer(match):
        key = match.group(1)
        return str(final_row_dict.get(key, match.group(0)))
    filled_query = re.sub(r"\{([A-Za-z0-9_]+)\}", replacer, template)

    return filled_query



def build_and_map_sql_query_to_row(row_dict: dict, sql_template: dict, snowflake_credentials: SnowflakeCredentials) -> dict:
    """
    Maps the SQL query from sql_template to the row_dict based on test_case_name.
    Updates row_dict['mapped_sql_query'] and Then build the final SQL query.
    """
    test_case_name = row_dict.get("test_case_name")
    if not test_case_name:
        raise ValueError("test_case_name is missing in row_dict")
    sql_query = sql_template.get(test_case_name)
    if not sql_query:
        raise ValueError(f"No SQL template found for test_case_name '{test_case_name}'")
    row_dict["mapped_sql_query"] = sql_query
    row_dict["final_rendered_sql_query"] = build_final_rendered_sql_query(row_dict, snowflake_credentials)

    return row_dict



def map_query_and_validate_test_case_data(
    df: pd.DataFrame,
    source_database_name: str,
    target_database_name: str,
    snowflake_credentials: SnowflakeCredentials
) -> list[dict]:
    """
    For each row in the DataFrame, map the corresponding SQL query from the SQL template file
    (using test_case_name as the key), add it to the row as 'mapped_sql_query', and validate
    the row using the TestCaseMetadata pydantic schema.

    Args:
        df (pd.DataFrame): DataFrame containing test case parameters.

    Returns:
        List[Dict]: A list of validated and processed test case rows, each as a dictionary.

    Raises:
        SQLFileNotFoundException: If the SQL template file path is missing.
        SQLTemplateNotFoundException: If no SQL templates are found in the file or for the test_case_name.
        TestCaseValidationException: If validation of a row fails.
    """
    try:
        validated_rows = []

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
            row_dict = {str(k).strip(): str(v).strip() for k, v in row_dict.items()}

            final_row_dict = build_and_map_sql_query_to_row(row_dict, sql_templates, snowflake_credentials)

            final_row_dict['set_params'] = parse_set_parameters(final_row_dict.get('set_params', ''))

            try:
                validated_params = TestCaseMetadata(**final_row_dict)
                validated_rows.append(validated_params.model_dump())

            except (KeyError, ValidationError) as e:
                raise TestCaseValidationException(
                    f"Validation failed for row {final_row_dict}: {e}"
                ) from e
    except Exception as e:
        raise TestCaseValidationException(f"Error processing rows: {e}") from e

    return validated_rows



def format_column(col):
    return f'"{col}"' if ' ' in col else col



def create_column_conditions_for_final_rendered_query(row_dict: dict) -> dict:
    """
    Enriches the input dictionary with SQL condition strings based on the specified test case type.

    This function inspects the 'test_case' field in the input dictionary and generates corresponding SQL condition
    strings for column validation. The generated conditions are added to the dictionary under specific keys, depending
    on the test case type. The function modifies the input dictionary in place and returns it.

    Supported test cases and their effects:
        - "NOT_NULL_CHECK": Adds a 'not_null_condition' key with a SQL condition to check for NULL values in the specified columns.
        - "DEFAULT_VALUE_CHECK": Adds a 'default_value_condition' key with a SQL condition to check for default values or NULLs.
        - "NEGATIVE_VALUE_CHECK": Adds a 'negative_value_condition' key with a SQL condition to check for negative values in numeric columns.
        - "ZERO_VALUE_CHECK": Adds a 'zero_value_condition' key with a SQL condition to check for zero values in numeric columns.

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
        row_dict["source_column_name"] = source_columns_values  # keep as comma-separated string for SQL
    elif target_columns_values:
        columns_values = target_columns_values
        columns = [col.strip() for col in target_columns_values.split(",")]
        row_dict["target_column_name"] = target_columns_values  # keep as comma-separated string for SQL
    else:
        columns_values = ""
        columns = []

    # For backward compatibility
    source_columns = [col.strip() for col in source_columns_values.split(",")] if source_columns_values else []
    target_columns = [col.strip() for col in target_columns_values.split(",")] if target_columns_values else []

    test_case_upper = row_dict.get("test_case_name", "").upper()

    if test_case_upper == "UNIQUE_CHECK" or test_case_upper == "COMBINATION_COLUMN_UNIQUE_CHECK":
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join([format_column(col.strip()) for col in columns])

    elif test_case_upper == "ORPHAN_CHECK":
        if len(columns) == 1:
            row_dict["source_column_name"] = format_column(columns[0])
            if target_columns:
                row_dict["target_column_name"] = format_column(target_columns[0])
        elif len(columns) > 1:
            row_dict["source_column_name"] = ",".join([format_column(col.strip()) for col in columns])
            if target_columns:
                row_dict["target_column_name"] = ",".join([format_column(col.strip()) for col in target_columns])

    elif test_case_upper == "NOT_NULL_CHECK":
        if len(columns) == 1:
            col = format_column(columns[0])
            not_null_condition = f"{col} IS NULL"
        elif len(columns) > 1:
            not_null_condition = " AND ".join(
                [f"{format_column(col)} IS NULL" for col in columns]
            )
        else:
            not_null_condition = "1=1"  # fallback if no columns
        row_dict["not_null_condition"] = not_null_condition

    elif test_case_upper == "DEFAULT_VALUE_CHECK":
        # Expect default_values as "POLICY_ID=0,PAYEE ID='0000'"
        default_values_str = row_dict.get("default_values", "")
        conditions = []
        if default_values_str:
            for pair in default_values_str.split(","):
                if "=" in pair:
                    col, val = pair.split("=", 1)
                    col = format_column(col.strip())  # <-- handles spaces
                    val = val.strip()
                    # Add quotes if value looks like a string and isn't already quoted
                    if not (val.startswith("'") and val.endswith("'")) and not val.replace('.', '', 1).isdigit():
                        val = f"'{val}'"
                    conditions.append(f"{col} = {val}")
        if conditions:
            default_value_condition = " AND ".join(conditions)
        else:
            # fallback: check for NULL as default
            default_value_condition = " AND ".join([f"{format_column(col)} IS NULL" for col in columns]) if columns else "1=1"
        row_dict["default_value_condition"] = default_value_condition

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
    validated_row: dict[str, Any], snowflake_credentials: SnowflakeCredentials
) -> tuple[str, str, str, int] | None:
    """
    Logs the test result in JSON format with table, columns, and test_results.
    The entire JSON is enclosed in a table-like ASCII box.
    Returns a tuple: (table_name, test_case_name, status, failure_count) or None if an error occurs
    """

    result_json: dict[str, Any] = {
        "test_results": []
    }

    # Section header for test case block
    block_sep = "\n" + "=" * 100

    if validated_row['test_case_type'] == TestCaseType.DATA_TESTING:
        if validated_row['source_database_name'] and validated_row['source_schema_name'] and validated_row['source_table_name']:
            table_name = f"{validated_row['source_database_name']}.{validated_row['source_schema_name']}.{validated_row['source_table_name']}"
        elif validated_row['target_database_name'] and validated_row['target_schema_name'] and validated_row['target_table_name']:
            table_name = f"{validated_row['target_database_name']}.{validated_row['target_schema_name']}.{validated_row['target_table_name']}"
        else:
            table_name = f"{validated_row['fact_table_name']}"

        source_column_str = validated_row.get('source_column_name', '').strip()
        target_column_str = validated_row.get('target_column_name', '').strip()
        if source_column_str:
            columns = [col.strip() for col in source_column_str.split(",")]
        elif target_column_str:
            columns = [col.strip() for col in target_column_str.split(",")]
        else:
            columns = []
        result_json = {
            "TABLE_NAME": table_name,
            "COLUMNS": columns,
            "TEST_RESULTS": []
        }
    else:
        if validated_row['fact_table_name']:
            table_name = f"{validated_row['fact_table_name']}"

        result_json = {
            "TABLE_NAME": table_name,
            "TEST_RESULTS": []
        }

    try:
        query = validated_row.get('final_rendered_sql_query')
        if not query:
            raise Exception("No rendered SQL query found in validated_row.")

        with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
            if validated_row['is_set']:
                set_statements = get_set_statements(validated_row['is_set'], validated_row['set_params'])
                all_statements = (set_statements or []) + [query]
                result_df = sq.execute_multiple_statements(all_statements, lazy=True)
            else:
                result_df = sq.execute_query(query, lazy=True)

            if result_df is not None and hasattr(result_df, "collect"):
                result_df = result_df.collect()
            else:
                raise Exception("Query execution did not return a valid result.")
    except Exception as e:
        test_result = {
            "TEST_CASE_NAME": validated_row['test_case_name'],
            "STATUS": "ERROR",
            "FAILURE_COUNT": None,
            "DETAILS": str(e)
        }
        result_json["TEST_RESULTS"].append(test_result)
        json_str = json.dumps(result_json, indent=2)
        box_width = max(len(line) for line in json_str.splitlines()) + 4
        table_box = (
            "\n"
            "+" + "-" * (box_width - 2) + "+\n"
            + "\n".join([f"| {line.ljust(box_width - 3)}|" for line in json_str.splitlines()])
            + "\n" + "+" + "-" * (box_width - 2) + "+"
            + f"{block_sep}\n\n\n"
        )
        logger.info(table_box)
        return None

    # Determine test result based on test case type and query result
    failure_count = 0
    details = []
    status = "PASS"

    if result_df:
        dict_list = [row.as_dict() for row in result_df]
        df = pd.DataFrame(dict_list)
        if not df.empty:
            failure_count = int(df.iloc[0].get("err_count", len(df)))
            status = "FAIL" if failure_count > 0 else "PASS"
            details = (df.head(10)).to_dict(orient="records")
        else:
            failure_count = 0
            status = "PASS"
            details = []
    else:
        failure_count = 0
        status = "PASS"
        details = []

    test_result = {
        "TEST_CASE_NAME": validated_row['test_case_name'],
        "STATUS": status,
        "FAILURE_COUNT": failure_count,
        "DETAILS": details
    }
    result_json["TEST_RESULTS"].append(test_result)

    json_str = json.dumps(result_json, indent=2, default=str)
    box_width = max(len(line) for line in json_str.splitlines()) + 4
    table_box = (
        "\n"
        + "+" + "-" * (box_width - 2) + "+\n"
        + "\n".join([f"| {line.ljust(box_width - 3)}|" for line in json_str.splitlines()])
        + "\n" + "+" + "-" * (box_width - 2) + "+"
        + f"{block_sep}\n\n\n"
    )
    logger.info(table_box)

    return (table_name, validated_row['test_case_name'], status, failure_count)



def analyze_and_log_results(result_list):
    """
    Analyze the result list, log each result, and raise exception if any test case failed.
    """
    if not result_list:
        raise NoTestResultsException("No test results to analyze.")

    failed_rows = [row for row in result_list if row is not None and len(row) >= 3 and str(row[2]).upper() == 'FAIL']
    if not failed_rows:
        return ""

    headers = ["TABLE_NAME", "TEST_CASE_NAME", "STATUS", "COUNT"]
    col_widths = [len(h) for h in headers]
    for row in failed_rows:
        for i in range(4):
            col_widths[i] = max(col_widths[i], len(str(row[i])))

    sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
    header_row = "| " + " | ".join([headers[i].ljust(col_widths[i]) for i in range(4)]) + " |"
    table_lines = [sep, header_row, sep]
    for row in failed_rows:
        row_line = "| " + " | ".join([str(row[i]).ljust(col_widths[i]) for i in range(4)]) + " |"
        table_lines.append(row_line)
    table_lines.append(sep)

    table_str = "\n".join(table_lines)
    logger.info("\nFailed Test Cases Summary:\n" + table_str)
    raise OneOrMoreTestCasesFailedException("One or more test cases failed. See logs for details.")



def extract_sql_to_snowflake_test_case_queries(
    sql_to_snowflake_query_file_path: str
) -> list[dict[str, str | None]]:
    with open(sql_to_snowflake_query_file_path) as f:
        content = f.read()

    test_blocks = re.findall(r'-- START_TEST(.*?)-- END_TEST', content, re.DOTALL)
    results = []
    for block in test_blocks:
        test_name = re.search(r'-- @TEST_NAME:\s*(.*)', block)
        sql_server_query = re.search(r'-- @START_SQL_SERVER_QUERY:(.*?)-- @END_SQL_SERVER_QUERY', block, re.DOTALL)
        snowflake_query = re.search(r'-- @START_SNOWFLAKE_QUERY:(.*?)-- @END_SNOWFLAKE_QUERY', block, re.DOTALL)
        results.append({
            'TEST_NAME': test_name.group(1).strip() if test_name else None,
            'SQL_SERVER_QUERY': sql_server_query.group(1).strip() if sql_server_query else None,
            'SNOWFLAKE_QUERY': snowflake_query.group(1).strip() if snowflake_query else None,
        })
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
    driver = credentials.driver.value if hasattr(credentials.driver, 'value') else credentials.driver
    if credentials.user and credentials.password:
        connection_url = (
            f"mssql+pyodbc://{credentials.user}:{credentials.password}@{credentials.server},{credentials.port}/{credentials.database}?driver={driver}&TrustServerCertificate=yes"
        )
    else:
        connection_url = (
            f"mssql+pyodbc://@{credentials.server},{credentials.port}/{credentials.database}?driver={driver}&TrustServerCertificate=yes"
        )
    engine = create_engine(connection_url)
    with engine.connect() as conn:
        sql_server_result_df = pd.read_sql(query, conn)
    return sql_server_result_df



def compare_sql_server_and_snowflake_result_df(
    sql_server_result_df: pd.DataFrame, snowflake_result_df: pd.DataFrame
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Compares two DataFrames after normalizing column names to uppercase and aligning columns.
    Returns (rows in SQL Server not in Snowflake, rows in Snowflake not in SQL Server).
    Raises Exception if columns do not match after normalization.
    """
    # Normalize column names (strip and uppercase)
    sql_server_result_df = sql_server_result_df.copy()
    snowflake_result_df = snowflake_result_df.copy()
    if snowflake_result_df.empty and len(snowflake_result_df.columns) == 0:
        # collect() on an empty Snowflake result can yield an empty DataFrame with no columns.
        # Use SQL Server columns as expected shape so mismatch is reported as row diffs, not schema error.
        snowflake_result_df = pd.DataFrame(columns=sql_server_result_df.columns)
    sql_server_result_df.columns = [col.strip().upper() for col in sql_server_result_df.columns]
    snowflake_result_df.columns = [col.strip().upper() for col in snowflake_result_df.columns]

    if set(sql_server_result_df.columns) != set(snowflake_result_df.columns):
        raise Exception(f"Column mismatch after normalization. SQL Server: {list(sql_server_result_df.columns)}, Snowflake: {list(snowflake_result_df.columns)}")

    # Align column order
    snowflake_result_df = snowflake_result_df.reindex(columns=list(sql_server_result_df.columns))

    # Treat SQL NULL and pandas nan/None as equivalent by replacing with a common string
    sql_server_result_df = sql_server_result_df.replace({np.nan: "NULL", None: "NULL"})
    snowflake_result_df = snowflake_result_df.replace({np.nan: "NULL", None: "NULL"})

    # Strip whitespace from string columns and ensure types match
    for col in sql_server_result_df.columns:
        if pd.api.types.is_object_dtype(sql_server_result_df[col]):
            sql_server_result_df[col] = sql_server_result_df[col].astype(str).str.strip()
            snowflake_result_df[col] = snowflake_result_df[col].astype(str).str.strip()
        elif pd.api.types.is_float_dtype(sql_server_result_df[col]) or pd.api.types.is_float_dtype(snowflake_result_df[col]):
            # Round floats to 6 decimals for comparison
            sql_server_result_df[col] = pd.to_numeric(sql_server_result_df[col], errors='coerce').round(6)
            snowflake_result_df[col] = pd.to_numeric(snowflake_result_df[col], errors='coerce').round(6)
        elif pd.api.types.is_integer_dtype(sql_server_result_df[col]) or pd.api.types.is_integer_dtype(snowflake_result_df[col]):
            sql_server_result_df[col] = pd.to_numeric(sql_server_result_df[col], errors='coerce')
            snowflake_result_df[col] = pd.to_numeric(snowflake_result_df[col], errors='coerce')

    # Sort both DataFrames by all columns
    sql_server_result_df = sql_server_result_df.sort_values(by=list(sql_server_result_df.columns)).reset_index(drop=True)
    snowflake_result_df = snowflake_result_df.sort_values(by=list(snowflake_result_df.columns)).reset_index(drop=True)

    # Compare as sets of tuples
    sql_tuples = set(map(tuple, sql_server_result_df.values))
    snowflake_tuples = set(map(tuple, snowflake_result_df.values))
    df_sql_minus_snowflake = pd.DataFrame(
        list(sql_tuples - snowflake_tuples),
        columns=sql_server_result_df.columns
    )
    df_snowflake_minus_sql = pd.DataFrame(
        list(snowflake_tuples - sql_tuples),
        columns=sql_server_result_df.columns
    )
    return df_sql_minus_snowflake, df_snowflake_minus_sql



def log_sql_server_vs_snowflake_comparison(
    test_case: dict[str, Any],
    df_sql_minus_snowflake: pd.DataFrame
) -> None:
    """
    Logs a summary and up to 10 rows of df_sql_minus_snowflake in a boxed format for a given test_case dict.
    """
    test_case_result = "PASS" if len(df_sql_minus_snowflake) == 0 else "FAIL"
    # Prepare small ASCII table for df_sql_minus_snowflake (show up to 10 rows)
    if not df_sql_minus_snowflake.empty:
        preview_df = df_sql_minus_snowflake.head(10)
        headers = list(preview_df.columns)
        col_widths = [max(len(str(h)), *(len(str(x)) for x in preview_df[h])) for h in headers]
        sep = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
        header_row = "| " + " | ".join([headers[i].ljust(col_widths[i]) for i in range(len(headers))]) + " |"
        table_lines = [sep, header_row, sep]
        for _, row in preview_df.iterrows():
            row_line = "| " + " | ".join([str(row[h]).ljust(col_widths[i]) for i, h in enumerate(headers)]) + " |"
            table_lines.append(row_line)
        table_lines.append(sep)
        table_str = "\n".join(table_lines)
    else:
        table_str = "(No unmatched rows)"

    # Combine summary and table, then wrap in a box
    summary_lines = [
        f"TEST_CASE_NAME: {test_case['TEST_NAME']}",
        f"SOURCE_DATABASE_NAME: {test_case['source_database_name']}",
        f"TARGET_DATABASE_NAME: {test_case['target_database_name']}",
        f"RESULT: {test_case_result}",
        f"FAILURE_COUNT: {len(df_sql_minus_snowflake)}",
        "ROWS IN SQL SERVER RESULT NOT IN SNOWFLAKE RESULT:",
    ]
    # Split table_str into lines for box calculation
    table_lines = table_str.splitlines()
    all_lines = summary_lines + table_lines
    max_width = max(len(line) for line in all_lines)
    border = "+" + "-" * (max_width + 2) + "+"
    box = [border]
    for line in all_lines:
        box.append(f"| {line.ljust(max_width)} |")
    box.append(border)
    logger.info("\n" + "\n".join(box))



def compare_source_sql_to_target_snowflake_data(
    sql_server_credentials: SQLServerCredentials,
    snowflake_credentials: SnowflakeCredentials,
    source_database_name: str,
    target_database_name: str,
    sql_to_snowflake_query_file_path: str
) -> None:
    """
    Compares data between SQL Server and Snowflake based on the provided query file.
    """
    try:
        sql_to_snowflake_test_cases = extract_sql_to_snowflake_test_case_queries(sql_to_snowflake_query_file_path)
        if not sql_to_snowflake_test_cases:
            raise SQLTemplateNotFoundException(
                f"No SQL templates found in file: {sql_to_snowflake_query_file_path}"
            )

        failed_tests: list[dict[str, Any]] = []
        for test_case in sql_to_snowflake_test_cases:
            test_case['test_case_type'] = TestCaseType.SQLSERVER_TO_SNOWFLAKE_DATA_TESTING
            test_case['source_database_name'] = source_database_name
            test_case['target_database_name'] = target_database_name
            test_case['sql_to_snowflake_query_file_path'] = sql_to_snowflake_query_file_path
            test_case['final_rendered_sql_server_query'] = get_sql_to_snowflake_final_rendered_query(test_case.get('SQL_SERVER_QUERY') or "", test_case)
            test_case['final_rendered_snowflake_query'] = get_sql_to_snowflake_final_rendered_query(test_case.get('SNOWFLAKE_QUERY') or "", test_case)

            # Execute SQL Server query
            if not test_case['final_rendered_sql_server_query']:
                raise Exception("No rendered SQL Server query found in test_case.")
            sql_server_result_df = execute_sql_server_query(sql_server_credentials, test_case['final_rendered_sql_server_query'])

            # Execute Snowflake query
            if not test_case['final_rendered_snowflake_query']:
                raise Exception("No rendered Snowflake SQL query found in test_case.")
            with SnowparkConnector(snowflake_credentials=snowflake_credentials) as sq:
                snowflake_query_result = sq.execute_query(test_case['final_rendered_snowflake_query'], lazy=True)
                if snowflake_query_result is not None:
                    if hasattr(snowflake_query_result, "collect"):
                        snowflake_rows = snowflake_query_result.collect()
                        if snowflake_rows:
                            snowflake_result_df = pd.DataFrame([row.as_dict() for row in snowflake_rows])
                        else:
                            snowflake_columns = []
                            if hasattr(snowflake_query_result, "schema") and hasattr(snowflake_query_result.schema, "fields"):
                                snowflake_columns = [field.name for field in snowflake_query_result.schema.fields]
                            snowflake_result_df = pd.DataFrame(columns=snowflake_columns)
                    elif isinstance(snowflake_query_result, list):
                        if snowflake_query_result:
                            snowflake_result_df = pd.DataFrame([row.as_dict() for row in snowflake_query_result])
                        else:
                            snowflake_result_df = pd.DataFrame()
                    else:
                        raise Exception("Unexpected result type from SnowparkConnector.execute_query.")
                else:
                    raise Exception("Query execution did not return a valid result.")

            df_sql_minus_snowflake, df_snowflake_minus_sql = compare_sql_server_and_snowflake_result_df(
                sql_server_result_df, snowflake_result_df
            )
            log_sql_server_vs_snowflake_comparison(test_case, df_sql_minus_snowflake)

            mismatch_count = len(df_sql_minus_snowflake) + len(df_snowflake_minus_sql)
            if mismatch_count > 0:
                failed_tests.append({
                    "test_name": test_case.get("TEST_NAME", "UNKNOWN"),
                    "source_database": test_case.get("source_database_name"),
                    "target_database": test_case.get("target_database_name"),
                    "mismatch_count": mismatch_count,
                })

        if failed_tests:
            fail_summary = "; ".join(
                f"{t['test_name']} ({t['mismatch_count']} diff)" for t in failed_tests
            )
            raise CompareSQLToSnowflakeException(
                f"One or more SQL-to-Snowflake test cases failed: {fail_summary}"
            )
    except CompareSQLToSnowflakeException:
        raise
    except Exception as e:
        raise CompareSQLToSnowflakeException(f"Error in comparing SQL query and snowflake: {e}") from e