import argparse
import re
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any
import yaml
from snowflake.snowpark.session import DataFrame, Session
from datamart_analytics.custom_exceptions.snowflake_exceptions import (
    SnowflakeQueryException,
    SnowflakeTableException,
)
from datamart_analytics.environment import environment_configuration
from datamart_analytics.logger import logger
from datamart_analytics.models.custom_models import (
    DatamartTable,
    DatamartTable_integrated,
    SnowflakeCredentials,
)
from datamart_analytics.models.logging_models import ProcessLog

# datamart_utils.py
# Utility functions for working with Snowflake/Snowpark in a SQL-analogous way.
# Example usage:
## Equivalent to: CREATE TABLE ...
# create_table_from_ddl(session, ddl_sql)
## Equivalent to: INSERT INTO ... SELECT ...
# stage_table(session, df, 'MY_DB.MY_SCHEMA.MY_TABLE')
## Equivalent to: MERGE INTO ...
# merge_sql = generate_merge_sql(...)
# execute_merge(session, merge_sql, ...)

# Table name regex pattern
TABLE_NAME_REGEX = re.compile(
    r"TABLE\s+[\[\\\"]?([a-zA-Z0-9_\-{}.]+)[\]\\\"]?", re.IGNORECASE
)


## Equivalent to: Template variable mapping for SQL scripts
def get_substitutions(datamart_table: DatamartTable_integrated) -> dict:
    """
    Builds a dictionary of template variable substitutions for SQL scripts.

    Args:
        datamart_table (DatamartTable_integrated): Table metadata/config.

    Returns:
        dict: Mapping of template variables to values.
    """
    return {
        "{{TARGET_DATABASE}}": getattr(datamart_table, "target_database", None),
        "{{TARGET_SCHEMA}}": getattr(datamart_table, "target_schema", None),
        "{{CARRIER_NAME}}": getattr(datamart_table, "carrier_name", None),
        "$carrier_name": getattr(datamart_table, "carrier_name", None),
        "$CARRIER_NAME": getattr(datamart_table, "carrier_name", None),
        "{{carrier_name}}": getattr(datamart_table, "carrier_name", None),
        "{{SOURCE_DATABASE}}": getattr(datamart_table, "source_database", None),
        "source_database": getattr(datamart_table, "source_database", None),
        "{{WAREHOUSE}}": getattr(datamart_table, "warehouse", None),
        "{{warehouse}}": getattr(datamart_table, "warehouse", None),
        "{{REFRESH_TYPE}}": getattr(datamart_table, "refresh_type", None),
        "{{FOLDER_NAME}}": getattr(datamart_table, "folder_name", None),
    }


## Equivalent to: CREATE TABLE ...
def create_table_from_ddl(
    session: Session,
    ddl_sql: str,
    substitutions: dict[str, Any] | None = None,
) -> None:
    """
    Executes a CREATE TABLE DDL statement in Snowflake.
    If substitutions are provided, replaces template variables in the DDL before execution.

    Args:
        session (Session): Snowflake session/connection.
        ddl_sql (str): The DDL statement.
        substitutions (dict[str, Any] | None): Optional dictionary for template replacements.

    Raises:
        Exception: If the DDL execution fails.
    """
    if substitutions:
        for key, value in substitutions.items():
            if value is not None:
                ddl_sql = ddl_sql.replace(key, str(value))

    try:
        session.sql(ddl_sql).collect()
        logger.info("Table created successfully from DDL.")
    except Exception as e:
        logger.error(f"Error creating table from DDL: {e}")
        raise SnowflakeTableException(f"Failed to create table from DDL: {e}")


## Equivalent to: INSERT INTO... SELECT... (for staging/transient tables)
def stage_table(
    session: Session,
    df: DataFrame,
    full_table_name: str,
    table_type: str = "transient",
) -> None:
    """
    Saves a DataFrame as a staging (transient) table in Snowflake.

    Args:
        session (Session): Snowflake session/connection.
        df (DataFrame): Data to write.
        full_table_name (str): Target table name (DB.SCHEMA.TABLE).
        table_type (str): Table type, e.g., 'transient'.

    Raises:
        Exception: If the write fails.
    """
    try:
        logger.info(f"Staging data to {full_table_name} as {table_type} table")
        df.write.save_as_table(
            name=full_table_name, mode="overwrite", table_type=table_type
        )
        logger.info(f"Data staged successfully to {full_table_name}")
    except Exception as e:
        logger.error(f"Error staging table {full_table_name}: {e}")
        raise SnowflakeTableException(f"Failed to stage table {full_table_name}: {e}")


## Equivalent to: MERGE INTO ...
def generate_merge_sql(
    target_db: str,
    target_schema: str,
    target_table: str,
    stage_db: str,
    stage_schema: str,
    stage_table: str,
    columns: list[str],
    pk_col: list[str],
) -> str:
    """
    Generates a MERGE SQL statement for upserting data from a staging table into a target table.

    Args:
        target_db (str): Target database.
        target_schema (str): Target schema.
        target_table (str): Target table.
        stage_db (str): Staging database.
        stage_schema (str): Staging schema.
        stage_table (str): Staging table.
        columns (list[str]): List of columns.
        pk_col (list[str]): Primary key column for matching.

    Returns:
        str: The MERGE SQL statement.
    """
    logger.debug(f"Generating MERGE SQL for columns: {columns}")
    logger.debug(f"Primary key columns: {pk_col}")

    if not columns or not pk_col:
        raise ValueError("Both columns and pk_col must be provided.")

    # Build ON condition
    on_conditions = [f"tgt.{col} = src.{col}" for col in pk_col]
    on_condition = " AND ".join(on_conditions)

    # Build UPDATE SET clause (exclude primary keys)
    update_columns = [col for col in columns if col not in pk_col]
    update_set_clause = ", ".join([f"{col} = src.{col}" for col in update_columns])

    # Build INSERT clause
    insert_columns = ", ".join(columns)
    insert_values = ", ".join([f"src.{col}" for col in columns])

    merge_sql = f"""
    MERGE INTO {target_db}.{target_schema}.{target_table} tgt
    USING {stage_db}.{stage_schema}.{stage_table} src
    ON {on_condition}
    WHEN MATCHED THEN UPDATE SET {update_set_clause}
    WHEN NOT MATCHED THEN INSERT ({insert_columns}) VALUES ({insert_values})
    """

    return merge_sql.strip()


def execute_merge(
    session: Session,
    merge_sql: str,
    start_time: datetime,
    db_name: str,
    schema: str,
    table_name: str,
) -> ProcessLog:
    """
    Executes a MERGE SQL statement in Snowflake and returns a process log.

    Args:
        session (Session): Snowflake session/connection.
        merge_sql (str): The MERGE SQL statement.
        start_time (datetime): Start time for logging.
        db_name (str): Database name.
        schema (str): Schema name.
        table_name (str): Table name.

    Returns:
        ProcessLog: Log of the merge operation.

    Raises:
        Exception: If the merge fails.
    """
    try:
        session.sql(merge_sql).collect()
        result = session.sql(
            "SELECT * FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))"
        ).collect()

        rows_inserted = 0
        rows_updated = 0

        for row in result:
            row_dict = row.as_dict()
            rows_inserted += row_dict.get("rows_inserted", 0)
            rows_updated += row_dict.get("rows_updated", 0)

        end_time = datetime.now()
        process_log = ProcessLog(
            id=str(uuid.uuid4()),
            sql_script="NA",
            database_name=db_name,
            table_schema=schema,
            table_name=table_name,
            inserted_row_count=rows_inserted,
            updated_row_count=rows_updated,
            query_duration=str(end_time - start_time),
            execution_timestamp=end_time.isoformat(),
            general_error_message=None,
            status="SUCCESS",
        )

        logger.info(
            f"Merge completed; {rows_inserted} inserted, {rows_updated} updated."
        )
        return process_log
    except Exception as e:
        logger.error(f"Error executing merge: {e}")
        raise SnowflakeQueryException(f"Failed to execute merge: {e}")


## Equivalent to: INSERT INTO LOG TABLE ...
def log_process(
    session: Session,
    process_logs: list[ProcessLog],
    log_table: str = "DATAMART_EXECUTION_LOG_TABLE",
) -> None:
    """
    Inserts process logs into a Snowflake log table.

    Args:
        session (Session): Snowflake session/connection.
        process_logs (list[ProcessLog]): List of process logs.
        log_table (str): Log table name.
    """
    if not process_logs:
        logger.warning("No process logs to insert.")
        return

    log_data = [
        (
            log.id,
            log.database_name,
            log.table_schema,
            log.table_name,
            log.inserted_row_count,
            log.updated_row_count,
            log.query_duration,
            log.execution_timestamp,
            log.general_error_message,
            log.status,
        )
        for log in process_logs
    ]

    schema = [
        "ID",
        "DATABASE_NAME",
        "TABLE_SCHEMA",
        "TABLE_NAME",
        "INSERTED_ROW_COUNT",
        "UPDATED_ROW_COUNT",
        "QUERY_DURATION",
        "EXECUTION_TIMESTAMP",
        "GENERAL_ERROR_MESSAGE",
        "STATUS",
    ]

    df = session.create_dataframe(log_data, schema=schema)
    df.write.mode("append").save_as_table(name=log_table)

    logger.info(f"Inserted {len(process_logs)} process logs into {log_table}.")


def extract_table_name_from_ddl(ddl_sql: str) -> str:
    """
    Extracts the table name from a CREATE TABLE DDL statement.

    Args:
        ddl_sql (str): The CREATE TABLE DDL statement.

    Returns:
        str: The extracted table name.

    Raises:
        ValueError: If the table name cannot be extracted.
    """
    pattern = re.compile(
        r"CREATE\s+(?:OR\s+REPLACE\s+)?(?:TRANSIENT\s+|TEMPORARY\s+|TEMP\s+)?TABLE\s+([\w]+)\.([\w]+)\.([\w]+)",
        re.IGNORECASE,
    )
    match = pattern.search(ddl_sql)

    if not match:
        raise ValueError("Could not extract table name from DDL.")

    return match.group(3)


# --- Utility: Case-insensitive template substitution ---
## Equivalent to: Case-insensitive variable replacement in SQL
def replace_template_vars_case_insensitive(sql: str, substitutions: dict) -> str:
    """
    Replaces all template variables in the SQL string in a case-insensitive way.

    Args:
        sql (str): SQL string with template variables.
        substitutions (dict): Mapping of template variables to values.

    Returns:
        str: SQL string with variables replaced.
    """
    for key, value in substitutions.items():
        if value is not None:
            pattern = re.compile(re.escape(key), re.IGNORECASE)
            sql = pattern.sub(str(value), sql)
    return sql


## Equivalent to: Read and preprocess CREATE TABLE ...
def read_and_substitute_ddl(
    ddl_path: Path, datamart_table: DatamartTable_integrated
) -> str:
    """
    Reads a DDL file and substitutes template variables before execution.

    Args:
        ddl_path (Path): Path to the DDL file.
        datamart_table (DatamartTable_integrated): Table metadata/config.

    Returns:
        str: DDL SQL with variables replaced.
    """
    with open(ddl_path) as ddl_file:
        ddl_sql = ddl_file.read()

    substitutions = get_substitutions(datamart_table)
    final_sql = replace_template_vars_case_insensitive(ddl_sql, substitutions)

    # Print/log the final SQL for debugging (only for fact table DDLs)
    if "fact" in str(ddl_path).lower():
        logger.info(f"Final SQL for fact table from ({ddl_path}):\n{final_sql}")

    return final_sql


def load_profile_yaml(folder: str) -> dict:
    """
    Load a YAML configuration file and return its contents as a dictionary.

    Parameters:
        folder (str): The folder name where the YAML file is located.

    Returns:
        dict: The contents of the YAML file as a dictionary.

    Raises:
        FileNotFoundError: If the YAML file does not exist.
        yaml.YAMLError: If there is an error parsing the YAML file.
    """
    try:
        # Search for the 'configuration' folder anywhere in the project
        project_root = Path(__file__).parent.parent
        configuration_folder = next(
            project_root.rglob("configuration"), None
        )

        if not configuration_folder:
            raise FileNotFoundError(
                "Configuration folder not found in the project."
            )

        yaml_file = configuration_folder / f"{folder}.yaml"

        if not yaml_file.exists():
            raise FileNotFoundError(f"YAML file not found: {yaml_file}")

        with open(yaml_file) as file:
            return yaml.safe_load(file)
    except yaml.YAMLError as e:
        raise RuntimeError(f"Error parsing YAML file: {e}")


def extract_and_validate_table_name(
    file_path: str, datamart_table: DatamartTable_integrated
) -> str | None:
    """
    Extracts the table name from the SQL file and validates it against runtime database and schema if the table name is not parameterized.

    Args:
        file_path (str): Path to SQL file.
        datamart_table (DatamartTable_integrated): Object with runtime database/schema parameters.

    Returns:
        Optional[str]: Extracted table name if valid, otherwise None.
    """
    try:
        with open(file_path, "r") as file:
            for line in file:
                match = TABLE_NAME_REGEX.search(line)
                if match:
                    full_table_name = match.group(1)

                    # Check if table name is parameterized
                    if "{{" in full_table_name:
                        # Extract the last part after the last dot
                        parts = full_table_name.split(".")
                        if parts:
                            return parts[-1]

                    # Validate fully qualified name
                    parts = full_table_name.split(".")

                    if len(parts) == 3:
                        # Fully qualified name: database.schema.table
                        database_name, schema_name, table_name = parts
                        if database_name != datamart_table.target_database:
                            logger.error(
                                f"Database name '{database_name}' in the SQL file does not match the runtime database '{datamart_table.target_database}'"
                            )
                            return None
                        if schema_name != datamart_table.target_schema:
                            logger.error(
                                f"Schema name '{schema_name}' in the SQL file does not match the runtime schema '{datamart_table.target_schema}'"
                            )
                            return None
                        logger.info(f"Extracted table name: {table_name}")
                        return table_name
                    elif len(parts) == 2:
                        # Partially qualified name: schema.table
                        schema_name, table_name = parts
                        if schema_name != datamart_table.target_schema:
                            logger.error(
                                f"Schema name '{schema_name}' in the SQL file does not match the runtime schema '{datamart_table.target_schema}'"
                            )
                            return None
                        logger.info(f"Extracted table name: {table_name}")
                        return table_name
                    else:
                        # Only table name is provided
                        table_name = parts[0]
                        logger.info(f"Extracted table name: {table_name}")
                        return table_name

        # If no match is found, log an error
        logger.error(f"Table name not found in file: {file_path}")
        return None
    except Exception as e:
        logger.error(f"Error extracting table name from file {file_path}: {e}")
        raise


def check_table_exists(session: Session, table_name: str) -> bool:
    """
    Check if a table exists in Snowflake.

    Args:
        session (Session): Snowflake session/connection.
        table_name (str): The name of the table to check.

    Returns:
        bool: True if the table exists, False otherwise.
    """
    try:
        result = session.sql(f"SHOW TABLES LIKE '{table_name}'").collect()
        return len(result) > 0
    except Exception as e:
        logger.error(f"Error checking if table '{table_name}' exists: {e}")
        return False


# Credential creation utilities for DatamartTable reports
def create_target_credentials(datamart_table: DatamartTable) -> SnowflakeCredentials:
    """
    Create SnowflakeCredentials for target database from environment configuration and datamart table.

    This function centralizes the logic for creating Snowflake credentials, ensuring consistency
    across all report scripts. This is especially important when managing 100+ similar reports.

    Args:
        datamart_table (DatamartTable): Configuration for the datamart table including
            target database, schema, and warehouse information.

    Returns:
        SnowflakeCredentials: Configured credentials object for connecting to Snowflake target database.

    Example:
        >>> from datamart_analytics.tools.datamart_utils import create_target_credentials
        >>> credentials = create_target_credentials(datamart_table)
        >>> from datamart_analytics.connector.snowpark_connector import SnowparkConnector
        >>> with SnowparkConnector(credentials) as connector:
        ...     # Use connector
    """
    return SnowflakeCredentials(
        user=environment_configuration.snowflake_user_target,
        password=environment_configuration.snowflake_password_target,
        account=environment_configuration.snowflake_account,
        warehouse=datamart_table.target_warehouse,
        database=datamart_table.target_database,
        table_schema=datamart_table.target_schema,
        role=environment_configuration.snowflake_role_target,
        authenticator=environment_configuration.snowflake_authenticator,
        private_key_file=environment_configuration.snowflake_private_key_file,
        private_key_password=environment_configuration.snowflake_private_key_password,
    )


def create_source_credentials(datamart_table: DatamartTable) -> SnowflakeCredentials:
    """
    Create SnowflakeCredentials for source database from environment configuration and datamart table.

    This function creates credentials for accessing the source database, which may be different
    from the target database credentials.

    Args:
        datamart_table (DatamartTable): Configuration for the datamart table including
            source database, schema, and warehouse information.

    Returns:
        SnowflakeCredentials: Configured credentials object for connecting to Snowflake source database.

    Example:
        >>> from datamart_analytics.tools.datamart_utils import create_source_credentials
        >>> credentials = create_source_credentials(datamart_table)
        >>> from datamart_analytics.connector.snowpark_connector import SnowparkConnector
        >>> with SnowparkConnector(credentials) as connector:
        ...     # Use connector
    """
    return SnowflakeCredentials(
        user=environment_configuration.snowflake_user_source,
        password=environment_configuration.snowflake_password_source,
        account=environment_configuration.snowflake_account,
        warehouse=datamart_table.source_warehouse,
        database=datamart_table.source_database,
        table_schema=datamart_table.source_schema,
        role=environment_configuration.snowflake_role_source,
        authenticator=environment_configuration.snowflake_authenticator,
        private_key_file=environment_configuration.snowflake_private_key_file,
        private_key_password=environment_configuration.snowflake_private_key_password,
    )


# Argument parsing utilities for DatamartTable reports
def create_datamart_table_parser(report_name: str) -> argparse.ArgumentParser:
    """
    Create an ArgumentParser with standard DatamartTable arguments.

    This function centralizes the argument parsing logic for all datamart reports,
    ensuring consistency across 100+ similar reports.

    Args:
        report_name (str): Name of the report (used in parser description).

    Returns:
        argparse.ArgumentParser: Configured parser with all standard DatamartTable arguments.

    Example:
        >>> parser = create_datamart_table_parser("my_report")
        >>> args = parser.parse_args()
        >>> datamart_table = parse_args_to_datamart_table(args, "my_report")
    """
    parser = argparse.ArgumentParser(
        description=f"Run {report_name} report.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Required arguments
    parser.add_argument(
        "--source_database",
        type=str,
        required=True,
        help="Source database name",
    )
    parser.add_argument(
        "--source_schema",
        type=str,
        required=True,
        help="Source schema name",
    )
    parser.add_argument(
        "--target_database",
        type=str,
        required=True,
        help="Target database name",
    )
    parser.add_argument(
        "--target_schema",
        type=str,
        required=True,
        help="Target schema name",
    )
    parser.add_argument(
        "--carrier_name",
        type=str,
        required=True,
        help="Carrier name for the report",
    )
    parser.add_argument(
        "--target_warehouse",
        type=str,
        required=True,
        help="Target warehouse name",
    )

    # Optional arguments
    parser.add_argument(
        "--source_warehouse",
        type=str,
        default=None,
        help="Source warehouse name (optional)",
    )
    parser.add_argument(
        "--target_table",
        type=str,
        default=None,
        help="Target table name (optional - reports may create multiple tables)",
    )
    parser.add_argument(
        "--report_start_dt",
        type=str,
        default=None,
        help="Report start datetime (YYYY-MM-DD HH:MM:SS, optional)",
    )
    parser.add_argument(
        "--report_end_dt",
        type=str,
        default=None,
        help="Report end datetime (YYYY-MM-DD HH:MM:SS, optional)",
    )
    parser.add_argument(
        "--source_table",
        type=str,
        default=None,
        help="Source table name (optional)",
    )
    parser.add_argument(
        "--last_load_date",
        type=str,
        default=None,
        help="Last load date for incremental data extraction (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--as_of_run_dt",
        type=str,
        default=None,
        help="As of run datetime (YYYY-MM-DD HH:MM:SS)",
    )
    parser.add_argument(
        "--report_run_dt",
        type=str,
        default=None,
        help="Report run datetime (YYYY-MM-DD HH:MM:SS)",
    )

    return parser


def parse_args_to_datamart_table(args: argparse.Namespace, report_name: str) -> DatamartTable:
    """
    Parse command-line arguments into a DatamartTable object.

    This function converts parsed arguments into a DatamartTable instance,
    centralizing the object creation logic for all reports.

    Args:
        args (argparse.Namespace): Parsed command-line arguments.
        report_name (str): Name of the report (used as DatamartTable.name).

    Returns:
        DatamartTable: Configured DatamartTable instance from parsed arguments.

    Example:
        >>> parser = create_datamart_table_parser("my_report")
        >>> args = parser.parse_args()
        >>> datamart_table = parse_args_to_datamart_table(args, "my_report")
    """
    return DatamartTable(
        name=report_name,
        source_database=args.source_database,
        source_schema=args.source_schema,
        target_database=args.target_database,
        target_schema=args.target_schema,
        carrier_name=args.carrier_name,
        target_warehouse=args.target_warehouse,
        source_warehouse=getattr(args, "source_warehouse", None),
        source_table=getattr(args, "source_table", None),
        target_table=getattr(args, "target_table", None),
        report_start_dt=getattr(args, "report_start_dt", None),
        report_end_dt=getattr(args, "report_end_dt", None),
        last_load_date=getattr(args, "last_load_date", None),
        as_of_run_dt=getattr(args, "as_of_run_dt", None),
        report_run_dt=getattr(args, "report_run_dt", None),
    )


def create_and_parse_datamart_table_args(report_name: str) -> DatamartTable:
    """
    Convenience function that creates parser, parses arguments, and returns DatamartTable.

    This is a one-stop function for the most common use case: create parser, parse args,
    and get a DatamartTable object.

    Args:
        report_name (str): Name of the report.

    Returns:
        DatamartTable: Configured DatamartTable instance from parsed arguments.

    Example:
        >>> if __name__ == "__main__":
        ...     datamart_table = create_and_parse_datamart_table_args("my_report")
        ...     run_my_report(datamart_table)
    """
    parser = create_datamart_table_parser(report_name)
    args = parser.parse_args()
    return parse_args_to_datamart_table(args, report_name)
