import re
import uuid
from pathlib import Path
from tracemalloc import Traceback
from snowflake.snowpark import DataFrame, QueryHistory, Row, Session, Table
from datamart_analytics.connector.base_snowpark import BaseSnowparkConnector
from datamart_analytics.custom_exceptions.snowflake_exceptions import (
    SnowflakeCredentialException,
    SnowflakeQueryException,
    SnowflakeSessionException,
    SnowflakeTableException,
    SnowflakeUpsertException,
)
from datamart_analytics.definitions.custom_definitions import (
    ApplicationEnvironment,
    ExecutionStatus,
    SnowflakeAuthenticatorType,
    SnowparkTableType,
    SnowparkTableWriteMode,
    UpsertResultStatus,
)
from datamart_analytics.environment import environment_configuration
from datamart_analytics.logger import d_logger, logger
from datamart_analytics.models.custom_models import (
    DatamartTable,
    DatamartTable_integrated,
    SnowflakeCredentials,
    UpsertResult,
)
from datamart_analytics.operations.obfuscation_operations import (
    load_snowflake_private_key,
)


class SnowparkConnector(BaseSnowparkConnector):
    """
    A class to represent a Snowpark query.
    """

    def __enter__(self) -> "SnowparkConnector":
        """
        Enter the runtime context related to this object.

        Returns:
            SnowparkConnector: The current instance of SnowparkConnector.
        """
        if (
            environment_configuration.datamart_analytics_framework_environment
            != ApplicationEnvironment.TEST.value
        ):
            if all(
                self.snowflake_credentials.password is None,
                self.snowflake_credentials.authenticator is None,
            ):
                raise SnowflakeCredentialException(
                    "Either password or authenticator must be provided in Snowflake credentials."
                )
            # Create the Snowpark session
            self.session = Session.builder.configs(
                options=self._get_connection_options()
            ).create()
        else:
            logger.info(
                "Running in test environment. No real Snowpark session will be created."
            )
        return self

    def __exit__(
        self,
        _exc_type: type | None,
        _exc_value: Exception | None,
        _traceback: Traceback | None,
    ) -> None:
        """
        Exit the runtime context related to this object.

        Params:
            exc_type: The exception type.
            exc_value: The exception value.
            traceback: The traceback object.
        """
        if self.session is not None:
            self.session.close()
            self.session = None

    def __init__(
        self,
        snowflake_credentials: SnowflakeCredentials,
    ) -> None:
        """
        Initialize the SnowparkConnector with Snowflake credentials and datamart table configuration.

        Params:
            snowflake_credentials (SnowflakeCredentials): Credentials for connecting to Snowflake.
        """
        
        self.snowflake_credentials: SnowflakeCredentials = snowflake_credentials
        self.session: Session | None = None
        

    def _get_connection_options(self) -> dict[str, int | str | bytes]:
        """
        Get the connection options for Snowflake.

        Returns:
            dict: A dictionary containing the connection options.

        Raises:
            Exception: If there is an error in the connection options.
        """
        if self.snowflake_credentials is None:
            raise SnowflakeCredentialException("Snowflake credentials are not provided.")

        options: dict[str, int | str | bytes] = {
            "account": self.snowflake_credentials.account,
            "user": self.snowflake_credentials.user,
            "warehouse": self.snowflake_credentials.warehouse,
            "database": self.snowflake_credentials.database,
            "schema": self.snowflake_credentials.table_schema,
            "role": self.snowflake_credentials.role,
        }

        if self.snowflake_credentials.password is not None:
            options.update(
                {
                    "password": self.snowflake_credentials.password,
                }
            )

        if self.snowflake_credentials.authenticator is not None:
            if (
                self.snowflake_credentials.authenticator
                == SnowflakeAuthenticatorType.EXTERNALBROWSER
            ):
                options.update(
                    {
                        "authenticator": self.snowflake_credentials.authenticator,
                    }
                )
            elif (
                self.snowflake_credentials.authenticator
                == SnowflakeAuthenticatorType.SNOWFLAKE_JWT
            ):
                private_key: bytes | None = None
                if self.snowflake_credentials.private_key_file is None:
                    raise SnowflakeCredentialException(
                        "Private key file must be provided for JWT authentication."
                    )
                if self.snowflake_credentials.private_key_password is None:
                    raise SnowflakeCredentialException(
                        "Private key password must be provided for JWT authentication."
                    )
                private_key = load_snowflake_private_key(
                    snowflake_secret_key_file=self.snowflake_credentials.private_key_file,
                    snowflake_private_key_password=self.snowflake_credentials.private_key_password,
                )
                if private_key is None:
                    raise SnowflakeCredentialException(
                        "Private key must be provided for JWT authentication."
                    )
                options.update(
                    {
                        "authenticator": self.snowflake_credentials.authenticator,
                        "private_key": private_key,
                    }
                )

        return options

    def set_session_variable(self, variable_name: str, value: str) -> None:
        """
        Set a Snowflake session variable that can be used in SQL with $variable_name.
        
        Args:
            variable_name: Name of the variable (without $ prefix)
            value: Value to set
            
        Example:
            >>> connector.set_session_variable("CARRIER_NAME", "ACME Corp")
            >>> # SQL can now use: SELECT * FROM table WHERE carrier = $CARRIER_NAME
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )
        
        try:
            set_sql = f"SET {variable_name} = '{value}'"
            self.session.sql(set_sql).collect()
            logger.info(f"Set session variable: {variable_name} = {value}")
        except Exception as e:
            logger.error(f"Error setting session variable {variable_name}: {e}")
            raise SnowflakeQueryException(f"Failed to set session variable {variable_name}: {e}")
    
    def set_session_variables_from_datamart_table(self, datamart_table: DatamartTable) -> None:
        """
        Set session variables from DatamartTable configuration.
        
        Sets variables that can be used in SQL with $ prefix:
        - $CARRIER_NAME
        - $REPORT_START_DT (if provided)
        - $REPORT_END_DT (if provided)
        - $REPORT_RUN_DT (if provided)
        - $AS_OF_RUN_DT (if provided)
        
        Args:
            datamart_table: DatamartTable configuration
            
        Example:
            >>> connector.set_session_variables_from_datamart_table(datamart_table)
            >>> # SQL: WHERE carrier = $CARRIER_NAME AND date >= $REPORT_START_DT
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )
        
        try:
            # Required variable
            self.set_session_variable("CARRIER_NAME", datamart_table.carrier_name)
            
            # Optional variables - DRY principle with dict mapping
            optional_vars = {
                "REPORT_START_DT": datamart_table.report_start_dt,
                "REPORT_END_DT": datamart_table.report_end_dt,
                "REPORT_RUN_DT": datamart_table.report_run_dt,
                "AS_OF_RUN_DT": datamart_table.as_of_run_dt,
            }
            
            for var_name, var_value in optional_vars.items():
                if var_value:
                    self.set_session_variable(var_name, var_value)
            
            logger.info("Successfully set all session variables from DatamartTable")
            
        except Exception as e:
            logger.error(f"Error setting session variables from DatamartTable: {e}")
            raise SnowflakeQueryException(f"Failed to set session variables: {e}")
    
    def get_table(self, table_name: str) -> Table | None:
        """
        Get a Snowflake table.

        Params:
            table_name (str): The name of the table to retrieve.

        Returns:
            Table | None: The Snowflake table object or None if the table does not exist.
        """
        table: Table | None = None
        try:
            if self.session is None:
                raise SnowflakeSessionException(
                    "Session is not initialized. Please use the context manager."
                )
            table = self.session.table(name=table_name)
        except Exception as e:
            logger.error(f"Error getting table {table_name}: {e}")
        return table

    def execute_query(
        self, query: str, lazy: bool = True
    ) -> list[Row] | DataFrame | None:
        """
        Execute a Snowflake query.

        Args:
            query (str): The SQL query to execute.
            lazy (bool): If True, return a DataFrame object. If False, return the result of the query.

        Returns:
            list[Row] | DataFrame: The result of the query.
        """
        result: list[Row] | DataFrame | None = None

        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        try:
            if lazy:
                result = self.session.sql(query)
            else:
                result = self.session.sql(query).collect()
        except Exception as e:
            logger.error(f"Error executing query: {e}")
        return result

    def execute_query_from_file(
        self,
        file_name: str,
        datamart_table: DatamartTable | DatamartTable_integrated,
        lazy: bool = True,
        folder_name: str | None = None,
        incremental_column: str | None = None,
        increment_records_from: str | None = None,
    ) -> list[Row] | DataFrame | None:
        """
        Execute a Snowflake query from a file.

        Args:
            file_name (str): The name of the SQL file to execute.
            datamart_table (DatamartTable): Configuration for the datamart table, including source and target.
            lazy (bool): If True, return a DataFrame object. If False, return the result of the query.
            folder_name (str | None): Optional folder name where the SQL file is located.
            incremental_column (str | None): Optional column for incremental loading.
            increment_records_from (str | None): Optional date from which to increment records.

        Returns:
            list[Row] | DataFrame: The result of the query.
            
        Note:
            - {{PLACEHOLDERS}} are replaced by Python for structural names (databases, schemas, tables)
            - $SESSION_VARIABLES are handled natively by Snowflake (set via set_session_variable())
        """
        result: list[Row] | DataFrame | None = None
        sql_base_path: Path = Path(__file__).parent.parent / "sql"
        query: str | None = None

        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        if file_name is None:
            raise SnowflakeQueryException("File name cannot be None")

        try:
            # Remove file extension if present
            if "." in file_name:
                file_name = file_name.split(".")[0]
            
            # Build path using pathlib
            if folder_name is not None:
                sql_file_path = sql_base_path / folder_name / f"{file_name}.sql"
            else:
                sql_file_path = sql_base_path / f"{file_name}.sql"

            with open(sql_file_path, "r") as file:
                query = file.read()

            if query is None:
                raise SnowflakeQueryException("Query could not be loaded.")

            # Replace placeholders in the query only if they exist
            if isinstance(datamart_table, DatamartTable_integrated):
                # Sources
                if "{{SOURCE_DATABASE}}" in query:
                    query = query.replace(
                        "{{SOURCE_DATABASE}}", datamart_table.source_database
                    )
                if "{{SOURCE_WAREHOUSE}}" in query:
                    query = query.replace(
                        "{{SOURCE_WAREHOUSE}}", datamart_table.warehouse
                    )
                if "{{TARGET_DATABASE}}" in query:
                    query = query.replace(
                        "{{TARGET_DATABASE}}", datamart_table.target_database
                    )
                if "{{TARGET_SCHEMA}}" in query:
                    query = query.replace(
                        "{{TARGET_SCHEMA}}", datamart_table.target_schema
                    )
                if "{{TARGET_WAREHOUSE}}" in query:
                    query = query.replace(
                        "{{TARGET_WAREHOUSE}}", datamart_table.warehouse
                    )
                if "{{CARRIER_NAME}}" in query:
                    query = query.replace(
                        "{{CARRIER_NAME}}", f"{datamart_table.carrier_name}"
                    )
                
                if "{{LAST_LOAD_DATE}}" in query:
                    if increment_records_from is None:
                        raise SnowflakeQueryException(
                            "increment_records_from must be provided for loading."
                        )
                    query = query.replace(
                        "{{LAST_LOAD_DATE}}", f"'{increment_records_from}'"
                    )

            if isinstance(datamart_table, DatamartTable):
                # Sources
                if "{{SOURCE_DATABASE}}" in query:
                    query = query.replace(
                        "{{SOURCE_DATABASE}}", datamart_table.source_database
                    )
                if "{{SOURCE_WAREHOUSE}}" in query:
                    query = query.replace(
                        "{{SOURCE_WAREHOUSE}}", datamart_table.source_warehouse
                    )
                if "{{SOURCE_SCHEMA}}" in query:
                    query = query.replace(
                        "{{SOURCE_SCHEMA}}", datamart_table.source_schema
                    )
                if datamart_table.source_table is not None:
                    if "{{SOURCE_TABLE}}" in query:
                        query = query.replace(
                            "{{SOURCE_TABLE}}", datamart_table.source_table
                        )

            # Targets
            if "{{TARGET_DATABASE}}" in query:
                query = query.replace(
                    "{{TARGET_DATABASE}}", datamart_table.target_database
                )
            if "{{TARGET_SCHEMA}}" in query:
                query = query.replace(
                    "{{TARGET_SCHEMA}}", datamart_table.target_schema
                )
            if "{{TARGET_WAREHOUSE}}" in query:
                query = query.replace(
                    "{{TARGET_WAREHOUSE}}", datamart_table.target_warehouse
                )
            if "{{TARGET_TABLE}}" in query:
                query = query.replace(
                    "{{TARGET_TABLE}}", datamart_table.target_table
                )
            if "{{CARRIER_NAME}}" in query:
                query = query.replace(
                    "{{CARRIER_NAME}}", f"{datamart_table.carrier_name}"
                )

            # Conditional logic for AND_CONDITION
            if "{{AND_CONDITION}}" in query:
                if incremental_column is not None:
                    if increment_records_from is None:
                        raise SnowflakeQueryException(
                            "increment_records_from must be provided when incremental_column is specified"
                        )
                    query = re.sub(
                        r"\{\{AND_CONDITION\}\}",
                        f" AND ({incremental_column}) > '{increment_records_from}'",
                        query,
                    )
                else:
                    query = re.sub(r"\{\{AND_CONDITION\}\}", "", query)

            # Conditional logic for WHERE_CONDITION
            if "{{WHERE_CONDITION}}" in query:
                if incremental_column is not None:
                    if increment_records_from is None:
                        raise SnowflakeQueryException(
                            "increment_records_from must be provided when incremental_column is specified"
                        )
                    query = re.sub(
                        r"\{\{WHERE_CONDITION\}\}",
                        f" WHERE ({incremental_column}) > '{increment_records_from}'",
                        query,
                    )
                else:
                    query = re.sub(r"\{\{WHERE_CONDITION\}\}", "", query)

            d_logger.debug(f"Final query: {query}")

            result = self.execute_query(query=query, lazy=lazy)

            d_logger.debug(
                f"Query executed successfully from file {file_name}. Result: {result}"
            )
        except Exception as e:
            logger.error(f"Error executing query from file: {e}")
        return result

    def save_as_table(
        self,
        dataframe: DataFrame,
        table_name: str,
        write_mode: SnowparkTableWriteMode = SnowparkTableWriteMode.OVERWRITE,
        table_type: SnowparkTableType = SnowparkTableType.PERMANENT,
    ) -> None:
        """
        Save a DataFrame as a Snowflake table.

        Params:
            dataframe (DataFrame): The DataFrame to save as a table.
            table_name (str): The name of the table to create or overwrite.
            write_mode (SnowparkTableWriteMode): The write mode for saving the table.
            table_type (SnowparkTableType): The type of the table (temporary, transient, or permanent).

        Raises:
            Exception: If the session is not initialized or if there is an error during saving.
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        try:
            if table_type == SnowparkTableType.PERMANENT:
                dataframe.write.mode(write_mode.value).save_as_table(
                    table_name=table_name,
                )
            else:
                dataframe.write.mode(write_mode.value).save_as_table(
                    table_name=table_name, table_type=table_type.value
                )
            d_logger.debug(
                f"DataFrame saved as table {table_name} with mode {write_mode.value} and type {table_type.value}"
            )
        except Exception as e:
            logger.error(
                f"Error saving DataFrame as table for table {table_name}. Error: {e}"
            )
            d_logger.error(
                f"Error saving DataFrame as table {table_name}: {e}", exc_info=True
            )
            raise SnowflakeTableException(f"Failed to save DataFrame as table {table_name}: {e}")
    
    def save_as_view(
        self,
        dataframe: DataFrame,
        view_name: str,
        replace: bool = True,
    ) -> None:
        """
        Save a DataFrame as a Snowflake view.
        
        Views are virtual tables that can be queried by other reports.
        They don't store data physically, making them efficient for reusable logic.

        Params:
            dataframe (DataFrame): The DataFrame to save as a view.
            view_name (str): The name of the view to create.
            replace (bool): If True, replace existing view. If False, fail if view exists.

        Raises:
            SnowflakeSessionException: If the session is not initialized.
            SnowflakeTableException: If view creation fails.
            
        Example:
            >>> df = connector.execute_query("SELECT * FROM table WHERE date > '2024-01-01'")
            >>> connector.save_as_view(df, "my_reusable_view")
            >>> # Other reports can now query this view:
            >>> # SELECT * FROM my_reusable_view WHERE some_condition
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        try:
            # Create view using CREATE OR REPLACE VIEW syntax
            replace_clause = "OR REPLACE " if replace else ""
            
            # Get the SQL query from the DataFrame
            queries = dataframe.queries
            if not queries:
                raise SnowflakeTableException(
                    f"Cannot create view '{view_name}': DataFrame has no query plan"
                )
            
            # Build CREATE VIEW statement
            view_sql = f"CREATE {replace_clause}VIEW {view_name} AS {queries['queries'][0]}"
            
            # Execute the CREATE VIEW statement
            self.session.sql(view_sql).collect()
            
            logger.info(f"Successfully created view: {view_name}")
            d_logger.debug(f"View '{view_name}' created with SQL: {view_sql}")
            
        except Exception as e:
            logger.error(f"Error creating view '{view_name}': {e}")
            d_logger.error(f"Error creating view '{view_name}': {e}", exc_info=True)
            raise SnowflakeTableException(f"Failed to create view '{view_name}': {e}")

    def get_query_history(
        self, id: str | None = None, limit: int = 10, lazy: bool = True
    ) -> DataFrame | None:
        """
        Get the query history.

        Params:
            id (str | None): The ID of the query to retrieve history for. If None, retrieves the most recent queries.
            limit (int): The maximum number of queries to return.
            lazy (bool): If True, return a DataFrame object. If False, return the result of the query.

        Returns:
            DataFrame | None: The query history or None in case of an error.
        """
        query_history: DataFrame | None = None
        try:
            query: str
            if id is not None:
                query = (
                    """
                    SELECT *
                    FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
                    WHERE QUERY_ID = '{}'
                    ORDER BY START_TIME DESC
                    LIMIT {}"""
                ).format(id, limit)
            else:
                query = (
                    """
                    SELECT *
                    FROM TABLE (SNOWFLAKE.INFORMATION_SCHEMA.QUERY_HISTORY())
                    ORDER BY START_TIME DESC
                    LIMIT {}"""
                ).format(limit)
            query_history = self.execute_query(query=query, lazy=lazy)
        except Exception as e:
            logger.error(f"Error getting query history: {e}")
        return query_history

    def upsert(
        self,
        target_table_name: str,
        join_keys: list[str],
        source_table_name: str | None = None,
        source_table_df: DataFrame | None = None,
        update_columns: list[str] | None = None,
        insert_columns: list[str] | None = None,
        delete_columns: list[str] | None = None,
        when_matched_condition: str | None = None,
        when_not_matched_condition: str | None = None,
        use_when_matching_condition: bool = True,
        use_when_not_matching_condition: bool = True,
    ) -> UpsertResult | None:
        """
        Perform upsert operation using a temporary table.
        Executes a MERGE statement to update existing records and insert new records.

        Params:
            target_table_name: Name of the target table
            join_keys: List of column names to join on
            source_table_name: Name of the source table (optional, if source_table_df is provided)
            source_table_df: DataFrame with source data (optional, if source_table_name is provided)
            update_columns: Columns to update (if None, updates all non-key columns)
            insert_columns: Columns to insert (if None, inserts all columns)
            delete_columns: Columns to delete (if None, no columns are deleted)
            when_matched_condition: Additional condition for WHEN MATCHED
            when_not_matched_condition: Additional condition for WHEN NOT MATCHED
            use_when_matching_condition: Whether to use the WHEN MATCHED condition.
            use_when_not_matching_condition: Whether to use the WHEN NOT MATCHED condition.

        Returns:
            Dictionary with operation results

        Raises:
            Exception: If the session is not initialized or if the target table does not exist.
            Exception: If there is an error during the upsert operation.
        """
        result: UpsertResult | None = None
        transient_table_name: str = (
            f"{target_table_name}_TRANSIENT_{str(uuid.uuid4()).replace('-', '')}"
        )
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )
        try:
            if not self.table_exists(target_table_name):
                raise SnowflakeTableException(f"Target table {target_table_name} does not exist.")
            if source_table_name is None:
                d_logger.debug(
                    f"Creating transient table {transient_table_name} from DataFrame"
                )
                self.save_as_table(
                    dataframe=source_table_df,
                    table_name=transient_table_name,
                    table_type=SnowparkTableType.TRANSIENT,
                )
                source_table_name = transient_table_name
            result = self._upsert_via_temp_table(
                source_table_name=(
                    source_table_name if source_table_name else transient_table_name
                ),
                target_table_name=target_table_name,
                join_keys=join_keys,
                update_columns=update_columns,
                insert_columns=insert_columns,
                delete_columns=delete_columns,
                when_matched_condition=when_matched_condition,
                when_not_matched_condition=when_not_matched_condition,
                use_when_matching_condition=use_when_matching_condition,
                use_when_not_matching_condition=use_when_not_matching_condition,
            )
        except Exception as e:
            logger.error(f"Upsert failed: {str(e)}", exc_info=True)
            raise e
        finally:
            if self.table_exists(transient_table_name):
                self.session.sql(
                    f"DROP TABLE IF EXISTS {transient_table_name}"
                ).collect()
        return result

    def upsert_from_query(
        self,
        source_query: str,
        target_table_name: str,
        join_keys: list[str],
        update_columns: list[str] | None = None,
        insert_columns: list[str] | None = None,
        temp_table_name: str | None = None,
    ) -> UpsertResult | None:
        """
        Perform upsert operation using SQL query as source.

        Returns:
            Dictionary with operation results
        """
        result: UpsertResult | None = None
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )
        try:
            # Create DataFrame from query
            source_table_df: DataFrame = self.session.sql(source_query)

            # If temp table specified, create it for better performance
            if temp_table_name:
                source_table_df.write.mode("overwrite").save_as_table(
                    temp_table_name, table_type="temporary"
                )
                source_table_df = self.session.table(temp_table_name)

            # Perform upsert using DataFrame method
            result = self.upsert(
                source_table_df=source_table_df,
                target_table_name=target_table_name,
                join_keys=join_keys,
                update_columns=update_columns,
                insert_columns=insert_columns,
            )

            # Clean up temp table if created
            if temp_table_name:
                self.session.sql(f"DROP TABLE IF EXISTS {temp_table_name}").collect()
        except Exception as e:
            logger.error(f"Query-based upsert failed: {str(e)}")
            return UpsertResult(
                status=UpsertResultStatus.FAIL,
                source_table=source_query,
                target_table=target_table_name,
            )
        return result

    def _upsert_via_temp_table(
        self,
        source_table_name: str,
        target_table_name: str,
        join_keys: list[str],
        update_columns: list[str] | None = None,
        insert_columns: list[str] | None = None,
        delete_columns: list[str] | None = None,
        when_matched_condition: str | None = None,
        when_not_matched_condition: str | None = None,
        use_when_matching_condition: bool = True,
        use_when_not_matching_condition: bool = True,
    ) -> UpsertResult | None:
        """
        Perform upsert via temporary table (cross-session or forced).

        Params:
            source_table_name: Name of the source table (can be a DataFrame or table name)
            target_table_name: Name of the target table
            join_keys: List of column names to join on
            update_columns: Columns to update (if None, updates all non-key columns)
            insert_columns: Columns to insert (if None, inserts all columns)
            delete_columns: Columns to delete (if None, no columns are deleted)
            when_matched_condition: Additional condition for WHEN MATCHED
            when_not_matched_condition: Additional condition for WHEN NOT MATCHED
            use_when_matching_condition: Whether to use the WHEN MATCHED condition
            use_when_not_matching_condition: Whether to use the WHEN NOT MATCHED condition

        Returns:
            Dictionary with operation results
        """
        result: UpsertResult | None = None
        # Now perform upsert using SQL with our session
        try:
            result = self._upsert_via_sql(
                source_table_name=source_table_name,
                target_table_name=target_table_name,
                join_keys=join_keys,
                update_columns=update_columns,
                insert_columns=insert_columns,
                delete_columns=delete_columns,
                when_matched_condition=when_matched_condition,
                when_not_matched_condition=when_not_matched_condition,
                use_when_matching_condition=use_when_matching_condition,
                use_when_not_matching_condition=use_when_not_matching_condition,
            )
        except Exception as e:
            logger.error(
                f"An error occurred during upsert for table {source_table_name}: {str(e)}"
            )
            d_logger.error(
                f"An error occurred during upsert for table {source_table_name}: {str(e)}",
                exc_info=True,
            )
        return result

    def _upsert_via_sql(
        self,
        source_table_name: str,
        target_table_name: str,
        join_keys: list[str],
        update_columns: list[str] | None = None,
        insert_columns: list[str] | None = None,
        delete_columns: list[str] | None = None,
        when_matched_condition: str | None = None,
        when_not_matched_condition: str | None = None,
        use_when_matching_condition: bool = True,
        use_when_not_matching_condition: bool = True,
    ) -> UpsertResult:
        """
        Perform upsert using pure SQL MERGE statement.

        Params:
            source_table_name: Source table name (can be a DataFrame or table name)
            target_table_name: Target table name (can include schema)
            join_keys: list of column names to join on
            update_columns: Columns to update (if None, updates all non-key columns)
            insert_columns: Columns to insert (if None, inserts all columns)
            delete_columns: Columns to delete (if None, no columns are deleted)
            when_matched_condition: Additional condition for WHEN MATCHED
            when_not_matched_condition: Additional condition for WHEN NOT MATCHED
            use_when_matching_condition: Whether to use the WHEN MATCHED condition
            use_when_not_matching_condition: Whether to use the WHEN NOT MATCHED condition

        Returns:
            UpsertResult: Result of the upsert operation
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        source_table: Table | DataFrame | None = None
        source_table = self.session.table(name=source_table_name)
        target_table: Table = self.session.table(name=target_table_name)

        source_columns = source_table.columns
        target_columns = target_table.columns

        if update_columns is None:
            update_columns = [
                col
                for col in source_columns
                if col not in join_keys and col in target_columns
            ]

        if insert_columns is None:
            insert_columns = [col for col in source_columns if col in target_columns]

        if delete_columns is None:
            delete_columns = [col for col in source_columns if col in target_columns]

        # Build SQL MERGE statement
        merge_sql: str = self._build_merge_sql(
            source_table_name=source_table_name,
            target_table_name=target_table_name,
            join_keys=join_keys,
            update_columns=update_columns,
            insert_columns=insert_columns,
            delete_columns=delete_columns,
            when_matched_condition=when_matched_condition,
            when_not_matched_condition=when_not_matched_condition,
            use_when_matching_condition=use_when_matching_condition,
            use_when_not_matching_condition=use_when_not_matching_condition,
        )

        d_logger.debug(f"Merge statement for table {target_table_name}: {merge_sql}")
        # Execute the merge
        merge_result: list[Row] = self.session.sql(merge_sql).collect()
        if (
            not merge_result
            or not isinstance(merge_result, list)
            and len(merge_result) == 0
        ):
            raise SnowflakeUpsertException(
                f"Merge operation did not return expected result for {target_table_name}"
            )

        upsert_result = UpsertResult(
            status=UpsertResultStatus.SUCCESS,
            source_table=source_table_name,
            target_table=target_table_name,
            join_keys=join_keys,
            update_columns=update_columns,
            insert_columns=insert_columns,
            delete_columns=delete_columns,
            when_matched_condition=when_matched_condition,
            when_not_matched_condition=when_not_matched_condition,
            use_when_matching_condition=use_when_matching_condition,
            use_when_not_matching_condition=use_when_not_matching_condition,
            records_inserted=(
                merge_result[0]["number of rows inserted"]
                if "number of rows inserted" in merge_result[0]
                else 0
            ),
            records_updated=(
                merge_result[0]["number of rows updated"]
                if "number of rows updated" in merge_result[0]
                else 0
            ),
            records_deleted=(
                merge_result[0]["number of rows deleted"]
                if "number of rows deleted" in merge_result[0]
                else 0
            ),
            method="sql_merge",
        )

        d_logger.info(
            f"Merge SQL executed successfully for table: {upsert_result.target_table} with: \n{upsert_result}"
        )
        logger.info(
            f"Merge SQL executed successfully for table: {upsert_result.target_table} with {upsert_result.records_inserted} inserted, {upsert_result.records_updated} updated, {upsert_result.records_deleted} deleted"
        )

        return upsert_result

    def _build_merge_sql(
        self,
        source_table_name: str,
        target_table_name: str,
        join_keys: list[str],
        update_columns: list[str],
        insert_columns: list[str],
        delete_columns: list[str] | None = None,
        when_matched_condition: str | None = None,
        when_not_matched_condition: str | None = None,
        use_when_matching_condition: bool = True,
        use_when_not_matching_condition: bool = True,
    ) -> str:
        """
        Build SQL MERGE statement.

        Params:
            source_table_name: Source table name. FQN encouraged.
            target_table_name: Target table name. FQN encouraged.
            join_keys: list of column names to join on
            update_columns: Columns to update (if None, updates all non-key columns)
            insert_columns: Columns to insert (if None, inserts all columns)
            delete_columns: Columns to delete (if None, no columns are deleted)
            when_matched_condition: Additional condition for WHEN MATCHED
            when_not_matched_condition: Additional condition for WHEN NOT MATCHED
            use_when_matching_condition: Whether to use the WHEN MATCHED condition
            use_when_not_matching_condition: Whether to use the WHEN NOT MATCHED condition

        Returns:
            SQL MERGE statement as a string
        """
        # Build join condition
        join_conditions: list[str] = []
        for key in join_keys:
            join_conditions.append(f"target.{key} = source.{key}")
        join_clause = " AND ".join(join_conditions)

        # Build UPDATE clause
        update_assignments: list[str] = []
        for col in update_columns:
            update_assignments.append(f"{col} = source.{col}")
        update_clause = ", ".join(update_assignments)

        # Build INSERT clause
        insert_cols = ", ".join(insert_columns)
        insert_values = ", ".join([f"source.{col}" for col in insert_columns])

        # Build complete MERGE statement
        merge_sql = f"""
MERGE INTO {target_table_name} AS target
USING {source_table_name} AS source
ON {join_clause}
"""

        if use_when_matching_condition:
            if update_columns is not None:
                when_matched_clause = f"WHEN MATCHED"
                if when_matched_condition is not None:
                    when_matched_clause += f" AND {when_matched_condition}"
                when_matched_clause += f" THEN UPDATE SET {update_clause}\n"
                merge_sql += when_matched_clause

        if use_when_not_matching_condition:
            if insert_columns is not None:
                when_not_matched_clause = f"WHEN NOT MATCHED"

                if when_not_matched_condition is not None:
                    when_not_matched_clause += f" AND (when_not_matched_condition)"

                when_not_matched_clause += (
                    f" THEN INSERT ({insert_cols}) VALUES ({insert_values})\n"
                )
                merge_sql += when_not_matched_clause

        if delete_columns:
            delete_conditions: list[str] = []
            for col in delete_columns:
                delete_conditions.append(f"source.{col} IS NULL")

            delete_clause = " OR ".join(delete_conditions)

            merge_sql += (
                f"WHEN NOT MATCHED BY SOURCE AND ({delete_clause})) THEN DELETE\n"
            )

        return merge_sql

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the Snowflake database.

        Args:
            table_name (str): The fully qualified name of the table (e.g., "database.schema.table").

        Returns:
            bool: True if the table exists, False otherwise.
        """
        try:
            self.session.table(name=table_name).collect()
            return True
        except Exception as e:
            return False

    def truncate_table(self, table_name: str) -> None:
        """
        Truncate a Snowflake table.

        Params:
            table_name (str): The fully qualified name of the table to truncate.

        Raises:
            Exception: If the session is not initialized or if the table does not exist.
            Exception: If there is an error during truncation.
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        try:
            if not self.table_exists(table_name):
                raise SnowflakeTableException(f"Table {table_name} does not exist.")
            self.session.sql(f"TRUNCATE TABLE {table_name}").collect()
            logger.info(f"Table {table_name} truncated successfully.")
        except Exception as e:
            logger.error(f"Error truncating table {table_name}: {e}")

    def drop_table(self, table_name: str) -> None:
        """
        Drop a Snowflake table.

        Params:
            table_name (str): The fully qualified name of the table to drop.

        Raises:
            Exception: If the session is not initialized or if the table does not exist.
            Exception: If there is an error during dropping.
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        if table_name is None:
            raise SnowflakeTableException("Table name cannot be None")

        try:
            if not self.table_exists(table_name):
                raise SnowflakeTableException(f"Table {table_name} does not exist.")
            self.session.sql(f"DROP TABLE IF EXISTS {table_name}").collect()
            logger.info(f"Table {table_name} dropped successfully.")
        except Exception as e:
            logger.error(f"Error dropping table {table_name}: {e}")

    def create_execution_table(self, datamart_table: DatamartTable) -> None:
        """Create the execution log table if it does not exist.

        Params:
            datamart_table (DatamartTable): The datamart table configuration, including target database and
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )
        if (
            datamart_table.target_database is None
            or datamart_table.target_schema is None
        ):
            raise ValueError("Target database and schema must be specified.")
        d_logger.debug(
            f"Creating execution table in {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
        )
        query: str = (
            f"""CREATE TABLE IF NOT EXISTS {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE (
            EXECUTION_ID STRING PRIMARY KEY,
            EXECUTION_START_TS STRING,
            EXECUTION_END_TS STRING NULL,
            EXECUTION_STATUS STRING,
            SOURCE_DATABASE STRING,
            SOURCE_SCHEMA STRING,
            SOURCE_WAREHOUSE STRING,
            TARGET_DATABASE STRING,
            TARGET_WAREHOUSE STRING,
            TARGET_SCHEMA STRING,
            TARGET_TABLE STRING,
            RECORDS_INSERTED INTEGER NULL,
            RECORDS_UPDATED INTEGER NULL,
            RECORDS_DELETED INTEGER NULL,
            CARRIER_NAME STRING,
            ERROR_MESSAGE STRING NULL
        )"""
        )
        result = self.execute_query(query, lazy=False)

        if result is None:
            raise SnowflakeTableException(
                f"Failed to create execution table {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
            )

        if "status" in result[0]:
            if result[0]["status"] not in [
                "Table DATAMART_EXECUTION_TABLE successfully created.",
                "DATAMART_EXECUTION_TABLE already exists, statement succeeded.",
            ]:
                d_logger.error(
                    f"Failed to create execution table {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
                )
                raise SnowflakeTableException(
                    f"Failed to create execution table {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
                )
        else:
            d_logger.error(
                f"Failed to create execution table {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
            )
            raise SnowflakeTableException(
                f"Failed to create execution table {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
            )

    def save_execution(
        self,
        datamart_table: DatamartTable,
        execution_start_ts: str,
        execution_end_ts: str,
        execution_status: ExecutionStatus,
        records_inserted: int = 0,
        records_updated: int = 0,
        records_deleted: int = 0,
        error_message: str | None = None,
    ) -> None:
        """
        Save the execution log to the database.

        Params:
            datamart_table (DatamartTable): The datamart table configuration, including target database and schema.
            execution_start_ts (str): The start timestamp of the execution.
            execution_end_ts (str): The end timestamp of the execution.
            execution_status (ExecutionStatus): The status of the execution.
            records_inserted (int): Number of records inserted during the execution.
            records_updated (int): Number of records updated during the execution.
            records_deleted (int): Number of records deleted during the execution.
            error_message (str | None): Error message if any error occurred during the execution.
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )
        if (
            datamart_table.target_database is None
            or datamart_table.target_schema is None
        ):
            raise ValueError("Target database and schema must be specified.")

        error_message = (
            re.sub(r"['\n]", "", error_message) if error_message else None
        )

        columns = [
            "EXECUTION_ID",
            "EXECUTION_START_TS",
            "EXECUTION_END_TS",
            "EXECUTION_STATUS",
            "SOURCE_DATABASE",
            "SOURCE_SCHEMA",
            "SOURCE_WAREHOUSE",
            "TARGET_DATABASE",
            "TARGET_WAREHOUSE",
            "TARGET_SCHEMA",
            "TARGET_TABLE",
            "RECORDS_INSERTED",
            "RECORDS_UPDATED",
            "RECORDS_DELETED",
            "CARRIER_NAME",
            "ERROR_MESSAGE",
        ]

        query = f"""
INSERT INTO {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE (
{', '.join(columns)}
) VALUES (
'{uuid.uuid4()}',
'{execution_start_ts}',
'{execution_end_ts}',
'{execution_status.value}',
'{datamart_table.source_database}',
'{datamart_table.source_schema}',
'{datamart_table.source_warehouse}',
'{datamart_table.target_database}',
'{datamart_table.target_warehouse}',
'{datamart_table.target_schema}',
'{datamart_table.target_table}',
{records_inserted},
{records_updated},
{records_deleted},
'{datamart_table.carrier_name}',
{f"'{error_message}'" if error_message else "NULL"}
)
"""
        result = self.execute_query(query, lazy=False)
        if result is None:
            raise SnowflakeTableException(
                f"Failed to save execution log to {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
            )

        if "number of rows inserted" in result[0]:
            if result[0]["number of rows inserted"] != 1:
                d_logger.error(
                    f"Failed to save execution log to {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
                )
                raise SnowflakeTableException(
                    f"Failed to save execution log to {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
                )
        else:
            d_logger.error(
                f"Failed to save execution log to {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
            )
            raise SnowflakeTableException(
                f"Failed to save execution log to {datamart_table.target_database}.{datamart_table.target_schema}.DATAMART_EXECUTION_TABLE"
            )

    def execute_multiple_statements(
        self, statements: list[str], lazy: bool = True
    ) -> list[Row] | DataFrame | None:
        """
        Executes multiple SQL statements sequentially in the same session.

        Args:
            statements (list[str]): A list of SQL statements to execute.
            lazy (bool): If True, returns a DataFrame for the last statement. If False, collects the result.

        Returns:
            list[Row] | DataFrame | None: The result of the final statement.
        """
        if self.session is None:
            raise SnowflakeSessionException(
                "Session is not initialized. Please use the context manager."
            )

        result = None

        try:
            for i, statement in enumerate(statements):
                if i == len(statements) - 1:
                    # Last statement: return result
                    result = self.session.sql(statement)
                    if not lazy:
                        result = result.collect()
                else:
                    # Intermediate SET statements: just execute
                    self.session.sql(statement).collect()
        except Exception as e:
            logger.error(f"Error executing multiple statements: {e}")
            return None

        return result
