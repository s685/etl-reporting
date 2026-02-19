from enum import Enum


class ApplicationEnvironment(Enum):
    """Enum for application environment."""

    DEV = "DEV"
    TEST = "TEST"
    PROD = "PROD"

    def __str__(self) -> str:
        return self.value


class DatamartFrameworkTable(Enum):
    """Enum for ETL framework tables."""

    CONTROL_TABLE = "CONTROL_TABLE"
    EXECUTION_TABLE = "EXECUTION_TABLE"
    MERGE_EXECUTION_LOG_TABLE = "MERGE_EXECUTION_LOG_TABLE"

    def __str__(self) -> str:
        return self.value


class SnowparkTableType(Enum):
    """Enum for Snowpark table types."""

    TEMPORARY = "temporary"
    TRANSIENT = "transient"
    PERMANENT = "permanent"

    def __str__(self) -> str:
        return self.value


class SnowparkTableWriteMode(Enum):
    """Enum for Snowpark table write modes."""

    APPEND = "append"
    OVERWRITE = "overwrite"
    TRUNCATE = "truncate"
    ERROR_IF_EXISTS = "error_if_exists"
    IGNORE = "ignore"

    def __str__(self) -> str:
        return self.value


class ExecutionStatus(Enum):
    """Enum for execution status."""

    STARTING = "STARTING"
    SUCCESS = "SUCCESS"
    FAIL = "FAIL"
    IN_PROGRESS = "IN_PROGRESS"

    def __str__(self) -> str:
        return self.value


class UpsertResultStatus(Enum):
    """Enum for upsert result status."""

    SUCCESS = "SUCCESS"
    FAIL = "FAIL"

    def __str__(self) -> str:
        return self.value


class TestCaseType(str, Enum):
    """Enum for test case types."""

    DATA_TESTING = "DATA_TESTING"
    SINGULAR_DATA_TESTING = "SINGULAR_DATA_TESTING"


class SnowflakeAuthenticatorType(str, Enum):
    """Enum for Snowflake authenticator type."""

    EXTERNALBROWSER = "EXTERNALBROWSER"
    SNOWFLAKE_JWT = "SNOWFLAKE_JWT"


class SourceTargetDatabaseConnectionType(str, Enum):
    """Enum for source-to-target database connection type in test framework."""

    SNOWFLAKE_TO_SNOWFLAKE = "SNOWFLAKE_TO_SNOWFLAKE"
    SQLSERVER_TO_SNOWFLAKE = "SQLSERVER_TO_SNOWFLAKE"


class SQLServerODBCVersion(str, Enum):
    """Enum for SQL Server ODBC driver version."""

    MS_ODBC_DRIVER_18 = "ODBC Driver 18 for SQL Server"
