# Test Framework Workflow Scenarios

Used to validate `test_framework_run.py` and `tools/test_framework_helper.py` across all workflows.

## Entry point

- **Script:** `test_framework_run.py`
- **Required args:** `--source_database_name`, `--target_database_name`, `--database_warehouse`, `--table_schema_name`
- **Optional:** `--specification_csv_path`, `--source_target_database_connection_type` (default: SNOWFLAKE_TO_SNOWFLAKE), `--sql_server_host`, `--sql_server_port`, `--sql_to_snowflake_query_file_path`

## Scenarios

### 1. SQLSERVER_TO_SNOWFLAKE

| Step | Action | Expected |
|------|--------|----------|
| 1.1 | `--source_target_database_connection_type SQLSERVER_TO_SNOWFLAKE` without `--sql_server_host` or `--sql_to_snowflake_query_file_path` | Exit 1, `LoadTestException`: "SQL Server host and query file path must be provided..." |
| 1.2 | Same with `--sql_to_snowflake_query_file_path nonexistent.sql` | Exit 1, file-not-found or `CompareSQLToSnowflakeException` (when file read fails) |
| 1.3 | Valid host + path to SQL file with no `-- START_TEST` blocks | Exit 1, `CompareSQLToSnowflakeException` (no templates found) |
| 1.4 | Valid host + path to valid SQL file; all comparisons match | Exit 0 |
| 1.5 | Valid host + path to valid SQL file; one or more test cases have data mismatch | Exit 1, `CompareSQLToSnowflakeException`: "One or more SQL-to-Snowflake test cases failed: ..." (test failure, not script crash) |

**Sample SQL file (format):** `sql/sql_server_to_snowflake/sample_sql_to_snowflake_tests.sql`

### 2. SNOWFLAKE_TO_SNOWFLAKE

| Step | Action | Expected |
|------|--------|----------|
| 2.1 | Default connection type, no `--specification_csv_path` | Exit 1, `CSVFileNotFoundException`: "Specification CSV file not found: (not provided)" |
| 2.2 | `--specification_csv_path /nonexistent.csv` | Exit 1, `CSVFileNotFoundException` |
| 2.3 | Path to CSV that exists but has no enabled rows (or empty) | Exit 1, `NoRowsValidatedException` |
| 2.4 | Valid CSV and SQL templates; all tests pass | Exit 0 |
| 2.5 | Valid CSV and SQL templates; one or more tests FAIL | Exit 1, `OneOrMoreTestCasesFailedException` (wrapped as `LoadTestException`) |

### 3. Invalid connection type

| Step | Action | Expected |
|------|--------|----------|
| 3.1 | `--source_target_database_connection_type INVALID` | Exit 1, `LoadTestException`: "Unsupported source_target_database_connection_type: INVALID..." |

### 4. CLI

| Step | Action | Expected |
|------|--------|----------|
| 4.1 | `--help` | Exit 0, usage and list of required/optional args |
| 4.2 | Only `--source_database_name x` (missing other required) | Exit 2, argparse error listing missing args |

## Helper: SQL-to-Snowflake file format

For `extract_sql_to_snowflake_test_case_queries()` (SQLSERVER_TO_SNOWFLAKE):

- Blocks between `-- START_TEST` and `-- END_TEST`
- Inside each block: `-- @TEST_NAME: <name>`, `-- @START_SQL_SERVER_QUERY:` … `-- @END_SQL_SERVER_QUERY`, `-- @START_SNOWFLAKE_QUERY:` … `-- @END_SNOWFLAKE_QUERY`
- Placeholders in query text: `{source_database_name}`, `{target_database_name}` (replaced by `get_sql_to_snowflake_final_rendered_query`)

## Running validation

With project env and dependencies (Snowflake/SQL Server optional for 1.1–1.3, 2.1–2.2, 3.1, 4.x):

```bash
python test_framework_run.py --help
python test_framework_run.py --source_database_name a --target_database_name b --database_warehouse c --table_schema_name d --source_target_database_connection_type SQLSERVER_TO_SNOWFLAKE
# Expect LoadTestException
python test_framework_run.py --source_database_name a --target_database_name b --database_warehouse c --table_schema_name d --source_target_database_connection_type INVALID
# Expect LoadTestException (unsupported type)
```
