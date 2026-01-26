"""
Tools Module
============

This module provides utility tools and helper functions for datamart operations,
configuration management, and test framework support.

Classes:
    - ConfigurationLoader: Load and manage datamart configurations from YAML

Functions:
    Datamart Utilities:
        - get_substitutions: Build template variable substitutions
        - create_table_from_ddl: Create tables from DDL statements
        - stage_table: Stage DataFrames as temporary tables
        - generate_merge_sql: Generate SQL MERGE statements
        - execute_merge: Execute merge operations
        - log_process: Log process execution details
        - extract_table_name_from_ddl: Extract table names from DDL
        - replace_template_vars_case_insensitive: Replace template variables
        - read_and_substitute_ddl: Read and substitute DDL templates
        - load_profile_yaml: Load profile configurations
        - extract_and_validate_table_name: Extract and validate table names
        - check_table_exists: Check if a table exists
        - create_target_credentials: Create target database credentials
        - create_source_credentials: Create source database credentials
        - create_datamart_table_parser: Create argument parser for reports
        - parse_args_to_datamart_table: Parse arguments to DatamartTable
        - create_and_parse_datamart_table_args: Create parser and parse arguments
    
    Test Framework Helpers:
        - load_test_case_cross_reference_table: Load test specifications
        - generate_set_statements: Generate SQL SET statements
        - parse_sql_file: Parse SQL files for test cases
        - map_and_validate_data: Map and validate test data
        - And many more test framework utility functions
"""

from datamart_analytics.tools.datamart_configuration import ConfigurationLoader
from datamart_analytics.tools.datamart_utils import (
    check_table_exists,
    create_and_parse_datamart_table_args,
    create_datamart_table_parser,
    create_source_credentials,
    create_table_from_ddl,
    create_target_credentials,
    execute_merge,
    extract_and_validate_table_name,
    extract_table_name_from_ddl,
    generate_merge_sql,
    get_substitutions,
    load_profile_yaml,
    log_process,
    parse_args_to_datamart_table,
    read_and_substitute_ddl,
    replace_template_vars_case_insensitive,
    stage_table,
)
from datamart_analytics.tools.test_framework_helper import (
    analyze_and_log_results,
    build_and_map_sql_query_to_row,
    build_final_rendered_sql_query,
    create_column_conditions_for_final_rendered_query,
    create_set_statements,
    format_column,
    generate_set_statements,
    load_test_case_cross_reference_table,
    log_validated_row,
    map_query_and_validate_test_case_data,
    parse_set_params,
    parse_sql_file,
)

__all__ = [
    "ConfigurationLoader",
    "get_substitutions",
    "create_table_from_ddl",
    "stage_table",
    "generate_merge_sql",
    "execute_merge",
    "log_process",
    "extract_table_name_from_ddl",
    "replace_template_vars_case_insensitive",
    "read_and_substitute_ddl",
    "load_profile_yaml",
    "extract_and_validate_table_name",
    "check_table_exists",
    "create_target_credentials",
    "create_source_credentials",
    "create_datamart_table_parser",
    "parse_args_to_datamart_table",
    "create_and_parse_datamart_table_args",
    "load_test_case_cross_reference_table",
    "create_set_statements",
    "parse_set_params",
    "generate_set_statements",
    "parse_sql_file",
    "build_final_rendered_sql_query",
    "build_and_map_sql_query_to_row",
    "map_query_and_validate_test_case_data",
    "format_column",
    "create_column_conditions_for_final_rendered_query",
    "log_validated_row",
    "analyze_and_log_results",
]
