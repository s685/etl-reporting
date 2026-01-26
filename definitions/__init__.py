"""
Definitions Module
==================

This module provides Enum definitions for standardizing constant values used
throughout the datamart analytics framework.

Enums:
    - ApplicationEnvironment: Environment types (DEV, TEST, PROD)
    - DatamartFrameworkTable: Framework table names
    - SnowparkTableType: Table types (TEMPORARY, TRANSIENT, PERMANENT)
    - SnowparkTableWriteMode: Write modes for tables
    - ExecutionStatus: Status values for execution tracking
    - UpsertResultStatus: Status values for upsert operations
    - TestCaseType: Types of test cases
    - SnowflakeAuthenticatorType: Authentication methods for Snowflake
"""

from datamart_analytics.definitions.custom_definitions import (
    ApplicationEnvironment,
    DatamartFrameworkTable,
    ExecutionStatus,
    SnowflakeAuthenticatorType,
    SnowparkTableType,
    SnowparkTableWriteMode,
    TestCaseType,
    UpsertResultStatus,
)

__all__ = [
    "ApplicationEnvironment",
    "DatamartFrameworkTable",
    "SnowparkTableType",
    "SnowparkTableWriteMode",
    "ExecutionStatus",
    "UpsertResultStatus",
    "TestCaseType",
    "SnowflakeAuthenticatorType",
]
