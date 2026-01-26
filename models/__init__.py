"""
Models Module
=============

This module provides Pydantic models for data validation and configuration
management throughout the datamart analytics framework.

Models:
    - SnowflakeCredentials: Configuration for Snowflake connection credentials
    - DatamartTable: Configuration for datamart table operations
    - DatamartTable_integrated: Integrated datamart table configuration
    - ExecutionLog: Model for execution logging
    - UpsertResult: Model for upsert operation results
    - TableConfiguration: Configuration for individual tables
    - DatamartConfiguration: Configuration for datamart collections
    - ProcessLog: Model for process logging
    - TestCaseMetadata: Metadata for test cases

Functions:
    - create_execution_log_table: Create execution log tables in Snowflake
    - create_execution_metadata_table: Create execution metadata tables
"""

from datamart_analytics.models.custom_models import (
    DatamartConfiguration,
    DatamartTable,
    DatamartTable_integrated,
    ExecutionLog,
    SnowflakeCredentials,
    TableConfiguration,
    UpsertResult,
)
from datamart_analytics.models.load_models import (
    create_execution_log_table,
    create_execution_metadata_table,
)
from datamart_analytics.models.logging_models import ProcessLog
from datamart_analytics.models.test_framework_models import TestCaseMetadata

__all__ = [
    "SnowflakeCredentials",
    "DatamartTable",
    "ExecutionLog",
    "UpsertResult",
    "DatamartTable_integrated",
    "TableConfiguration",
    "DatamartConfiguration",
    "ProcessLog",
    "TestCaseMetadata",
    "create_execution_log_table",
    "create_execution_metadata_table",
]
