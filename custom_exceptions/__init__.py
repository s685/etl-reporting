"""
Custom Exceptions Module
=========================

This module provides custom exception classes for handling various error scenarios
in the datamart analytics framework.

Exception Hierarchy:
    - ClaimStatusException: For claim status mapping errors
    - ConfigurationException: Base for configuration errors
        - ConfigurationFileNotFoundException
        - ConfigurationLoadException
        - ConfigurationValidationException
        - TableConfigurationNotFoundException
    - SnowflakeException: Base for Snowflake-related errors
        - CouldNotCreateSnowflakeConnectionException
        - SnowflakeCredentialException
        - SnowflakeSessionException
        - SnowflakeQueryException
        - SnowflakeTableException
        - SnowflakeUpsertException
        - SnowflakePrivateKeyException
    - TestFrameworkException: Base for test framework errors
        - LoggingInitializationException
        - NoRowsValidatedException
        - LoadTestException
        - UnhandledFrameworkException
        - SQLFileNotFoundException
        - CSVFileNotFoundException
        - TestCaseParseException
        - TestCaseNotFoundException
        - SQLTemplateNotFoundException
        - TestCaseValidationException
        - OneOrMoreTestCasesFailedException
"""

from datamart_analytics.custom_exceptions.claim_status_exceptions import (
    ClaimStatusException,
)
from datamart_analytics.custom_exceptions.configuration_exceptions import (
    ConfigurationException,
    ConfigurationFileNotFoundException,
    ConfigurationLoadException,
    ConfigurationValidationException,
    TableConfigurationNotFoundException,
)
from datamart_analytics.custom_exceptions.snowflake_exceptions import (
    CouldNotCreateSnowflakeConnectionException,
    SnowflakeCredentialException,
    SnowflakeException,
    SnowflakePrivateKeyException,
    SnowflakeQueryException,
    SnowflakeSessionException,
    SnowflakeTableException,
    SnowflakeUpsertException,
)
from datamart_analytics.custom_exceptions.test_framework_exceptions import (
    CSVFileNotFoundException,
    LoadTestException,
    LoggingInitializationException,
    NoRowsValidatedException,
    OneOrMoreTestCasesFailedException,
    SQLFileNotFoundException,
    SQLTemplateNotFoundException,
    TestCaseNotFoundException,
    TestCaseParseException,
    TestCaseValidationException,
    TestFrameworkException,
    UnhandledFrameworkException,
)

__all__ = [
    "ClaimStatusException",
    "ConfigurationException",
    "ConfigurationFileNotFoundException",
    "ConfigurationLoadException",
    "ConfigurationValidationException",
    "TableConfigurationNotFoundException",
    "CouldNotCreateSnowflakeConnectionException",
    "SnowflakeException",
    "SnowflakeCredentialException",
    "SnowflakeSessionException",
    "SnowflakeQueryException",
    "SnowflakeTableException",
    "SnowflakeUpsertException",
    "SnowflakePrivateKeyException",
    "TestFrameworkException",
    "LoggingInitializationException",
    "NoRowsValidatedException",
    "LoadTestException",
    "UnhandledFrameworkException",
    "SQLFileNotFoundException",
    "CSVFileNotFoundException",
    "TestCaseParseException",
    "TestCaseNotFoundException",
    "SQLTemplateNotFoundException",
    "TestCaseValidationException",
    "OneOrMoreTestCasesFailedException",
]
