"""
Connector Module
================

This module provides connectors for interacting with Snowflake using Snowpark.

Classes:
    BaseSnowparkConnector: Abstract base class defining the interface for Snowpark connectors
    SnowparkConnector: Concrete implementation of Snowpark connector with full functionality

Usage:
    from datamart_analytics.connector import SnowparkConnector
    from datamart_analytics.models import SnowflakeCredentials
    
    credentials = SnowflakeCredentials(...)
    with SnowparkConnector(credentials) as connector:
        df = connector.execute_query("SELECT * FROM table")
"""

from datamart_analytics.connector.base_snowpark import BaseSnowparkConnector
from datamart_analytics.connector.snowpark_connector import SnowparkConnector

__all__ = [
    "BaseSnowparkConnector",
    "SnowparkConnector",
]
