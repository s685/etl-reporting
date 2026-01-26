"""
Operations Module
=================

This module provides utility operations for the datamart analytics framework,
including obfuscation, encoding/decoding, and Snowflake-specific operations.

Functions:
    Obfuscation Operations:
        - encode_string: Encode strings to Base64
        - decode_string: Decode Base64 strings
        - load_snowflake_private_key: Load and process Snowflake private keys
    
    Snowflake Query Operations:
        - create_execution_log_table: Create execution log tables in Snowflake
"""

from datamart_analytics.operations.obfuscation_operations import (
    decode_string,
    encode_string,
    load_snowflake_private_key,
)
from datamart_analytics.operations.snowflake_query_operations import (
    create_execution_log_table,
)

__all__ = [
    "encode_string",
    "decode_string",
    "load_snowflake_private_key",
    "create_execution_log_table",
]
