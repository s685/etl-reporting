class SnowflakeException(Exception):
    """Base exception for all Snowflake-related errors."""
    
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class CouldNotCreateSnowflakeConnectionException(SnowflakeException):
    """Exception raised when Snowflake connection could not be created."""
    
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakeCredentialException(SnowflakeException):
    """Exception raised when there are issues with Snowflake credentials."""
    
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakeSessionException(SnowflakeException):
    """Exception raised when Snowflake session is not initialized or has issues."""
    
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakeQueryException(SnowflakeException):
    """Exception raised when a Snowflake query execution fails."""
    
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakeTableException(SnowflakeException):
    """Exception raised when there are issues with Snowflake tables."""
    
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakeUpsertException(SnowflakeException):
    """Exception raised when upsert operations fail."""
    
    def __init__(self, message: str):
        super().__init__(message)


class SnowflakePrivateKeyException(SnowflakeException):
    """Exception raised when there are issues with private key authentication."""
    
    def __init__(self, message: str):
        super().__init__(message)
