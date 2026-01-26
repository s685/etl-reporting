"""
Configuration Exceptions
========================

Custom exceptions for configuration-related errors.
"""


class ConfigurationException(Exception):
    """Base exception for configuration-related errors."""
    
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class ConfigurationFileNotFoundException(ConfigurationException):
    """Exception raised when a configuration file is not found."""
    
    def __init__(self, message: str):
        super().__init__(message)


class ConfigurationLoadException(ConfigurationException):
    """Exception raised when loading configuration fails."""
    
    def __init__(self, message: str):
        super().__init__(message)


class ConfigurationValidationException(ConfigurationException):
    """Exception raised when configuration validation fails."""
    
    def __init__(self, message: str):
        super().__init__(message)


class TableConfigurationNotFoundException(ConfigurationException):
    """Exception raised when a specific table configuration is not found."""
    
    def __init__(self, message: str):
        super().__init__(message)
