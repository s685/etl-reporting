class TestFrameworkException(Exception):
    """
    Custom exception for test framework errors.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


class LoggingInitializationException(TestFrameworkException):
    """
    Raised when logging initialization fails.
    """

    def __init__(self, message: str):
        super().__init__(message)


class NoRowsValidatedException(TestFrameworkException):
    """
    Raised when no rows are validated in the test.
    """

    def __init__(self, message: str):
        super().__init__(message)


class LoadTestException(TestFrameworkException):
    """
    Raised for errors during the load_test execution.
    """

    def __init__(self, message: str):
        super().__init__(message)


class UnhandledFrameworkException(TestFrameworkException):
    """
    Raised for any unhandled exceptions in the test framework.
    """

    def __init__(self, message: str):
        super().__init__(message)


class SQLFileNotFoundException(TestFrameworkException):
    """
    Raised when the SQL file is not found.
    """

    def __init__(self, message: str):
        super().__init__(message)


class CSVFileNotFoundException(TestFrameworkException):
    """
    Raised when the CSV file is not found.
    """

    def __init__(self, message: str):
        super().__init__(message)


class TestCaseParseException(TestFrameworkException):
    """
    Raised when parsing a test case block fails.
    """

    def __init__(self, message: str):
        super().__init__(message)


class TestCaseNotFoundException(TestFrameworkException):
    """
    Raised when a test_case is invalid or missing in the CSV row.
    """

    def __init__(self, message: str):
        super().__init__(message)


class SQLTemplateNotFoundException(TestFrameworkException):
    """
    Raised when a SQL template is not found for a test_case.
    """

    def __init__(self, message: str):
        super().__init__(message)


class TestCaseValidationException(TestFrameworkException):
    """
    Raised when validation of a test case row fails.
    """

    def __init__(self, message: str):
        super().__init__(message)


class OneOrMoreTestCasesFailedException(TestFrameworkException):
    """
    Raised when one or more test cases fail.
    """

    def __init__(self, message: str):
        super().__init__(message)
