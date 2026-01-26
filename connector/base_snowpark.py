from abc import ABC, abstractmethod
from snowflake.snowpark import DataFrame, QueryHistory, Row, Table
from datamart_analytics.models.custom_models import (
    DatamartTable,
    DatamartTable_integrated,
)


class BaseSnowparkConnector(ABC):
    """
    Base class for Snowpark query handlers.
    """

    def __init__(self) -> None:
        """
        Initialize the SnowparkQuery with a Snowflake session.

        :param snowflake_credentials: Credentials for connecting to Snowflake.
        :param datamart_table: Configuration for the datamart table, including source and target.
        """
        self.last_query_id: str | None = None
        self.last_query_result: str | None = None

    @abstractmethod
    def get_table(self, table_name: str) -> Table | None:
        """
        Get a Snowpark table by its name.

        :param table_name: The name of the table to retrieve.
        :return: The Snowpark table object or None if the table does not exist.
        :raises NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def execute_query(
        self, query: str, lazy: bool = False
    ) -> list[Row] | DataFrame | None:
        """
        Execute a Snowpark query.

        :param query: The SQL query to execute.
        :param lazy: If True, return a DataFrame object. If False, return the result of the query.
        :return: The result of the query execution.
        :raises NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def execute_query_from_file(
        self,
        file_name: str,
        datamart_table: DatamartTable | DatamartTable_integrated,
        lazy: bool = True,
        folder_name: str | None = None,
    ) -> list[Row] | DataFrame | None:
        """
        Execute a Snowpark query from a file.

        :param file_name: The name of the file containing the SQL query.
        :param datamart_table: The configuration for the datamart table, including source and target.
        :param lazy: If True, return a DataFrame object. If False, return the result of the query.
        :param folder_name: Optional folder name where the file is located.
        :return: The result of the query execution.
        :raises NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method.")

    @abstractmethod
    def get_query_history(
        self, id: str | None = None, limit: int = 10, lazy: bool = True
    ) -> QueryHistory | None:
        """
        Get the history of a specific query execution.

        :param query_id: The ID of the query to retrieve history for.
        :return: The query history or None if not found.
        :raises NotImplementedError: If the method is not implemented in the subclass.
        """
        raise NotImplementedError("Subclasses must implement this method.")
