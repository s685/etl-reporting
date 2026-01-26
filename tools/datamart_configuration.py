from pathlib import Path
from typing import Any
import yaml
from datamart_analytics.custom_exceptions.configuration_exceptions import (
    ConfigurationFileNotFoundException,
    ConfigurationLoadException,
    ConfigurationValidationException,
    TableConfigurationNotFoundException,
)
from datamart_analytics.models.custom_models import (
    DatamartConfiguration,
    TableConfiguration,
)


class ConfigurationLoader:
    """
    Loader class for datamart configurations from YAML files.
    """

    def __init__(self) -> None:
        """
        Initialize the ConfigurationLoader and load the datamart configuration.
        """
        self.datamart_configuration: list[DatamartConfiguration] = (
            self._load_configuration()
        )

    @staticmethod
    def _load_configuration() -> list[DatamartConfiguration]:
        """
        Loads data from a YAML file.

        Returns:
            list[DatamartConfiguration]: List of DatamartConfiguration instances populated with data from the YAML file.

        Raises:
            FileNotFoundError: If the file does not exist.
            yaml.YAMLError: If the file is not valid YAML.
        """
        base_path: Path = Path(__file__).parent.parent
        config_file_path: Path = base_path / "datamart" / "configuration" / "datamart_configuration.yaml"

        data: dict[str, Any] | None = None

        try:
            with open(config_file_path, "r") as f:
                data = yaml.safe_load(f)
        except FileNotFoundError:
            raise ConfigurationFileNotFoundException(f"Configuration file not found: {config_file_path}")
        except yaml.YAMLError as e:
            raise ConfigurationLoadException(f"Failed to parse YAML configuration file: {e}")
        except Exception as e:
            raise ConfigurationLoadException(f"An error occurred while loading the YAML file: {e}")

        if data is None:
            raise ConfigurationValidationException(
                "Configuration file is empty or contains no valid data"
            )

        if "datamarts" not in data:
            raise ConfigurationValidationException(
                "Configuration file must contain a 'datamarts' key at the root level"
            )

        configurations: list[DatamartConfiguration] = []
        for config_data in data["datamarts"]:
            if "tables" in config_data:
                tables = [
                    TableConfiguration(**table_data)
                    for table_data in config_data["tables"]
                ]
                config = DatamartConfiguration(tables=tables)
                configurations.append(config)

        return configurations

    def get_table_by_name(self, table_name: str) -> TableConfiguration | None:
        """
        Get Table configuration by name.

        Params:
            table_name (str): Name of the Table to filter the configuration.

        Returns:
            TableConfiguration: The Table configuration that matches the provided name.

        Raises:
            Exception: If an error occurs while retrieving the Table by name.
        """
        found_table: TableConfiguration | None = None

        try:
            for configuration in self.datamart_configuration:
                for table in configuration.tables:
                    if table.name == table_name:
                        found_table = table
        except Exception as e:
            raise TableConfigurationNotFoundException(
                f"An error occurred while retrieving table configuration '{table_name}': {e}"
            )

        return found_table
