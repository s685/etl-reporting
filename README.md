# Datamart Analytics Framework

A comprehensive Python framework for managing Snowflake-based datamart operations, including data extraction, transformation, loading (ETL), and testing.

## Features

- **Snowpark Integration**: Seamless integration with Snowflake Snowpark for efficient data operations
- **Configuration Management**: YAML-based configuration for datamarts and tables
- **Credential Management**: Secure handling of Snowflake credentials with support for multiple authentication methods
- **Custom Exception Handling**: Comprehensive exception hierarchy for better error handling and debugging
- **Logging**: Structured logging with both main and debug loggers
- **Test Framework**: Built-in testing framework for data validation
- **Type Safety**: Full type hints and Pydantic models for data validation

## Installation

### Prerequisites

- Python 3.10 or higher
- Snowflake account with appropriate permissions
- Private key file (if using key-pair authentication)

### Setup

1. Clone the repository:
```bash
git clone <repository-url>
cd test
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

## Configuration

### Environment Variables

Create a `.env` file with the following variables:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER_SOURCE=source_user
SNOWFLAKE_USER_TARGET=target_user
SNOWFLAKE_PASSWORD_SOURCE=source_password  # Base64 encoded
SNOWFLAKE_PASSWORD_TARGET=target_password  # Base64 encoded
SNOWFLAKE_ROLE_SOURCE=source_role
SNOWFLAKE_ROLE_TARGET=target_role
SNOWFLAKE_PRIVATE_KEY_FILE=path/to/key.p8
SNOWFLAKE_PRIVATE_KEY_PASSWORD=key_password
SNOWFLAKE_AUTHENTICATOR=externalbrowser  # or snowflake_jwt
DATAMART_ANALYTICS_FRAMEWORK_ENVIRONMENT=DEV  # DEV, TEST, or PROD
```

### Datamart Configuration

Configure datamarts in YAML files under `datamart/configuration/`:

```yaml
datamarts:
  - tables:
      - name: example_table
        source_table_name: SOURCE_TABLE
        target_table_name: TARGET_TABLE
        join_keys:
          - KEY_COLUMN
        update_columns:
          - COLUMN1
          - COLUMN2
```

## Usage

### Running a Report

```python
from datamart_analytics.tools import create_and_parse_datamart_table_args, create_target_credentials
from datamart_analytics.connector import SnowparkConnector

# Parse command-line arguments
datamart_table = create_and_parse_datamart_table_args("report_name")

# Create credentials
credentials = create_target_credentials(datamart_table)

# Execute queries
with SnowparkConnector(credentials) as connector:
    df = connector.execute_query_from_file(
        file_name="query.sql",
        datamart_table=datamart_table,
        folder_name="report_folder"
    )
    connector.save_as_table(df, "output_table")
```

### Command-Line Arguments

Reports support the following command-line arguments:

```bash
python report.py \
    --name "report_name" \
    --source-database "SOURCE_DB" \
    --source-warehouse "SOURCE_WH" \
    --source-schema "SOURCE_SCHEMA" \
    --target-database "TARGET_DB" \
    --target-warehouse "TARGET_WH" \
    --target-schema "TARGET_SCHEMA" \
    --target-table "TARGET_TABLE" \
    --carrier-name "CARRIER_NAME"
```

## Architecture

### Module Structure

```
datamart_analytics/
├── connector/          # Snowpark connectors
├── custom_exceptions/  # Custom exception classes
├── definitions/        # Enum definitions
├── models/            # Pydantic models
├── operations/        # Utility operations
├── tools/             # Helper utilities
├── sql/               # SQL scripts and reports
├── environment.py     # Environment configuration
└── logger.py          # Logging configuration
```

### Key Components

- **BaseSnowparkConnector**: Abstract base class for Snowpark operations
- **SnowparkConnector**: Concrete implementation with full functionality
- **DatamartTable**: Configuration model for datamart tables
- **SnowflakeCredentials**: Credential management model
- **ConfigurationLoader**: YAML configuration loader

## Development

### Running Tests

```bash
pytest tests/
```

### Code Quality

The project uses pre-commit hooks for code quality:

```bash
pre-commit install
pre-commit run --all-files
```

### Type Checking

```bash
mypy .
```

## Exception Handling

The framework provides a comprehensive exception hierarchy:

- **SnowflakeException**: Base for Snowflake errors
  - SnowflakeCredentialException
  - SnowflakeSessionException
  - SnowflakeQueryException
  - SnowflakeTableException
  - SnowflakeUpsertException
  - SnowflakePrivateKeyException

- **ConfigurationException**: Base for configuration errors
  - ConfigurationFileNotFoundException
  - ConfigurationLoadException
  - ConfigurationValidationException
  - TableConfigurationNotFoundException

- **TestFrameworkException**: Base for test framework errors

## Logging

Two loggers are available:

- **logger**: Main logger for INFO level messages
- **d_logger**: Debug logger for detailed diagnostic information

Logs are written to:
- Console (INFO and above)
- `datamart_framework.log` (all levels)
- `datamart_framework_debug.log` (debug logger only)

## Contributing

1. Create a feature branch
2. Make your changes
3. Run tests and linters
4. Submit a pull request

## License

[Your License Here]

## Support

For issues and questions, please open an issue in the repository.
