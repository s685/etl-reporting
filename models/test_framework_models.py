from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field, field_validator
from datamart_analytics.definitions.custom_definitions import TestCaseType


class TestCaseMetadata(BaseModel):
    """
    Validate a row of test metadata from the CSV and map the SQL query.
    - For multiple columns, column_name can be a single string or a list of strings.
    """

    test_case_type: TestCaseType
    test_case_name: str | None = None
    is_enabled: bool
    fact_table_name: str | None = None
    source_database_name: str | None = None
    source_schema_name: str | None = None
    source_table_name: str | None = None
    source_column_name: str | None = None
    target_database_name: str | None = None
    target_schema_name: str | None = None
    target_table_name: str | None = None
    target_column_name: str | None = None
    query_file_path: str | None = None
    created_date: datetime | None = None
    created_by: str | None = None
    updated_date: datetime | None = None
    updated_by: str | None = None
    is_set: bool
    set_params: dict[str, Any] | None = None
    carrier_name: str | None = None
    mapped_sql_query: str = Field(..., description="Mapped SQL query")
    final_rendered_sql_query: str = Field(
        ..., description="Final rendered SQL query after mapping"
    )

    @field_validator("created_date", "updated_date", mode="before")
    @classmethod
    def allow_empty_string_for_dates(cls, v):
        """
        Allow empty strings to be converted to None for date fields.
        """
        if v == "" or v is None:
            return None
        return v
