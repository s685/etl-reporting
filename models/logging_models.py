from __future__ import annotations
from pydantic import BaseModel, Field


class ProcessLog(BaseModel):
    """
    Model for process logging.
    """

    id: str = Field(..., title="ID", description="ID")
    database_name: str | None = Field(
        default=None, title="Table database name", description="Table database name"
    )
    sql_script: str | None = Field(
        default=None, title="Sql script", description="script name"
    )
    table_schema: str | None = Field(
        default=None, title="Table Schema", description="Table Schema"
    )
    table_name: str | None = Field(
        default=None, title="Table Name", description="Table Name"
    )
    inserted_row_count: int = Field(
        default=0,
        title="inserted Row Count",
        description="inserted Table Row Count",
    )
    updated_row_count: int = Field(
        default=0,
        title="Updated Row Count",
        description="Source Table Processed Row Count",
    )
    query_duration: str | None = Field(
        default=None, title="Query Duration", description="Query Duration"
    )
    execution_timestamp: str | None = Field(
        default=None,
        title="Execution Timestamp",
        description="Execution Timestamp",
    )
    general_error_message: str | None = Field(
        default=None,
        title="General Error Message",
        description="General Error Message",
    )
    status: str | None = Field(default=None, title="Status", description="Status")
