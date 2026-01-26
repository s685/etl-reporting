import os
from pydantic import Field, field_validator
from pydantic_settings import BaseSettings
from datamart_analytics.definitions.custom_definitions import SnowflakeAuthenticatorType
from datamart_analytics.operations.obfuscation_operations import decode_string


class EnvironmentConfiguration(BaseSettings):
    """
    Configuration class for environment variables.
    """

    datamart_analytics_framework_environment: str = Field(
        ..., description="The environment the framework is running in"
    )
    snowflake_account: str = Field(
        ..., description="The Snowflake account identifier, e.g. xy12345.us-east-1"
    )
    snowflake_password_source: str | None = Field(
        default=None, description="If not provided, will use externalbrowser"
    )
    snowflake_password_target: str | None = Field(
        default=None, description="If not provided, will use externalbrowser"
    )
    snowflake_user_source: str = Field(
        ..., description="If not provided, will use externalbrowser"
    )
    snowflake_user_target: str = Field(
        ..., description="If not provided, will use externalbrowser"
    )
    snowflake_role_source: str = Field(
        ..., description="If not provided, will use externalbrowser"
    )
    snowflake_role_target: str = Field(
        ..., description="If not provided, will use externalbrowser"
    )
    snowflake_authenticator: SnowflakeAuthenticatorType | None = Field(
        default=None, description="External authenticator, externalbrowser"
    )
    snowflake_private_key_file: str | None = Field(
        default=None, description="Path to the private key file for key pair authentication"
    )
    snowflake_private_key_password: str | None = Field(
        default=None, description="Password for the private key"
    )

    # When setting up environmental variables, there is no way to set them to None.
    # As a workaround, we will set them to an empty string and convert them to None here.

    @field_validator("snowflake_password_source")
    def check_snowflake_password_source(cls, value: str) -> str | None:
        """Check if the snowflake password source is an empty string and convert it to None."""
        if len(value) == 0:
            return None
        return value

    @field_validator("snowflake_password_target")
    def check_snowflake_password_target(cls, value: str) -> str | None:
        """Check if the snowflake password target is an empty string and convert it to None."""
        if len(value) == 0:
            return None
        return value

    @field_validator(
        "snowflake_account",
        "snowflake_password_source",
        "snowflake_password_target",
        "snowflake_user_source",
        "snowflake_user_target",
        "snowflake_private_key_file",
        "snowflake_private_key_password",
    )
    def decode_field(cls, value: str) -> str | None:
        """Decode the field if it is not None or empty string."""
        if value is None:
            return None
        if len(value) == 0:
            return None
        return decode_string(value)

    @field_validator("snowflake_authenticator", mode="before")
    def check_snowflake_authenticator(cls, value: str) -> str | None:
        """Check if the snowflake authenticator is an empty string and convert it to None."""
        if value is None:
            return None
        if len(value) == 0:
            return None
        return value.upper()

    class Config:
        """
        Configuration for Pydantic settings.
        """

        env_file = (
            ".env" if os.path.exists(".env") else ".env.example" if os.path.exists(".env.example") else None
        )
        env_file_encoding = "utf-8"


environment_configuration = EnvironmentConfiguration()
