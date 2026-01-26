import base64
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric.types import PrivateKeyTypes
from datamart_analytics.custom_exceptions.snowflake_exceptions import (
    SnowflakePrivateKeyException,
)
from datamart_analytics.logger import logger


def encode_string(value: str) -> str:
    """
    Encodes a string to its Base64 representation.

    Params:
        value (str): The string to encode.

    Returns:
        str: The base64 representation of the input string.
    """
    return base64.b64encode(value.encode()).decode()


def decode_string(value: str) -> str:
    """
    Decodes a Base64 encoded string back to its original representation.

    Params:
        value (str): The Base64 encoded string to decode.

    Returns:
        str: The original string representation.
    """
    return base64.b64decode(value.encode()).decode()


def load_snowflake_private_key(
    snowflake_secret_key_file: str, snowflake_private_key_password: str
) -> bytes:
    """
    Loads and decodes the Snowflake private key from an environment variable.

    Params:
        snowflake_secret_key_file (str): Path to the private key file.
        snowflake_private_key_password (str): Password for the private key.

    Returns:
        bytes: The decoded private key in bytes.
    """
    if snowflake_secret_key_file is None or snowflake_private_key_password is None:
        raise ValueError(
            "Both private key file path and password must be provided."
        )

    try:
        with open(snowflake_secret_key_file, "rb") as key_file:
            private_key_file = key_file.read()
    except FileNotFoundError:
        logger.error(f"Private key file not found: {snowflake_secret_key_file}")
        raise SnowflakePrivateKeyException(
            f"Private key file not found: {snowflake_secret_key_file}"
        )
    except Exception as e:
        logger.error(f"Error reading private key file: {e}")
        raise SnowflakePrivateKeyException(f"Error reading private key file: {e}")

    try:
        private_key: PrivateKeyTypes = serialization.load_pem_private_key(
            data=private_key_file,
            password=snowflake_private_key_password.encode(),
            backend=default_backend(),
        )

        private_key_bytes: bytes = private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

        return private_key_bytes
    except Exception as e:
        logger.error(f"Error loading private key: {e}")
        raise SnowflakePrivateKeyException(f"Error loading private key: {e}")
