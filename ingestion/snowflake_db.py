import os
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv
from snowflake.connector import SnowflakeConnection

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(PROJECT_ROOT / ".env")


def get_snowflake_connection() -> SnowflakeConnection:
    """
    Create and return a connection to the E-commerce Snowflake warehouse.
    Supports password and external browser authentication.
    """

    required_variables = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
    ]

    missing = [variable for variable in required_variables if not os.getenv(variable)]

    if missing:
        raise ValueError(f"Missing Snowflake environment variables: {missing}")

    authenticator = os.getenv("SNOWFLAKE_AUTHENTICATOR", "snowflake")

    connection_config = {
        "account": os.getenv("SNOWFLAKE_ACCOUNT"),
        "user": os.getenv("SNOWFLAKE_USER"),
        "authenticator": authenticator,
    }

    # Optional parameters (falls back to user's default in Snowflake if unset)
    for param, env_var in [
        ("warehouse", "SNOWFLAKE_WAREHOUSE"),
        ("database", "SNOWFLAKE_DATABASE"),
        ("schema", "SNOWFLAKE_SCHEMA"),
        ("role", "SNOWFLAKE_ROLE"),
    ]:
        val = os.getenv(env_var)
        if val:
            connection_config[param] = val

    if authenticator == "snowflake":
        password = os.getenv("SNOWFLAKE_PASSWORD")

        if not password:
            raise ValueError(
                "SNOWFLAKE_PASSWORD is required for password authentication."
            )

        connection_config["password"] = password

    connection = snowflake.connector.connect(**connection_config)

    return connection
