import os
import sys
from typing import Any

import snowflake.connector

# Retrieve Snowflake credentials from environment variables
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_TOKEN = os.getenv("SNOWFLAKE_TOKEN")
SNOWFLAKE_AUTHENTICATOR = os.getenv(
    "SNOWFLAKE_AUTHENTICATOR", "PROGRAMMATIC_ACCESS_TOKEN"
)
SNOWFLAKE_ROLE = os.getenv("SNOWFLAKE_ROLE")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")

# Validate input explicitly (object name and GitHub Issue ID)
if len(sys.argv) != 3:
    print("Usage: python drop_schema.py <object_name> <github_issue_id>")
    sys.exit(1)

object_name = sys.argv[1]
github_issue_id = sys.argv[2]

# Construct the dynamic schema name to drop.  In our DataOps pipeline,
# dbt’s dynamic naming macro appends `_issue_<github_issue_id>` to a
# custom schema (e.g. `bronze_issue_123`).  To align with that convention
# and drop the correct schema, build the name from the provided
# object_name and GitHub Issue ID.  For example, `bronze` + `_issue_42`
# becomes `bronze_issue_42`.
schema_name = f"{object_name}_issue_{github_issue_id}"


def connect_to_snowflake() -> snowflake.connector.SnowflakeConnection:
    """Establish and return a Snowflake connection."""
    if not all(
        [
            SNOWFLAKE_ACCOUNT,
            SNOWFLAKE_USER,
            SNOWFLAKE_TOKEN,
            SNOWFLAKE_ROLE,
            SNOWFLAKE_WAREHOUSE,
            SNOWFLAKE_DATABASE,
        ]
    ):
        raise RuntimeError("Missing Snowflake credentials for token-based auth")
    return snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        authenticator=SNOWFLAKE_AUTHENTICATOR,
        token=SNOWFLAKE_TOKEN,
        role=SNOWFLAKE_ROLE,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
    )


def execute_sql(conn: snowflake.connector.SnowflakeConnection, query: str) -> None:
    """Execute a single SQL statement and report success or failure."""
    try:
        with conn.cursor() as cs:
            cs.execute(query)
            print(f"✅ Successfully executed: {query}")
    except Exception as exc:
        print(f"🚨 Error executing SQL: {query}\nError: {exc}")
        sys.exit(1)


def drop_schema_and_objects(schema_name: str) -> None:
    """Drop a Snowflake schema and all contained objects."""
    conn = connect_to_snowflake()
    print(
        f"🔄 Connected explicitly to Snowflake to drop schema and objects: {schema_name}"
    )
    # Construct the drop statement
    drop_schema_query = (
        f"DROP SCHEMA IF EXISTS {SNOWFLAKE_DATABASE}.{schema_name} CASCADE;"
    )
    execute_sql(conn, drop_schema_query)
    conn.close()
    print(f"🧹 Dropped schema and objects for: {schema_name}")


if __name__ == "__main__":
    drop_schema_and_objects(schema_name)
