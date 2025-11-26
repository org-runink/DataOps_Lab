#!/usr/bin/env python3
import os
import pathlib
import sys

import snowflake.connector

DEFAULT_AUTHENTICATOR = "PROGRAMMATIC_ACCESS_TOKEN"


def fail(msg: str, code: int = 1):
    print(f"::error::{msg}")
    sys.exit(code)


def render_sql(sql_path: pathlib.Path, database: str, prefix: str) -> str:
    if not sql_path.exists():
        fail(f"SQL file not found at: {sql_path}")

    sql_text = sql_path.read_text(encoding="utf-8")
    replacements = {
        "__database__": database,
        "__schema_prefix__": prefix,
        "__schemaPrefix__": prefix,  # tolerate camelCase
    }
    for k, v in replacements.items():
        sql_text = sql_text.replace(k, v)
    return sql_text


def main():
    required_env = [
        "SNOWFLAKE_ACCOUNT",
        "SNOWFLAKE_USER",
        "SNOWFLAKE_ROLE",
        "SNOWFLAKE_WAREHOUSE",
        "METRICS_DATABASE",
        "METRICS_SCHEMA_PREFIX",
        "METRICS_SQL_PATH",
        "SNOWFLAKE_TOKEN",
    ]
    missing = [k for k in required_env if not os.environ.get(k)]
    if missing:
        fail(f"Missing required environment variables: {', '.join(missing)}")

    account = os.environ["SNOWFLAKE_ACCOUNT"]
    user = os.environ["SNOWFLAKE_USER"]
    role = os.environ["SNOWFLAKE_ROLE"]
    wh = os.environ["SNOWFLAKE_WAREHOUSE"]
    database = os.environ["METRICS_DATABASE"]
    prefix = os.environ["METRICS_SCHEMA_PREFIX"]
    sql_path = os.environ["METRICS_SQL_PATH"]
    gh_output = os.environ.get("GITHUB_OUTPUT")
    token = os.environ["SNOWFLAKE_TOKEN"]
    authenticator = os.environ.get("SNOWFLAKE_AUTHENTICATOR", DEFAULT_AUTHENTICATOR)

    repo_root = pathlib.Path(os.environ.get("GITHUB_WORKSPACE", "."))
    sql_full_path = (repo_root / sql_path).resolve()
    sql_text = render_sql(sql_full_path, database, prefix)

    print("::group::Rendered SQL (sql_metrics.sql)")
    for line in sql_text.splitlines():
        print(line)
    print("::endgroup::")

    conn = snowflake.connector.connect(
        account=account,
        user=user,
        authenticator=authenticator,
        token=token,
        role=role,
        warehouse=wh,
        database=database,
        session_parameters={"QUERY_TAG": "observability_metrics_workflow"},
    )
    try:
        cur = conn.cursor()
        cur.execute(sql_text)
        row = cur.fetchone()
        if not row:
            fail("Metrics query returned no rows.")
        columns = [col[0] for col in cur.description] if cur.description else None
    finally:
        try:
            conn.close()
        except Exception:
            pass

    try:
        if columns:
            col_map = {str(columns[i]).upper(): row[i] for i in range(len(columns))}
            last_date = col_map.get("LAST_DATE", row[0])
            wk_total_queries = col_map.get("WK_TOTAL_QUERIES", row[1])
            wk_failed_queries = col_map.get("WK_FAILED_QUERIES", row[2])
            wk_avg = col_map.get("WK_AVG_QUERY_DURATION_SEC", row[3])
            storage_gb = col_map.get("STORAGE_GB_ESTIMATE", row[4])
        else:
            last_date, wk_total_queries, wk_failed_queries, wk_avg, storage_gb = row
    except Exception as exc:
        fail(
            f"Unexpected result shape from metrics SQL. Expected 5 columns; error: {exc}"
        )

    print("=== Snowflake INFORMATION_SCHEMA Metrics (last 7 days) ===")
    print(f"Last date:            {last_date}")
    print(f"Total queries:        {wk_total_queries}")
    print(f"Failed queries:       {wk_failed_queries}")
    print(f"Avg query duration s: {wk_avg}")
    print(f"Storage (GB):         {storage_gb}")

    if gh_output:
        with open(gh_output, "a") as fh:
            fh.write(f"last_date={last_date}\n")
            fh.write(f"wk_total_queries={wk_total_queries}\n")
            fh.write(f"wk_failed_queries={wk_failed_queries}\n")
            fh.write(f"wk_avg_sec={wk_avg}\n")
            fh.write(f"storage_gb={storage_gb}\n")
    else:
        print("::warning::GITHUB_OUTPUT not set; step outputs will not be exported.")


if __name__ == "__main__":
    main()
