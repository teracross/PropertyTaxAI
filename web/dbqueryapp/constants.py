"""
This module contains constants used across the DBQueryApp.

Constants:
- DEFAULT_SQL_QUERIES: A dictionary of predefined, read-only SQL queries for the application.
- DISALLOWED_OPERATIONS: A tuple of SQL operations (e.g., INSERT, UPDATE) that are restricted to ensure read-only query execution.
- ALLOWED_SQL_KEYWORDS: A tuple of allowed SQL keywords (e.g., SELECT, WITH) to enforce safe query validation.

Purpose:
- DEFAULT_SQL_QUERIES provides a whitelist of safe, pre-approved SQL queries for execution in the application.
- DISALLOWED_OPERATIONS and ALLOWED_SQL_KEYWORDS are used in SQL validation to ensure that only read-only queries are executed.
"""
from sqlglot.expressions import Insert, Update, Delete, Drop, Alter, Create, TruncateTable, Merge, Grant, Revoke, Replace

DEFAULT_SQL_QUERIES = {
    'get_first_100_real_acct': """
        SELECT
          *
        FROM
          real_acct
        LIMIT
          100
    """,
    'get_avg_bldg_and_land_val_by_state_class': """
        SELECT
          state_class,
          AVG(CAST(bld_val AS NUMERIC)) AS avg_building_value,
          AVG(CAST(land_val AS NUMERIC)) AS avg_land_value
        FROM
          real_acct
        GROUP BY
          state_class
    """,
    'get_first_100_unique_owners_for_residential': """
        SELECT
          b.acct,
          COUNT(DISTINCT o.name) AS distinct_owner_count,
          STRING_AGG(DISTINCT o.name, ', ') AS owner_names
        FROM
          building_res b
          JOIN ownership_history o ON b.acct = o.acct
        WHERE
          b.acct IN (
            SELECT
              acct
            FROM
              building_res
            LIMIT
              100
          )
        GROUP BY
          b.acct
    """,
}

# Constants for SQL validation

# Disallow DML/DDL keywords to keep execution read-only
DISALLOWED_OPERATIONS = (Insert, Update, Delete, Drop, Alter, Create, TruncateTable, Merge, Grant, Revoke, Replace)

# Allowed SQL query types
ALLOWED_SQL_KEYWORDS = ("SELECT", "WITH")