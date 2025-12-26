"""
Utility helpers for SQL validation, formatting, and safe read-only handling.

Provided helpers:
- `format_sql_rows(result, columns)` — Convert iterable query results into SQL VALUES tuples.
- `generate_insert_sql(result, table_name="table_name")` — Produce an INSERT statement intended only for data export.
- `clean_sql_input(sql)` — Normalize whitespace, unicode spaces, and newlines in SQL input.
- `validate_sql_with_sqlglot(sql)` — Parse and validate SQL using `sqlglot`, enforcing read-only rules
    and a configurable nesting depth (`MAX_QUERY_DEPTH` from Django settings).
- `_compute_query_depth(parsed)` — Internal helper to compute query nesting via sqlglot's scope tree.
- `generate_unique_query_key()` — UUID-based unique key generator for queries.

Security notes:
- `validate_sql_with_sqlglot` rejects disallowed operations (DML/DDL and other unsafe nodes) defined
    in `DISALLOWED_OPERATIONS` and enforces read-only execution intent.
- These utilities are intended for read-only analysis and export only. They do not replace parameterized
    queries, proper DB roles, or other protections against SQL injection. Always validate and sanitize
    user-provided input before executing any SQL.
"""
from typing import Tuple, Optional
from sqlglot import parse_one
from sqlglot.errors import ParseError
from sqlglot.expressions import Expression
from sqlglot.optimizer.scope import build_scope
from .constants import ALLOWED_SQL_KEYWORDS, DISALLOWED_OPERATIONS
import logging
from django.conf import settings
import uuid
import re
from .query_depth import QueryDepthAnalyzer

logger = logging.getLogger("DjangoApp Utilities Module")
logger.setLevel(logging.INFO)  # Set default logging level to INFO

MAX_QUERY_DEPTH = getattr(settings, 'MAX_QUERY_DEPTH', 10)

def generate_unique_query_key():
    """Generate a unique query key for cache using UUID."""
    return str(uuid.uuid4())

def format_sql_rows(result, columns):
    """Format rows for SQL export."""
    rows_out = []
    for row in result:
        vals = []
        for col in columns:
            v = row.get(col)
            if v is None:
                vals.append('NULL')
            elif isinstance(v, (int, float)):
                vals.append(str(v))
            else:
                s = str(v).replace("'", "''")
                vals.append(f"'{s}'")
        rows_out.append('  (' + ', '.join(vals) + ')')
    return rows_out


def generate_export_sql(result, table_name="table_name"):
    """Generate SQL text for file export."""
    columns = list(result[0].keys()) if result else []
    if not columns:
        return None  # No columns, return None to indicate empty result

    header = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES"
    rows_out = format_sql_rows(result, columns)
    return header + '\n' + ',\n'.join(rows_out) + ';'

# TODO: strip SQL comments?
def clean_sql_input(sql: str) -> str:
    """Normalize whitespace in SQL input.

    Replace various unicode space characters with a regular space, normalize
    Windows/Mac line endings to Unix newlines, and trim surrounding
    whitespace.
    """
    sql = re.sub(r'[\u00A0\u1680\u180E\u2000-\u200B\u202F\u205F\u3000]', ' ', sql)
    sql = sql.replace('\r\n', '\n').replace('\r', '\n')
    return sql.strip()

def _compute_query_depth(parsed: Expression) -> int:
    analyzer = QueryDepthAnalyzer()
    return analyzer.compute_from_parsed(parsed)

def validate_sql_with_sqlglot(sql: str) -> Tuple[bool, Optional[str], Optional[Expression]]:
    """
    Validate a parsed SQL Expression and enforce read-only and complexity constraints.

    Behavior:
    - Rejects queries containing disallowed DML/DDL operations.
    - Computes semantic nesting depth using sqlglot's scope tree (CTEs/subqueries).
      If scope construction fails, the depth check is skipped and a warning is logged.
    - Enforces `MAX_QUERY_DEPTH` as the maximum allowed nesting depth.
    - Accepts only queries starting with allowed keywords defined in `ALLOWED_SQL_KEYWORDS`.
    - Removes comments from SQL input.

    Args:
        sql (str | sqlglot.Expression): Raw SQL string or already-parsed Expression.

    Returns:
        Tuple[bool, Optional[str], Optional[Expression]]: (is_valid, error_message, parsed_expression).
        `parsed_expression` will be None when parsing fails.
    """
    try:
        # Input must be a raw SQL string
        if not isinstance(sql, str) or not sql.strip():
            return False, "Empty SQL statement provided.", None
        try:
            parsed = parse_one(sql, read="postgres")
        except ParseError as e:
            logger.error(f"SQL parse failed: {e}")
            return False, f"Invalid SQL syntax: {sql}", None

        # Check for disallowed operations
        for node in parsed.walk():
            if isinstance(node, DISALLOWED_OPERATIONS):
                names = ', '.join(op.__name__ for op in DISALLOWED_OPERATIONS)
                return False, f"Query contains one of unsafe operations: ({names})", parsed

        # Use sqlglot scope tree to measure actual query nesting depth
        query_depth = _compute_query_depth(parsed)
        if query_depth > MAX_QUERY_DEPTH:
            logger.debug(f"Query depth {query_depth} exceeds maximum allowed depth of {MAX_QUERY_DEPTH}.")
            return False, "Query is too complex (exceeds maximum depth).", parsed
        elif query_depth == 0:
            logger.warning("Could not compute query depth, potentially flat query or scope construction failure.")

        # Ensure the query is a SELECT or WITH query AND comments are stripped
        if not parsed.sql(comments=False).strip().upper().startswith(ALLOWED_SQL_KEYWORDS):
            return False, "Only SELECT or WITH queries are allowed.", parsed

    except (AttributeError, TypeError, KeyError) as e:
        logger.error(f"Internal error during scope processing: {e}")
        return False, "Internal error while processing query scope.", None

    except ParseError as e:
        # Log invalid queries for debugging
        logger.error(f"SQL validation failed: {e}")
        return False, f"Invalid SQL syntax: {sql}", None

    except Exception as e:
        logger.error(f"Unexpected error during SQL validation: {e}")
        return False, "An unexpected error occurred during SQL validation.", None

    return True, None, parsed
