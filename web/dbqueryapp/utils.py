import re
from typing import Tuple, Optional, Any

# Disallow DML/DDL keywords to keep execution read-only
FORBIDDEN_PATTERN = re.compile(r'\b(insert|update|delete|drop|alter|create|truncate|grant|revoke|replace|merge)\b', re.IGNORECASE)


def _coerce_sql_to_str(sql: Any) -> str:
    """Coerce various input types to a stripped string. None -> ''.

    Bytes/bytearray are decoded as UTF-8 with replacement for invalid
    sequences. Other non-string objects are converted via `str()`.
    """
    if sql is None:
        return ""
    if isinstance(sql, (bytes, bytearray)):
        try:
            return sql.decode("utf-8", errors="replace").strip()
        except Exception:
            return str(sql).strip()
    return str(sql).strip()


def validate_sql_is_select(sql: Any) -> Tuple[bool, Optional[str]]:
    """Return (True, None) if SQL appears to be a read-only SELECT/ WITH query, else (False, error_message)."""
    s = _coerce_sql_to_str(sql)
    if not s:
        return False, "Empty SQL provided."
    # strip trailing semicolon for checks
    s = s.rstrip(';')

    if FORBIDDEN_PATTERN.search(s):
        return False, "Only read-only SELECT/ WITH queries are allowed."

    # allow queries starting with SELECT or WITH
    first = s.split(None, 1)[0].lower()
    if first not in ('select', 'with'):
        return False, "Queries must start with SELECT or WITH."

    return True, None


def ensure_sql_has_limit(sql: Any, limit: int = 1000) -> str:
    """If the SQL already contains a LIMIT clause (naive check), return it unchanged.
    Otherwise append a LIMIT to protect the DB from huge results.
    """
    s = _coerce_sql_to_str(sql)
    if not s:
        return ""
    s = s.rstrip(';')
    if re.search(r'\blimit\b', s, re.IGNORECASE):
        return s
    return f"{s} LIMIT {limit}"
