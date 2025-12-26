"""
Centralized mapping from exceptions raised during query execution to
safe HTTP status codes, user-friendly messages and log levels.

Keep this module small and dependency-light: psycopg-specific classes are
looked up conditionally so tests can run even if driver exceptions differ.
"""
from typing import Tuple
import logging

from django.db import (
    DatabaseError,
    ProgrammingError,
    OperationalError,
    InterfaceError,
    DataError,
    IntegrityError,
)
from django.core.exceptions import SuspiciousOperation

# Try to import psycopg driver-specific errors when available
_pg = None
try:
    import psycopg.errors as _pg
except Exception:
    _pg = None

logger = logging.getLogger("DjangoApp Errors")


def map_exception_to_response(e: Exception) -> Tuple[int, str, str]:
    """Map an exception to (http_status, safe_message, log_level).

    log_level is one of: 'ERROR' or 'WARNING'.
    """
    # Psycopg-specific mappings if available
    if _pg is not None:
        if isinstance(e, getattr(_pg, 'SyntaxError', ())):
            return 400, "SQL syntax error.", 'WARNING'
        if isinstance(e, getattr(_pg, 'UndefinedTable', ())):
            return 400, "Referenced table does not exist.", 'WARNING'
        if isinstance(e, getattr(_pg, 'UndefinedColumn', ())):
            return 400, "Referenced column does not exist.", 'WARNING'
        if isinstance(e, getattr(_pg, 'InsufficientPrivilege', ())):
            return 403, "Permission denied to execute this query.", 'WARNING'
        if isinstance(e, getattr(_pg, 'IntegrityError', ())):
            return 409, "Database constraint violation.", 'WARNING'

    # Django DB exceptions
    if isinstance(e, ProgrammingError):
        return 400, "SQL syntax error or invalid SQL referenced.", 'WARNING'
    if isinstance(e, DataError):
        return 400, "Invalid data encountered when executing the query.", 'WARNING'
    if isinstance(e, IntegrityError):
        return 409, "Database constraint violation.", 'WARNING'
    if isinstance(e, (OperationalError, InterfaceError)):
        return 503, "Database temporarily unavailable; please try again later.", 'ERROR'
    if isinstance(e, DatabaseError):
        return 500, "Database error while executing query.", 'ERROR'

    # Session/suspicious activity
    if isinstance(e, SuspiciousOperation):
        return 403, "Suspicious request or session data detected.", 'WARNING'

    # Fallback for other runtime errors (serialization, CSV writing, etc.)
    # Treat as server error but keep message generic.
    return 500, "Server error processing the request.", 'ERROR'
