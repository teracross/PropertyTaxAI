"""
This module contains the views for the Django application, including the `home` view for executing SQL queries.

Changes:
- Integrated `validate_sql_with_sqlglot` from `utils.py` to validate SQL queries before execution.
- Enhanced error handling to provide user-friendly messages for invalid queries.

Security Considerations:
- The `validate_sql_with_sqlglot` function ensures that only read-only queries are executed.
- Queries are validated before execution to prevent SQL injection and other malicious activities.

Usage:
- The `home` view handles user-submitted queries via a form and displays the results in a table format.
- Ensure that all queries added to `SQL_QUERIES` are pre-approved and safe for execution.
"""

import csv
import json
import logging
from decimal import Decimal

from django.shortcuts import render
from django.db import connection
from django.http import HttpResponse, HttpRequest
from io import StringIO
from django.core.serializers.json import DjangoJSONEncoder
from .forms import QueryForm, CustomSQLForm
from .utils import clean_sql_input, generate_export_sql, generate_unique_query_key, validate_sql_with_sqlglot
from .errors import map_exception_to_response
from .constants import DEFAULT_SQL_QUERIES
from sqlglot.errors import ParseError
from typing import Optional, List, Dict, Any

DEFAULT_QUERY_LIMIT = 1000
DEFAULT_DOWNLOAD_FORMATS = ['csv', 'json', 'sql']
logger = logging.getLogger("DjangoApp")

class RequestQueryData:
    def __init__(self, sql, result=None):
        self.sql = sql
        self.result = result

    def to_dict(self):
        """Convert the object to a dictionary for storing in the session."""
        return {
            "sql": self.sql,
            "result": self.result,
        }

    @classmethod
    def from_dict(cls, data):
        """Create an object from a dictionary."""
        return cls(
            sql=data.get("sql"),
            result=data.get("result"),
        )

def retrieve_cursor_as_dict(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def _execute_sql(sql_text):
    """Execute SQL and return (result, error)."""
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql_text)
            return retrieve_cursor_as_dict(cursor), None
    except Exception as e:
        # Map exception to a safe message for the caller and log appropriately.
        status, msg, level = map_exception_to_response(e)
        if level == 'ERROR':
            logger.exception("Error executing SQL: %s", e)
        else:
            logger.warning("SQL execution warning: %s", e)
        return None, msg

def _convert_decimal_to_serializable(obj):
    """Recursively convert Decimal objects to string in a dictionary or list."""
    if isinstance(obj, Decimal):
        return str(obj)  # Convert Decimal to string
    elif isinstance(obj, dict):
        return {key: _convert_decimal_to_serializable(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [_convert_decimal_to_serializable(item) for item in obj]
    return obj

def _save_data_in_session(request, sql, result, query_id):
    """Store a custom SQL statement and its result in the session using a custom class."""
    logger.debug(f"Saving data in session for query_id: {query_id}")
    request_data = RequestQueryData(
        sql=sql,
        result=_convert_decimal_to_serializable(result)
    )
    stored = request.session.get('data', {})

    if not isinstance(stored, dict):
        logger.warning("Session data 'data' is not a dictionary. Resetting to an empty dictionary.")
        stored = {}

    # Update the dictionary with the new query_id and its associated data
    stored[query_id] = request_data.to_dict()
    request.session['data'] = stored  # Save the updated dictionary back to the session
    request.session.modified = True  # Mark the session as modified

def home(request: HttpRequest) -> HttpResponse:
    result: Optional[List[Dict[str, Any]]] = None
    error: Optional[str] = None
    form: QueryForm = QueryForm(request.POST or None)
    custom_form: CustomSQLForm = CustomSQLForm(request.POST or None)
    query_id: Optional[str] = None 
    sql: Optional[str] = None
    formatted_sql: Optional[str] = None

    if request.method == 'POST':
        form_type = request.POST.get('form_type')

        match form_type:
            case 'custom_sql_form':
                if custom_form.is_valid():
                    logger.debug("Custom SQL form submitted.")
                    sql = custom_form.cleaned_data.get('user_sql')

                    if not sql:
                        error = "No SQL provided."
                    else:
                        try:
                            sql = clean_sql_input(sql)
                            is_valid, error, parsed_sql = validate_sql_with_sqlglot(sql)

                            if is_valid:
                                result, exec_err = _execute_sql(sql)
                                if exec_err:
                                    error = exec_err
                                formatted_sql = parsed_sql.sql(pretty=True) if parsed_sql else sql
                        except ParseError as pe:
                            error = f"SQL Parsing Error: {pe}"

            case 'query_form':
                if form.is_valid():
                    logger.debug("Predefined query form submitted.")
                    query_key = form.cleaned_data['query']
                    sql = DEFAULT_SQL_QUERIES.get(query_key)

                    if not sql:
                        error = 'No SQL query found for the selected option.'
                    else:
                        result, exec_err = _execute_sql(sql)
                        if exec_err:
                            error = exec_err
                        formatted_sql = sql

            case _:  # Default case for unsupported form types
                error = "Unsupported form type submitted."

        if sql and result is not None:
            query_id = generate_unique_query_key()
            logger.debug(f"Generated query_id: {query_id} for the executed SQL.")
            _save_data_in_session(request, sql, result, query_id)

    return render(request, 'home.html', {
        'form': form,
        'custom_form': custom_form,
        'result': result,
        'error': error,
        'query_id': query_id,
        'sql': sql,
        'formatted_sql': formatted_sql,
    })

def generate_response(content, content_type, filename):
        response = HttpResponse(content, content_type=content_type)
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response

def export_results(request, query_id: Optional[str] = None) -> HttpResponse:
    """Export the results of a predefined or custom SQL query using the session_id."""
    format = request.GET.get('format', 'csv')  # Default to CSV
    if format not in DEFAULT_DOWNLOAD_FORMATS:
        return HttpResponse("Unsupported format", status=400)

    # Retrieve the query and results from session data
    stored = request.session.get('data', {})
    if not isinstance(stored, dict):
        stored = {}

    session_data = stored.get(query_id)
    logger.debug(f"Available keys in session data: {list(stored.keys())}")

    if not session_data or 'sql' not in session_data:
        logger.debug(f"No SQL found in session data for query_id: {query_id}")
        return HttpResponse(f"Error: Unable to find corresponding SQL for download. Please try again.", status=503)
    else: 
        session_data = RequestQueryData.from_dict(session_data)

    try:
        with connection.cursor() as cursor:
            cursor.execute(session_data.sql)
            result = retrieve_cursor_as_dict(cursor)
    except Exception as e:
        status, msg, level = map_exception_to_response(e)
        if level == 'ERROR':
            logger.exception("Error exporting results for query_id=%s: %s", query_id, e)
        else:
            logger.warning("Export warning for query_id=%s: %s", query_id, e)
        return HttpResponse(msg, status=status)

    filename = f"query_result.{format}"

    if format == 'csv':
        if not result:
            return generate_response('', 'text/csv', filename)
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=result[0].keys())
        writer.writeheader()
        writer.writerows(result)
        return generate_response(output.getvalue(), 'text/csv', filename)

    elif format == 'json':
        return generate_response(json.dumps(result, cls=DjangoJSONEncoder), 'application/json', filename)

    elif format == 'sql':
        if not result:
            return generate_response('', 'text/plain', filename)
        sql_text = generate_export_sql(result, 'result_table')
        if sql_text:
            return generate_response(sql_text, 'text/plain', filename)
        else:
            return generate_response('', 'text/plain', filename)

    return HttpResponse("Unsupported format", status=400)
