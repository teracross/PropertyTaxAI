import csv
import json
import logging
import sqlparse
import uuid
from django.shortcuts import render
from django.db import connection
from django.http import HttpResponse
from io import StringIO
from .forms import QueryForm, CustomSQLForm
from .utils import validate_sql_is_select, ensure_sql_has_limit

SQL_QUERIES = {
    'get_first_100_real_acct': "SELECT * FROM real_acct LIMIT 100",
    'get_avg_bldg_and_land_val_by_state_class': "SELECT state_class, AVG(CAST(bld_val AS numeric)) AS avg_building_value, AVG(CAST(land_val AS numeric)) AS avg_land_value FROM real_acct GROUP BY state_class;",
    'get_first_100_unique_owners_for_residential':"SELECT b.acct, COUNT(DISTINCT o.name) AS distinct_owner_count, STRING_AGG(DISTINCT o.name, ', ') AS owner_names FROM building_res b JOIN ownership_history o ON b.acct = o.acct WHERE b.acct IN (SELECT acct FROM building_res LIMIT 100) GROUP BY b.acct;",

}

def dictfetchall(cursor):
    "Return all rows from a cursor as a dict"
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]

def home(request):
    result = None
    error = None
    form = QueryForm(request.POST or None)
    custom_form = CustomSQLForm(request.POST or None)
    query_key = None 
    raw_sql = None
    formatted_sql = None
    if request.method == 'POST':
        # Prefer custom SQL if provided
        if custom_form.is_valid() and custom_form.cleaned_data.get('user_sql'):
            user_sql = custom_form.cleaned_data.get('user_sql')
            is_valid, msg = validate_sql_is_select(user_sql)
            if not is_valid:
                error = msg
            else:
                try:
                    raw_sql = ensure_sql_has_limit(user_sql, limit=1000)
                    with connection.cursor() as cursor:
                        cursor.execute(raw_sql)
                        result = dictfetchall(cursor)
                except Exception as e:
                    error = str(e)
            try:
                formatted_sql = sqlparse.format(raw_sql or user_sql, reindent=True, keyword_case='upper')
            except Exception:
                pass
        # Otherwise fall back to the predefined query selector
        elif form.is_valid():
            query_key = form.cleaned_data['query']
            raw_sql = SQL_QUERIES.get(query_key)

            if not raw_sql:
                error = 'No SQL query found for the selected option.'
            else: 
                try:
                    with connection.cursor() as cursor:
                        cursor.execute(raw_sql)
                        result = dictfetchall(cursor)
                except Exception as e:
                    error = str(e)
                
                try:
                    formatted_sql = sqlparse.format(raw_sql, reindent=True, keyword_case='upper')
                except Exception as e:
                    logging.getLogger(__name__).warning("SQL formatting failed for query '%s': %s", query_key, e)
        # If a custom SQL was executed (raw_sql present but no predefined query_key), save it in session
        if raw_sql and not query_key and result is not None:
            gen_key = f"custom_{uuid.uuid4().hex[:8]}"
            stored = request.session.get('custom_sql', {})
            stored[gen_key] = raw_sql
            request.session['custom_sql'] = stored
            request.session.modified = True
            query_key = gen_key
    return render(request, 'home.html', {
        'form': form,
        'custom_form': custom_form,
        'result': result,
        'error': error,
        'query_key': query_key,
        'sql': raw_sql,
        'formatted_sql': formatted_sql,
    })

def export_results(request, query_key):
    format_type = request.GET.get('format', 'csv') # Default to CSV
    sql = SQL_QUERIES.get(query_key)
    # fallback: check session-stored custom SQLs
    if not sql:
        sql = request.session.get('custom_sql', {}).get(query_key)
    if not sql:
        return HttpResponse("Invalid query", status=404)
    
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            result = dictfetchall(cursor)
    except Exception as e:
        return HttpResponse(f"Error: {str(e)}", status=500)
    
    filename = f"query_result.{format_type}"
    
    if format_type == 'csv':
        response = HttpResponse(content_type='text/csv')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=result[0].keys() if result else [])
        writer.writeheader()
        writer.writerows(result)
        response.write(output.getvalue())
        return response
    
    elif format_type == 'json':
        response = HttpResponse(json.dumps(result), content_type='application/json')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
    
    elif format_type == 'sql':
        # Simple SQL dump (INSERT statements)
        response = HttpResponse(content_type='text/plain')
        response['Content-Disposition'] = f'attachment; filename="{filename}"'
        columns = list(result[0].keys()) if result else []
        if columns:
            header = f"INSERT INTO table_name ({', '.join(columns)}) VALUES"
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
            sql_text = header + '\n' + ',\n'.join(rows_out) + ';'
            response.write(sql_text)
        else:
            response.write('')
        return response
    
    return HttpResponse("Unsupported format", status=400)
