import csv
import json
from django.shortcuts import render
from django.db import connection
from django.http import HttpResponse
from io import StringIO
from .forms import QueryForm

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
    query_key = None 

    if request.method == 'POST' and form.is_valid():
        query_key = form.cleaned_data['query']
        sql = SQL_QUERIES.get(query_key)
        if not sql:
            error = 'No SQL query found for the selected option.'
        else: 
            try:
                with connection.cursor() as cursor:
                    cursor.execute(sql)
                    result = dictfetchall(cursor)
            except Exception as e:
                error = str(e)
    return render(request, 'home.html', {'form': form, 'result': result, 'error': error, 'query_key': query_key})

def export_results(request, query_key):
    format_type = request.GET.get('format', 'csv') # Default to CSV
    sql = SQL_QUERIES.get(query_key)
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
        lines = []
        columns = result[0].keys() if result else []
        placeholders = ', '.join(['%s'] * len(columns))
        lines.append(f"INSERT INTO table_name ({', '.join(columns)}) VALUES")
        for row in result:
            lines.append(f"  ({placeholders}),\n" % tuple(row.values()))
        response.write('\n'.join(lines[:-1]) + ';')  # Remove last comma
        return response
    
    return HttpResponse("Unsupported format", status=400)
